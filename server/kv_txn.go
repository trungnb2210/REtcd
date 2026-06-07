package server

import (
	"context"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// Txn is the most critical KV operation — almost every Kubernetes write uses it.
//
// Kubernetes sends exactly two shapes of Txn:
//
//  1. CREATE  — "if key does not exist, create it"
//     Compare: mod_revision == 0
//     Success: Put(key, value)
//     Failure: (empty)
//
//  2. UPDATE  — "if key has this revision, update it; else return current"
//     Compare: mod_revision == N
//     Success: Put(key, newValue)
//     Failure: Range(key)
//
// We pattern-match these two shapes (like kine's isCreate / isUpdate) and
// route each to the atomic Lua script in the store, which performs the
// compare-and-swap in a single Redis round-trip.
func (s *KVServer) Txn(ctx context.Context, req *pb.TxnRequest) (*pb.TxnResponse, error) {
	if put := isCreate(req); put != nil {
		return s.create(ctx, put)
	}

	if rev, key, value, lease, ok := isUpdate(req); ok {
		return s.update(ctx, key, rev, value, lease)
	}

	// Fallback: generic Txn for any shape we don't recognise.
	return s.genericTxn(ctx, req)
}

// isCreate returns the PutRequest if this Txn is a create-only operation
// (compare mod_revision == 0, success is a single Put, no failure ops).
func isCreate(req *pb.TxnRequest) *pb.PutRequest {
	if len(req.Compare) == 1 &&
		req.Compare[0].Target == pb.Compare_MOD &&
		req.Compare[0].Result == pb.Compare_EQUAL &&
		req.Compare[0].GetModRevision() == 0 &&
		len(req.Success) == 1 &&
		req.Success[0].GetRequestPut() != nil &&
		len(req.Failure) == 0 {
		return req.Success[0].GetRequestPut()
	}
	return nil
}

// isUpdate returns the fields of an update Txn:
// (expectedModRevision, key, newValue, leaseID, ok)
func isUpdate(req *pb.TxnRequest) (int64, string, []byte, int64, bool) {
	if len(req.Compare) == 1 &&
		req.Compare[0].Target == pb.Compare_MOD &&
		req.Compare[0].Result == pb.Compare_EQUAL &&
		len(req.Success) == 1 &&
		req.Success[0].GetRequestPut() != nil &&
		len(req.Failure) == 1 &&
		req.Failure[0].GetRequestRange() != nil {
		return req.Compare[0].GetModRevision(),
			string(req.Compare[0].Key),
			req.Success[0].GetRequestPut().Value,
			req.Success[0].GetRequestPut().Lease,
			true
	}
	return 0, "", nil, 0, false
}

// create handles the CREATE pattern: atomically write the key only if it does not exist.
func (s *KVServer) create(ctx context.Context, put *pb.PutRequest) (*pb.TxnResponse, error) {
	// expectedModRevision = -1 tells the Lua script "key must not exist".
	res, err := s.store.Txn(ctx, string(put.Key), -1, "PUT", put.Value, put.Lease)
	if err != nil {
		return nil, err
	}

	resp := &pb.TxnResponse{
		Header:    &pb.ResponseHeader{ClusterId: 1, MemberId: 1, Revision: res.Revision},
		Succeeded: res.Succeeded,
	}
	if res.Succeeded {
		resp.Responses = []*pb.ResponseOp{{
			Response: &pb.ResponseOp_ResponsePut{
				ResponsePut: &pb.PutResponse{
					Header: &pb.ResponseHeader{ClusterId: 1, MemberId: 1, Revision: res.Revision},
				},
			},
		}}
	}
	return resp, nil
}

// update handles the UPDATE pattern: atomically swap the value if mod_revision matches.
// On failure, returns the current value so the caller can retry with the new revision.
func (s *KVServer) update(ctx context.Context, key string, expectedRev int64, value []byte, lease int64) (*pb.TxnResponse, error) {
	res, err := s.store.Txn(ctx, key, expectedRev, "PUT", value, lease)
	if err != nil {
		return nil, err
	}

	resp := &pb.TxnResponse{
		Header:    &pb.ResponseHeader{ClusterId: 1, MemberId: 1, Revision: res.Revision},
		Succeeded: res.Succeeded,
	}

	if res.Succeeded {
		resp.Responses = []*pb.ResponseOp{{
			Response: &pb.ResponseOp_ResponsePut{
				ResponsePut: &pb.PutResponse{
					Header: &pb.ResponseHeader{ClusterId: 1, MemberId: 1, Revision: res.Revision},
				},
			},
		}}
	} else {
		// Return the current value so the caller can see what revision it's now at.
		rangeResp := &pb.RangeResponse{
			Header: &pb.ResponseHeader{ClusterId: 1, MemberId: 1, Revision: res.Revision},
		}
		if res.Current != nil {
			rangeResp.Count = 1
			rangeResp.Kvs = append(rangeResp.Kvs, toProtoKV(res.Current))
		}
		resp.Responses = []*pb.ResponseOp{{
			Response: &pb.ResponseOp_ResponseRange{ResponseRange: rangeResp},
		}}
	}

	return resp, nil
}

// genericTxn handles any Txn shape that isn't a recognised create or update.
// Each Compare is evaluated against the key's current Version / ModRevision /
// CreateRevision in the store. The corresponding Success or Failure ops then run
// and their responses are returned. There is a small read-then-write race window;
// for the patterns Kubernetes actually sends here (apiserver compactor CAS on a
// single dedicated key) that window is benign.

// This potentially can be apart of future work since this can also implemented
// as a lua script, which would provide the atomicity like the hot path of create
// or update.
func (s *KVServer) genericTxn(ctx context.Context, req *pb.TxnRequest) (*pb.TxnResponse, error) {
	succeeded, err := s.evalCompares(ctx, req.Compare)
	if err != nil {
		return nil, err
	}

	ops := req.Success
	if !succeeded {
		ops = req.Failure
	}

	var results []*pb.ResponseOp
	for _, op := range ops {
		resp, err := s.executeOp(ctx, op)
		if err != nil {
			return nil, err
		}
		results = append(results, resp)
	}

	return &pb.TxnResponse{
		Header:    s.header(ctx),
		Succeeded: succeeded,
		Responses: results,
	}, nil
}

func (s *KVServer) evalCompares(ctx context.Context, compares []*pb.Compare) (bool, error) {
	for _, c := range compares {
		ok, err := s.evalCompare(ctx, c)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func (s *KVServer) evalCompare(ctx context.Context, c *pb.Compare) (bool, error) {
	kv, err := s.store.Get(ctx, string(c.Key))
	if err != nil {
		return false, err
	}
	var actual, expected int64
	switch c.Target {
	case pb.Compare_VERSION:
		if kv != nil {
			actual = kv.Version
		}
		expected = c.GetVersion()
	case pb.Compare_MOD:
		if kv != nil {
			actual = kv.ModRevision
		}
		expected = c.GetModRevision()
	case pb.Compare_CREATE:
		if kv != nil {
			actual = kv.CreateRevision
		}
		expected = c.GetCreateRevision()
	default:
		return false, nil
	}
	switch c.Result {
	case pb.Compare_EQUAL:
		return actual == expected, nil
	case pb.Compare_NOT_EQUAL:
		return actual != expected, nil
	case pb.Compare_GREATER:
		return actual > expected, nil
	case pb.Compare_LESS:
		return actual < expected, nil
	}
	return false, nil
}

func (s *KVServer) executeOp(ctx context.Context, op *pb.RequestOp) (*pb.ResponseOp, error) {
	switch v := op.Request.(type) {
	case *pb.RequestOp_RequestPut:
		rev, _, err := s.store.Put(ctx, string(v.RequestPut.Key), v.RequestPut.Value, v.RequestPut.Lease)
		if err != nil {
			return nil, err
		}
		return &pb.ResponseOp{
			Response: &pb.ResponseOp_ResponsePut{
				ResponsePut: &pb.PutResponse{
					Header: &pb.ResponseHeader{ClusterId: 1, MemberId: 1, Revision: rev},
				},
			},
		}, nil
	case *pb.RequestOp_RequestRange:
		rangeResp, err := s.Range(ctx, v.RequestRange)
		if err != nil {
			return nil, err
		}
		return &pb.ResponseOp{
			Response: &pb.ResponseOp_ResponseRange{ResponseRange: rangeResp},
		}, nil
	case *pb.RequestOp_RequestDeleteRange:
		delResp, err := s.DeleteRange(ctx, v.RequestDeleteRange)
		if err != nil {
			return nil, err
		}
		return &pb.ResponseOp{
			Response: &pb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: delResp},
		}, nil
	}
	return &pb.ResponseOp{}, nil
}
