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
// Kubernetes rarely sends anything else, but we handle it to avoid panics.
func (s *KVServer) genericTxn(ctx context.Context, req *pb.TxnRequest) (*pb.TxnResponse, error) {
	succeeded, compareKey, expectedRev := evalCompares(req.Compare)

	var ops []*pb.RequestOp
	if succeeded {
		ops = req.Success
	} else {
		ops = req.Failure
	}

	var results []*pb.ResponseOp
	for _, op := range ops {
		switch v := op.Request.(type) {
		case *pb.RequestOp_RequestPut:
			if compareKey != "" {
				res, err := s.store.Txn(ctx, compareKey, expectedRev, "PUT", v.RequestPut.Value, v.RequestPut.Lease)
				if err != nil {
					return nil, err
				}
				succeeded = res.Succeeded
				results = append(results, &pb.ResponseOp{
					Response: &pb.ResponseOp_ResponsePut{
						ResponsePut: &pb.PutResponse{
							Header: &pb.ResponseHeader{ClusterId: 1, MemberId: 1, Revision: res.Revision},
						},
					},
				})
			} else {
				rev, _, err := s.store.Put(ctx, string(v.RequestPut.Key), v.RequestPut.Value, v.RequestPut.Lease)
				if err != nil {
					return nil, err
				}
				results = append(results, &pb.ResponseOp{
					Response: &pb.ResponseOp_ResponsePut{
						ResponsePut: &pb.PutResponse{
							Header: &pb.ResponseHeader{ClusterId: 1, MemberId: 1, Revision: rev},
						},
					},
				})
			}
		case *pb.RequestOp_RequestRange:
			rangeResp, err := s.Range(ctx, v.RequestRange)
			if err != nil {
				return nil, err
			}
			results = append(results, &pb.ResponseOp{
				Response: &pb.ResponseOp_ResponseRange{ResponseRange: rangeResp},
			})
		case *pb.RequestOp_RequestDeleteRange:
			delResp, err := s.DeleteRange(ctx, v.RequestDeleteRange)
			if err != nil {
				return nil, err
			}
			results = append(results, &pb.ResponseOp{
				Response: &pb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: delResp},
			})
		}
	}

	return &pb.TxnResponse{
		Header:    s.header(ctx),
		Succeeded: succeeded,
		Responses: results,
	}, nil
}

// evalCompares checks all compare conditions and returns:
//   - whether all conditions pass
//   - the key being compared
//   - the expected mod_revision (-1 means "key must not exist")
func evalCompares(compares []*pb.Compare) (bool, string, int64) {
	if len(compares) == 0 {
		return true, "", 0
	}
	c := compares[0]
	key := string(c.Key)
	switch c.Target {
	case pb.Compare_MOD:
		expectedRev := c.GetModRevision()
		if c.Result == pb.Compare_EQUAL || c.Result == pb.Compare_GREATER {
			return true, key, expectedRev
		}
	case pb.Compare_VERSION:
		if c.GetVersion() == 0 && c.Result == pb.Compare_EQUAL {
			return true, key, -1
		}
	case pb.Compare_CREATE:
		if c.GetCreateRevision() == 0 && c.Result == pb.Compare_EQUAL {
			return true, key, -1
		}
	}
	return true, key, 0
}
