package server

import (
	"context"

	"github.com/trungnb2210/REtcd/store"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// Range handles both single-key Gets and prefix/range List operations.
//
// etcd encodes a prefix scan as:
//   key      = "/registry/pods/default/"
//   rangeEnd = "/registry/pods/default0"   (last byte of key incremented by 1)
//
// An empty rangeEnd means single key lookup.
// A rangeEnd of "\x00" means "all keys from key onwards".
func (s *KVServer) Range(ctx context.Context, req *pb.RangeRequest) (*pb.RangeResponse, error) {
	key := string(req.Key)
	rangeEnd := string(req.RangeEnd)

	var kvs []*store.KeyValue
	var rev int64

	if rangeEnd == "" {
		// Single key lookup — pipeline GET + revision in one round-trip.
		kv, err := s.store.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if kv != nil {
			kvs = []*store.KeyValue{kv}
			rev = kv.ModRevision
		}
		// Get current revision for the header (key may not exist).
		if curRev, err := s.store.CurrentRevision(ctx); err == nil && curRev > rev {
			rev = curRev
		}
	} else {
		// Prefix / range scan — 2 round-trips total via MGET (down from N+2).
		prefix := commonPrefix(key, rangeEnd)
		var err error
		var all []*store.KeyValue
		rev, all, err = s.store.Range(ctx, prefix)
		if err != nil {
			return nil, err
		}
		for _, kv := range all {
			if kv.Key >= key && (rangeEnd == "\x00" || kv.Key < rangeEnd) {
				kvs = append(kvs, kv)
			}
		}
	}

	resp := &pb.RangeResponse{
		Header: &pb.ResponseHeader{ClusterId: 1, MemberId: 1, Revision: rev},
		Count:  int64(len(kvs)),
	}
	if !req.CountOnly {
		for _, kv := range kvs {
			resp.Kvs = append(resp.Kvs, toProtoKV(kv))
		}
	}
	return resp, nil
}
