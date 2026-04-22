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

	if rangeEnd == "" {
		// Single key lookup.
		kv, err := s.store.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if kv != nil {
			kvs = []*store.KeyValue{kv}
		}
	} else {
		// Prefix / range scan.
		// Derive the common prefix from key and rangeEnd so we can narrow
		// the Redis SSCAN, then filter the results to the exact [key, rangeEnd) window.
		prefix := commonPrefix(key, rangeEnd)
		all, err := s.store.Range(ctx, prefix)
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
		Header: s.header(ctx),
		Count:  int64(len(kvs)),
	}
	for _, kv := range kvs {
		resp.Kvs = append(resp.Kvs, toProtoKV(kv))
	}
	return resp, nil
}
