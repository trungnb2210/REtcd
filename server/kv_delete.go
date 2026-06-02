package server

import (
	"context"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// DeleteRange deletes a single key or a range of keys.
//
// Kubernetes calls this for:
//   - Explicit deletes:  key="/registry/pods/default/mypod", rangeEnd=""
//   - Namespace cleanup: key="/registry/pods/default/", rangeEnd="/registry/pods/default0"
//
// Each deleted key emits a DELETE event to the Redis Stream so Watch clients
// are notified. Range deletes are not atomic — each key is deleted in a
// separate Redis call. This is a known limitation for the FYP scope.
func (s *KVServer) DeleteRange(ctx context.Context, req *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	key := string(req.Key)
	rangeEnd := string(req.RangeEnd)

	var deleted int64

	if rangeEnd == "" {
		// Single key delete.
		_, prev, err := s.store.Delete(ctx, key)
		if err != nil {
			return nil, err
		}
		if prev != nil {
			deleted = 1
		}
	} else {
		// Range delete — find all matching keys then delete each one.
		prefix := commonPrefix(key, rangeEnd)
		_, kvs, err := s.store.Range(ctx, prefix)
		if err != nil {
			return nil, err
		}
		for _, kv := range kvs {
			if kv.Key >= key && (rangeEnd == "\x00" || kv.Key < rangeEnd) {
				if _, _, err := s.store.Delete(ctx, kv.Key); err != nil {
					return nil, err
				}
				deleted++
			}
		}
	}

	return &pb.DeleteRangeResponse{
		Header:  s.header(ctx),
		Deleted: deleted,
	}, nil
}
