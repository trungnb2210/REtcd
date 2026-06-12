// lossbench measures the durability loss window: it hammers sequential Txn-create
// writes and records the highest key index it got an ACK for. Run it from a host
// that SURVIVES the backend's crash (the laptop, against a VM backend), so the ack
// record is durable. Then power-loss the backend, recover it, and run --mode=count
// to see how many acked keys survived. Lost = acked - present.
package main

import (
	"context"
	"flag"
	"fmt"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ep := flag.String("endpoint", "127.0.0.1:2379", "etcd v3 endpoint")
	mode := flag.String("mode", "write", "write | count")
	flag.Parse()

	conn, err := grpc.NewClient(*ep, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println("DIAL_ERR", err)
		return
	}
	defer func() { _ = conn.Close() }()
	kv := pb.NewKVClient(conn)
	ctx := context.Background()

	if *mode == "count" {
		// All keys with prefix "loss/" → range ["loss/", "loss0").
		resp, err := kv.Range(ctx, &pb.RangeRequest{
			Key: []byte("loss/"), RangeEnd: []byte("loss0"), CountOnly: true,
		})
		if err != nil {
			fmt.Println("COUNT_ERR", err)
			return
		}
		fmt.Printf("PRESENT=%d\n", resp.Count)
		return
	}

	last := 0
	for i := 1; ; i++ {
		key := fmt.Sprintf("loss/%012d", i)
		resp, err := kv.Txn(ctx, &pb.TxnRequest{
			Compare: []*pb.Compare{{
				Target: pb.Compare_MOD, Result: pb.Compare_EQUAL,
				Key: []byte(key), TargetUnion: &pb.Compare_ModRevision{ModRevision: 0},
			}},
			Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{
				RequestPut: &pb.PutRequest{Key: []byte(key), Value: []byte("x")},
			}}},
		})
		if err != nil {
			fmt.Printf("STOPPED acked=%d err=%v\n", last, err)
			return
		}
		if resp.Succeeded {
			last = i
			// fine-grained milestones: when the writer runs ON the crashing host
			// with stdout streamed off-host over ssh, the last milestone received
			// is the (slightly conservative) acked count that survives the crash
			if i%500 == 0 {
				fmt.Printf("acked=%d\n", i)
			}
		}
	}
}
