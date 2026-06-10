# REtcd
Redis etcd

# Boot a Kind cluster backed by REtcd (Docker Desktop on macOS)

```sh
docker run -d --name retcd-redis -p 6379:6379 redis:7 --appendonly yes
go build . && ./retcd &
kind create cluster --config kind-config.yaml   # 192.168.65.254 = Docker Desktop host gateway
```

`kind-config.yaml` patches kubeadm to use an external etcd endpoint, so the cluster's
control plane runs against REtcd on the host instead of its own etcd.

# Commands
Create test nginx pods        

kubectl run nginx --image=nginx                                                                                  
kubectl get pod nginx

docker restart retcd-redis  
pkill retcd                             
./retcd & 

kubectl get pod nginx 

# run lint
golangci-lint run ./...        # check
golangci-lint run --fix ./...  # auto-fix formatting/quickfixes