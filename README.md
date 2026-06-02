# REtcd
Redis etcd

# Commands
Create test nginx pods        

kubectl run nginx --image=nginx                                                                                  
kubectl get pod nginx

docker restart retcd-redis  
pkill retcd                             
./retcd & 

kubectl get pod nginx 