# install prometheus monitoring and bind to kind-worker nodes

helm install prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace \
  --set kubeEtcd.service.port=2381 \
  --set kubeEtcd.service.targetPort=2381 \
  --set prometheus.prometheusSpec.nodeSelector.tier=management \
  --set alertmanager.alertmanagerSpec.nodeSelector.tier=management \
  --set grafana.nodeSelector.tier=management \
  --set prometheusOperator.nodeSelector.tier=management \
  --set kube-state-metrics.nodeSelector.tier=management

# install kwok and create kwok nodes

helm repo add kwok https://kwok.sigs.k8s.io/charts/
helm repo update

helm upgrade --namespace kube-system --install kwok kwok/kwok \
  --set hostNetwork=true

helm upgrade --install kwok-stage-fast kwok/stage-fast

for i in {1..50}; do
  kubectl apply -f - <<EOF
apiVersion: v1
kind: Node
metadata:
  name: kwok-node-$i
  annotations:
    kwok.x-k8s.io/node: fake
  labels:
    type: kwok
status:
  capacity: { cpu: "4", memory: "8Gi", pods: "110" }
  phase: Running
EOF
done