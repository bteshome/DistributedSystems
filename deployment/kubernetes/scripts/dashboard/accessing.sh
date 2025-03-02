
# 1. for temporary access, create a token and access the dashboard at https://localhost:8443
kubectl create token admin-user -n kubernetes-dashboard

# 2. for long term access,
kubectl get secret admin-user -n kubernetes-dashboard -o jsonpath="{.data.token}" | base64 -d

# then forward port:
kubectl -n kubernetes-dashboard port-forward svc/kubernetes-dashboard-kong-proxy 8443:443
