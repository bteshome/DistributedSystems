
eksctl get clusters
# This is not needed if using eksctl, willb be autoconfigured.
#aws eks update-kubeconfig --region us-east-1 --name c2
aws eks describe-fargate-profile --cluster-name dev  --fargate-profile-name fp-default --query "fargateProfile.podExecutionRoleArn"
# probably, you will see something like this: eksctl-c3-cluster-FargatePodExecutionRole-HsslKAsvPrF7
# then, make sure that roles the necessary permissions.
aws eks list-access-entries --cluster-name dev
kubectl config delete-context  arn:aws:eks:us-east-1:868497626916:cluster/c1
kubectl config get-contexts
kubectl config use-context docker-desktop       # revert back to dd
kubectl cluster-info
kubectl get nodes

