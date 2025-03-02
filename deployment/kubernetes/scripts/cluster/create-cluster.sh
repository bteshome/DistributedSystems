#!/bin/bash
#eksctl create cluster --name c1 --region us-east-1 --fargate
eksctl create cluster -f cluster-config.yml
kubectl apply -f ingress-class.yml
kubectl apply -f storage-class.yml
cd ../dashboard
source ./setup.sh
