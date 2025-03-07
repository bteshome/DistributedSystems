#!/bin/bash

echo "Tagging images..."
docker tag bteshome/api-gateway:latest 868497626916.dkr.ecr.us-east-1.amazonaws.com/api-gateway:latest
docker tag bteshome/config-server:latest 868497626916.dkr.ecr.us-east-1.amazonaws.com/config-server:latest
docker tag bteshome/rate-limiter-rules-dashboard:latest 868497626916.dkr.ecr.us-east-1.amazonaws.com/rate-limiter-rules-dashboard:latest
docker tag bteshome/kvs-admin-dashboard:latest 868497626916.dkr.ecr.us-east-1.amazonaws.com/kvs-admin-dashboard:latest
docker tag bteshome/kvs-storage-server:latest 868497626916.dkr.ecr.us-east-1.amazonaws.com/kvs-storage-server:latest
docker tag bteshome/kvs-metadata-server:latest 868497626916.dkr.ecr.us-east-1.amazonaws.com/kvs-metadata-server:latest
docker tag bteshome/os-admin-dashboard:latest 868497626916.dkr.ecr.us-east-1.amazonaws.com/os-admin-dashboard:latest
docker tag bteshome/os-inventory-service:latest 868497626916.dkr.ecr.us-east-1.amazonaws.com/os-inventory-service:latest
docker tag bteshome/os-order-service:latest 868497626916.dkr.ecr.us-east-1.amazonaws.com/os-order-service:latest
docker tag bteshome/os-ordering-ui:latest 868497626916.dkr.ecr.us-east-1.amazonaws.com/os-ordering-ui:latest
echo "Image tagging completed successfully!"
echo " "
echo " "

