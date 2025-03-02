
#!/bin/bash
echo "Pushing images to ECR ..."
echo " "
echo " "

aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 868497626916.dkr.ecr.us-east-1.amazonaws.com
echo " "
echo " "

docker push 868497626916.dkr.ecr.us-east-1.amazonaws.com/api-gateway:latest
echo " "
echo " "

docker push 868497626916.dkr.ecr.us-east-1.amazonaws.com/config-server:latest
echo " "
echo " "

docker push 868497626916.dkr.ecr.us-east-1.amazonaws.com/rate-limiter-rules-dashboard:latest
echo " "
echo " "

docker push 868497626916.dkr.ecr.us-east-1.amazonaws.com/kvs-admin-dashboard:latest
echo " "
echo " "

docker push 868497626916.dkr.ecr.us-east-1.amazonaws.com/kvs-storage-server:latest
echo " "
echo " "

docker push 868497626916.dkr.ecr.us-east-1.amazonaws.com/kvs-metadata-server:latest
echo " "
echo " "

docker push 868497626916.dkr.ecr.us-east-1.amazonaws.com/os-admin-dashboard:latest
echo " "
echo " "

docker push 868497626916.dkr.ecr.us-east-1.amazonaws.com/os-inventory-service:latest
echo " "
echo " "

docker push 868497626916.dkr.ecr.us-east-1.amazonaws.com/os-order-service:latest
echo " "
echo " "


#docker push 868497626916.dkr.ecr.us-east-1.amazonaws.com/os-ordering-ui:latest
#echo " "
#echo " "

echo "Image pushing completed successfully!"

