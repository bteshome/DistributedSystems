
#!/bin/bash
echo "Creating images..."
cd /mnt/c/code/DistributedSystems


cd api-gateway
mvn clean compile jib:dockerBuild
echo " "
echo " "


cd ../config-server
mvn clean compile jib:dockerBuild
echo " "
echo " "


cd ../rate-limiter-rules-dashboard
mvn clean compile jib:dockerBuild
echo " "
echo " "


cd ../key-value-store/common
mvn clean install
echo " "
echo " "


cd ../client
mvn clean install
echo " "
echo " "


cd ../admin-dashboard
mvn clean compile jib:dockerBuild
echo " "
echo " "


cd ../metadata-server
mvn clean compile jib:dockerBuild
echo " "
echo " "


cd ../storage-server
mvn clean compile jib:dockerBuild
echo " "
echo " "


cd ../../online-store/admin-dashboard
mvn clean compile jib:dockerBuild
echo " "
echo " "


cd ../inventory-service
mvn clean compile jib:dockerBuild
echo " "
echo " "


cd ../order-service
mvn clean compile jib:dockerBuild
echo " "
echo " "


cd ../ordering-ui
docker build -t os-ordering-ui:latest .


echo "Image creation completed successfully!"
