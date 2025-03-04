#!/bin/bash
#wsl - manual first step
cd /mnt/c/code/DistributedSystems/deployment/kubernetes/scripts/images/
source ./create-images.sh
cd /mnt/c/code/DistributedSystems/deployment/kubernetes/scripts/images/
source ./tag-images.sh
exit
# login to ecr - manual first step
cd /c/code/DistributedSystems/deployment/kubernetes/scripts/images/
source ./push-images.sh
cd /c/code/DistributedSystems/deployment/kubernetes/scripts/cluster
source ./create-cluster.sh

