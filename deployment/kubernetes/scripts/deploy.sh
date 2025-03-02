
#!/bin/bash
wsl
cd /mnt/c/code/DistributedSystems/deployment/kubernetes/scripts/images/
source ./create-images.sh
source ./tag-images.sh
exit
cd /c/code/DistributedSystems/deployment/kubernetes/scripts/images/
source ./push-images.sh
cd ../cluster
source ./create-cluster.sh

