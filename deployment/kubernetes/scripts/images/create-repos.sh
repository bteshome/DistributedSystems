
#!/bin/bash
aws ecr create-repository --repository-name api-gateway --region us-east-1
aws ecr create-repository --repository-name config-server --region us-east-1
aws ecr create-repository --repository-name rate-limiter-rules-dashboard --region us-east-1
aws ecr create-repository --repository-name kvs-admin-dashboard --region us-east-1
aws ecr create-repository --repository-name kvs-storage-server --region us-east-1
aws ecr create-repository --repository-name kvs-metadata-server --region us-east-1
aws ecr create-repository --repository-name os-admin-dashboard --region us-east-1
aws ecr create-repository --repository-name os-inventory-service --region us-east-1
aws ecr create-repository --repository-name os-order-service --region us-east-1
aws ecr create-repository --repository-name os-ordering-ui --region us-east-1