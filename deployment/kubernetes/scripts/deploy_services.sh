#!/bin/bash
# install
helm install config config-server
helm install dev env_dev

# cleanup
helm uninstall config
helm uninstall dev