#!/bin/bash
docker run \
   --rm \
   -p 9000:9000 \
   -p 9001:9001 \
   --name minio \
   --user $(id -u):$(id -g) \
   -v ~/minio-data:/data \
   -e "MINIO_ROOT_USER=test" \
   -e "MINIO_ROOT_PASSWORD=testtesttest" \
   quay.io/minio/minio server /data --console-address ":9001"
