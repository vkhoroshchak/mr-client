#!/bin/bash
docker network create mr_network
echo "Network mr_network has been added to mr-client/docker-compose.yaml if not existed yet!"
docker-compose up --build --remove-orphans