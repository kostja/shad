#!/bin/bash

# Building an image and starting the docker, stopping already existing container
# and removing the image beforehand if there are any.

parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )  
cd "$parent_path"

sudo docker kill pg
sudo docker rm pg
sudo docker rmi pg
sudo docker build -t pg -f pg.Dockerfile .
sudo docker run --name pg -d pg