#!/bin/bash
 
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )  
cd "$parent_path"

sudo docker exec -it pg psql -U postgres -d graph
