#!/bin/bash

if [[ "$1" == "up" ]]; then
    docker-compose up -d
elif [[ "$1" == "down" ]]; then
    docker-compose down 2>/dev/null
fi