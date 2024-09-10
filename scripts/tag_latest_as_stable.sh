#!/bin/bash
# Pull latest image, tag it as stable and push it to the registry so it triggers the deploy process

docker pull ghcr.io/planetary-social/nos-followers:latest && \
docker tag ghcr.io/planetary-social/nos-followers:latest ghcr.io/planetary-social/nos-followers:stable && \
docker push ghcr.io/planetary-social/nos-followers:stable
