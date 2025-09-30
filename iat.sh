#!/bin/bash
docker ps
echo Van welk docker image wil je inloggen op de active terminal [3ae4fc53ea1e]?
read containerid


if [ -z "$containerid" ]
then
      echo "is empty"
      containerid="3ae4fc53ea1e"
else
      echo "Not empty"
fi

docker exec -it $containerid /bin/bash
