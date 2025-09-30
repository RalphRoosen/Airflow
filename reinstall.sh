echo "Let op: .env wordt leeg overschreven. Ook alle variabelen uit airflow zijn weg hierna!";
docker compose stop
docker compose down --volumes --rmi all
docker rm $(docker ps --filter status=exited -q)
docker container prune
docker image prune
docker image prune -a
rm -rf logs
mkdir logs

#rm .env
#echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

#docker-compose up airflow-init

#docker compose up -d
