for SERVICE in $(docker compose ps --services | shuf)
do
    echo "${SERVICE}"
    docker compose up -d "${SERVICE}"
    sleep 600
done
