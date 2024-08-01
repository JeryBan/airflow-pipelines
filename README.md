
# Project Title

## Initialization
----------------------

```docker-compose build```

```docker compose up airflow-init```

## Start services

```docker compose up -d```

Server runs at http://localhost:8080

## Dependancies

To include dependancies:

    1. update `requiremnets.txt` file  
    2. run `docker-compose build` again.

## Cleaning-up environment

---
```commandlin
docker compose down -v

