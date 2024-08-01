## Initialization

---

```commandline
docker-compose build

docker compose up airflow-init

# start services
docker compose up -d
```

* server runs at http://localhost:8080

## Dependancies

---
To include dependancies, update `requiremnets.txt` file and run `docker-compose build` again.

## Cleaning-up environment

---
```commandline
docker compose down -v
```