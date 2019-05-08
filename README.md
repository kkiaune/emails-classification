# email-AI

## How to run
```
# 1 - build the images
docker-compose build

# 2 - launch the stack
docker-compose up -d
```

## How to stop
```
docker-compose down
```

## Service - Port Mappings
| Service | Port | URL |
| --- | --- | --- |
| Airflow | 8080 | [Airflow](http://localhost:8080/admin/) |
| FastApi | 8000 | [FastApi](http://localhost:80/docs/) |
| Jupyter | 8888 | [Jupyter](http://localhost:8888/) |
| Minio | 9000 | [Minio](http://localhost:9000/minio/) |
| Portainer | 9090 | [Portainer](http://localhost:9090/) |
| Postgres | 5432 | - |
| Superset | 8088 | [Superset](http://localhost:8088/login/) |
| pgAdmin 4 | 5050 | [pgAdmin 4](http://localhost:5050/login?next=%2F) |
