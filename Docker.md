# Docker

## List all containers (only IDs)

```shell
docker ps -aq
```

## Stop all running containers

```shell
docker stop $(docker ps -aq)
```

## Remove all containers

```shell
docker rm $(docker ps -aq)
```

## Remove all images

```shell
docker rmi $(docker images -q)
```

