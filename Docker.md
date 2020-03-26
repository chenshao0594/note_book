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

## Entry Running Container

````shell
docker exec -i -t ${container_id}  /bin/bash
````

## Out Running Container

```shell
Ctrl -D
```

## Docker push private Registry, denid

```shell
docker login ${registry_ip}:${registry_port}
```

## k8s网关

```shell
traefik
```