# 日常操作-Docker

搭建私服

```shell
docker pull registry
docker run -d -p ....
docker tag ....
docker push
....
#modify daemon.json
vi /etc/docker/daemon.json
  "insecure-registries","registry-mirrors"
```

