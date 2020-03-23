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

启动Mysql

```shell
$ docker run --name poc-mysql -e MYSQL_ROOT_PASSWORD=root -d mysql
$ docker container exec -it poc-mysql bash
mysql -uroot -p
create user 'zhaoolee' identified with mysql_native_password by 'eelooahzpw';
grant all privileges on *.* to 'zhaoolee';
flush privileges;

```

Redis

```
docker run -itd --name poc-redis -p 6379:6379 redis
docker exec -it poc-redis /bin/bash
```

