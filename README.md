# Apache Pulsar - Request/Reply in Go

Example repository showing request/reply pattern over Pulsar

```
$ docker run -it \
 -p 6650:6650 \
 -p 8080:8080 \
 -e PULSAR_MEM=" -Xms512m -Xmx512m -XX:MaxDirectMemorySize=1g" \
 --mount source=pulsardata,target=/pulsar/data \
 --mount source=pulsarconf,target=/pulsar/conf \
 apachepulsar/pulsar:2.8.0 \
 bin/pulsar standalone

$ go run main.go
```
