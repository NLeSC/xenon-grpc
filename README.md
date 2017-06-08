gRPC (http://www.grpc.io/) server for Xenon (https://nlesc.github.io/Xenon/).

Can be used to use Xenon in a non-java based language. 
For example pyxenon (https://github.com/NLeSC/pyxenon) will use the Xenon gRPC server in the future. 

[![Build Status](https://travis-ci.org/NLeSC/xenon-grpc.svg?branch=master)](https://travis-ci.org/NLeSC/xenon-grpc)
[![Build status](https://ci.appveyor.com/api/projects/status/tep8bad05e76a69w/branch/master?svg=true)](https://ci.appveyor.com/project/NLeSC/xenon-grpc/branch/master)
[![SonarCloud Gate](https://sonarcloud.io/api/badges/gate?key=nlesc:xenon-grpc)](https://sonarcloud.io/dashboard?id=nlesc:xenon-grpc)
[![SonarCloud Coverage](https://sonarcloud.io/api/badges/measure?key=nlesc:xenon-grpc&metric=coverage)](https://sonarcloud.io/component_measures/domain/Coverage?id=nlesc:xenon-grpc)

# Develop

## Run server

```bash
./gradlew installShadowDist
./build/install/xenon-grpc-shadow/bin/xenon-grpc
```

## Run client

For use polyglot

```bash
wget https://github.com/grpc-ecosystem/polyglot/releases/download/v1.2.0/polyglot.jar
java -jar polyglot.jar --command=list_services  --proto_discovery_root=src/main/proto
echo {} | java -jar polyglot.jar --endpoint=localhost:50051 --proto_discovery_root=src/main/proto --full_method=xenon.XenonJobs/getAdaptorDescriptions --command=call
```

## grpc gateway

JSON REST api around server with https://github.com/grpc-ecosystem/grpc-gateway

TODO

## Python client

```
cd src/main/python
pip install -r requirements.txt
# compile proto into python stubs
python -m grpc_tools.protoc -I../proto --python_out=. --grpc_python_out=. ../proto/xenon.proto
python client.py
```

## Mutual TLS

Create self-signed certificate and use for server and client on same machine.
Make sure `Common Name` field is filled with hostname of machine.
See http://httpd.apache.org/docs/2.4/ssl/ssl_faq.html#selfcert


```bash
openssl req -new -x509 -nodes -out server.crt -keyout server.key
./build/install/xenon-grpc/bin/xenon-grpc --server-cert-chain server.crt --server-private-key server.key --client-cert-chain server.crt
```

In a ipython shell in src/main/python dir
```python
import grpc
import xenon_pb2
import xenon_pb2_grpc
import socket

creds = grpc.ssl_channel_credentials(
    root_certificates=open('../../../server.crt').read(),
    private_key=open('../../../server.key', 'rb').read(),
    certificate_chain=open('../../../server.crt', 'rb').read()
)
channel = grpc.secure_channel(socket.gethostname() + ':50051', creds)
stub = xenon_pb2_grpc.XenonJobsStub(channel)
response = stub.getAdaptorDescriptions(xenon_pb2.Empty())
print(response)
```
