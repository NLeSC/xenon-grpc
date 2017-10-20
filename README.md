gRPC (http://www.grpc.io/) server for Xenon (https://nlesc.github.io/Xenon/).

Can be used to use Xenon in a non-java based language. 
For example pyxenon (https://github.com/NLeSC/pyxenon) will use the Xenon gRPC server in the future. 

[![Build Status](https://travis-ci.org/NLeSC/xenon-grpc.svg?branch=master)](https://travis-ci.org/NLeSC/xenon-grpc)
[![Build status](https://ci.appveyor.com/api/projects/status/tep8bad05e76a69w/branch/master?svg=true)](https://ci.appveyor.com/project/NLeSC/xenon-grpc/branch/master)
[![SonarCloud Gate](https://sonarcloud.io/api/badges/gate?key=nl.esciencecenter.xenon.grpc:xenon-grpc)](https://sonarcloud.io/dashboard?id=nl.esciencecenter.xenon.grpc:xenon-grpc)
[![SonarCloud Coverage](https://sonarcloud.io/api/badges/measure?key=nl.esciencecenter.xenon.grpc:xenon-grpc&metric=coverage)](https://sonarcloud.io/component_measures/domain/Coverage?id=nl.esciencecenter.xenon.grpc:xenon-grpc)

# Install

On [releases page](https://github.com/NLeSC/xenon-grpc/releases) download a tarball (or zipfile).

The tarball can be installed with:
```bash
tar -xf xenon-grpc-shadow*.tar
```
Add `xenon-grpc*/bin` to your PATH environment variable for easy usage.

# Usage

To start the grpc server with default arguments run

```bash
./xenon-grpc*/bin/xenon-grpc
```

To get help run

```bash
./xenon-grpc*/bin/xenon-grpc --help
```

Or call the jar directly with
```bash
java -jar xenon-grpc-*/lib/xenon-grpc-*-all.jar
```

# Development

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

## Python client

Compile proto into python stubs
```
pip install grpcio grpcio-tools
xenon-grpc --proto > xenon.proto
python -m grpc_tools.protoc -I../proto --python_out=. --grpc_python_out=. xenon.proto
```

Now use the generated stubs, see https://grpc.io/docs/tutorials/basic/python.html#creating-the-client

## Mutual TLS

Create self-signed certificate and use for server and client on same machine.
Make sure `Common Name` field is filled with hostname of machine.
See http://httpd.apache.org/docs/2.4/ssl/ssl_faq.html#selfcert


```bash
openssl req -new -x509 -nodes -out server.crt -keyout server.key
./build/install/xenon-grpc/bin/xenon-grpc --server-cert-chain server.crt --server-private-key server.key --client-cert-chain server.crt
```

In a ipython shell with generated stubs in working directory:
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

## New release

```
./gradlew build
```

Generates application tar/zip in `build/distributions/` directory.

1. Create a new GitHub release
2. Upload the files in `build/distributions/` directory to that release
3. Publish release
