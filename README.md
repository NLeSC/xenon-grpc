Experiment to combine Xenon (https://nlesc.github.io/Xenon/) and gRpc (http://www.grpc.io/).

[![Build Status](https://travis-ci.org/NLeSC/xenon-cli.svg?branch=master)](https://travis-ci.org/NLeSC/xenon-cli)
[![Build status](https://ci.appveyor.com/api/projects/status/tep8bad05e76a69w/branch/master?svg=true)](https://ci.appveyor.com/project/NLeSC/xenon-cli/branch/master)
[![SonarQube Gate](https://sonarqube.com/api/badges/gate?key=nlesc%3Axenon-cli)](https://sonarqube.com/dashboard?id=nlesc%3Axenon-cli)
[![SonarQube Coverage](https://sonarqube.com/api/badges/measure?key=nlesc%3Axenon-cli&metric=coverage)](https://sonarqube.com/component_measures/domain/Coverage?id=nlesc%3Axenon-cli)

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
