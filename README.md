Experiment to combine Xenon (nlesc.github.io/Xenon/) and gRpc (http://www.grpc.io/).

# Run server

```bash
./gradlew installDist
./build/install/xenon-grpc/bin/xenon-grpc
```

# Run client

For use polyglot

```bash
wget https://github.com/grpc-ecosystem/polyglot/releases/download/v1.2.0/polyglot.jar
java -jar polyglot.jar --command=list_services  --proto_discovery_root=src/main/proto
echo {} | java -jar polyglot.jar --endpoint=localhost:50051 --proto_discovery_root=src/main/proto --full_method=xenon.XenonJobs/GetAdaptorStatuses --command=call
```

# grpc gateway

JSON REST api around server with https://github.com/grpc-ecosystem/grpc-gateway

TODO

# Python client

```
cd src/main/python
pip install -r requirements.txt
# compile proto into python stubs
python -m grpc_tools.protoc -I../proto --python_out=. --grpc_python_out=. ../proto/xenon.proto
python client.py
```