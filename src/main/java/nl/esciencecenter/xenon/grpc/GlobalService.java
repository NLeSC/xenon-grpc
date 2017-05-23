package nl.esciencecenter.xenon.grpc;

import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

public class GlobalService extends XenonGlobalGrpc.XenonGlobalImplBase {
    private final XenonSingleton singleton;

    GlobalService(XenonSingleton singleton) {
        super();
        this.singleton = singleton;
    }

    @Override
    public void newXenon(XenonProto.Properties request, StreamObserver<XenonProto.Empty> responseObserver) {
        try {
            singleton.setProperties(request.getPropertiesMap());
            responseObserver.onNext(XenonProto.Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }
}
