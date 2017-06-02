package nl.esciencecenter.xenon.grpc;

import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import nl.esciencecenter.xenon.AdaptorStatus;
import nl.esciencecenter.xenon.Xenon;
import nl.esciencecenter.xenon.XenonPropertyDescription;

import java.util.List;

import static nl.esciencecenter.xenon.grpc.MapUtils.mapPropertyDescriptions;

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

    @Override
    public void getSupportedProperties(XenonProto.Empty request, StreamObserver<XenonProto.PropertyDescriptions> responseObserver) {
        Xenon xenon = singleton.getInstance();
        // TODO use xenon.getSupportedProperties(), when https://github.com/NLeSC/Xenon/issues/430 is completed
        AdaptorStatus[] statuses = xenon.getAdaptorStatuses();

        XenonProto.PropertyDescriptions.Builder setBuilder = XenonProto.PropertyDescriptions.newBuilder();
        for (AdaptorStatus status : statuses) {
            List<XenonProto.PropertyDescription> adaptorProps = mapPropertyDescriptions(status, XenonPropertyDescription.Component.XENON);
            setBuilder.addAllProperties(adaptorProps);
        }
        responseObserver.onNext(setBuilder.build());
        responseObserver.onCompleted();
    }
}
