package nl.esciencecenter.xenon.grpc;

import io.grpc.stub.StreamObserver;
import nl.esciencecenter.xenon.AdaptorStatus;
import nl.esciencecenter.xenon.Xenon;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.XenonFactory;

import java.util.Arrays;

public class XenonJobsImpl extends XenonJobsGrpc.XenonJobsImplBase {
    @Override
    public void getAdaptorStatuses(Empty request, StreamObserver<AdaptorStatuses> responseObserver) {
        AdaptorStatus[] adaptors = getAdaptorStatuses();
        AdaptorStatuses.Builder builder = AdaptorStatuses.newBuilder();
        for (AdaptorStatus adaptor : adaptors) {
            builder.addAdaptors(
                MAdaptorStatus.newBuilder()
                        .setName(adaptor.getName())
                        .addAllSchemes(Arrays.asList(adaptor.getSupportedSchemes()))
            );
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    private AdaptorStatus[] getAdaptorStatuses() {
        AdaptorStatus[] adaptors = {};
        try {
            Xenon xenon = XenonFactory.newXenon(null);
            adaptors = xenon.getAdaptorStatuses();
            XenonFactory.endXenon(xenon);
        } catch (XenonException e) {
            e.printStackTrace();
        }
        return adaptors;
    }
}
