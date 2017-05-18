package nl.esciencecenter.xenon.grpc;

import nl.esciencecenter.xenon.Xenon;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.XenonFactory;
import nl.esciencecenter.xenon.jobs.Scheduler;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public class XenonJobsImpl extends XenonJobsGrpc.XenonJobsImplBase {
    @Override
    public void newScheduler(XenonProto.NewSchedulerRequest request, StreamObserver<XenonProto.Scheduler> responseObserver) {
        try {
            // TODO dont do this here
            Xenon xenon = XenonFactory.newXenon(null);

            System.err.print("Location empty = ");
            System.err.println(request.getLocation().isEmpty());

            if (!"local".equals(request.getAdaptor()) && request.getLocation().isEmpty()) {
                throw new XenonException(request.getAdaptor(), "Location can not be empty");
            }

            Scheduler scheduler = xenon.jobs().newScheduler(
                    request.getAdaptor(),
                    request.getLocation(),
                    null,
                    null
            );

            // TODO use more unique id, maybe use new label/alias field from request or a uuid
            String id = scheduler.getAdaptorName() + ":" + scheduler.getLocation();
            // TODO store scheduler some where so it can be retrieved in other methods

            XenonProto.Scheduler value = XenonProto.Scheduler.newBuilder()
                    .setId(id)
                    .setRequest(request)
                    .build();
            responseObserver.onNext(value);
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asException());
        } finally {
            // TODO dont do this here
            XenonFactory.endAll();
        }
    }
}
