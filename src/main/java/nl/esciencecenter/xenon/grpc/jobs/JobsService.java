package nl.esciencecenter.xenon.grpc.jobs;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import nl.esciencecenter.xenon.AdaptorStatus;
import nl.esciencecenter.xenon.Xenon;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.XenonPropertyDescription;
import nl.esciencecenter.xenon.grpc.XenonJobsGrpc;
import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.grpc.XenonSingleton;
import nl.esciencecenter.xenon.jobs.Scheduler;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public class JobsService extends XenonJobsGrpc.XenonJobsImplBase {
    private final XenonSingleton singleton;
    private final Map<String, SchedulerContainer> schedulers = new ConcurrentHashMap<>();

    public JobsService(XenonSingleton singleton) {
        super();
        this.singleton = singleton;
    }

    @Override
    public void newScheduler(XenonProto.NewSchedulerRequest request, StreamObserver<XenonProto.Scheduler> responseObserver) {
        try {
            Xenon xenon = singleton.getInstance();

            System.err.print("Location empty = ");
            System.err.println(request.getLocation().isEmpty());

            // TODO remove this experiment
            if (!"local".equals(request.getAdaptor()) && request.getLocation().isEmpty()) {
                throw new XenonException(request.getAdaptor(), "Location can not be empty");
            }

            Scheduler scheduler = xenon.jobs().newScheduler(
                    request.getAdaptor(),
                    request.getLocation(),
                    // TODO implement credentials
                    null,
                    request.getPropertiesMap()
            );

            // TODO use more unique id, maybe use new label/alias field from request or a uuid
            String id = scheduler.getAdaptorName() + ":" + scheduler.getLocation();
            schedulers.put(id, new SchedulerContainer(request, scheduler));

            XenonProto.Scheduler value = XenonProto.Scheduler.newBuilder()
                    .setId(id)
                    .setRequest(request)
                    .build();
            responseObserver.onNext(value);
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asException());
        }
    }

    @Override
    public void getAdaptorDescriptions(XenonProto.Empty request, StreamObserver<XenonProto.JobAdaptorDescriptions> responseObserver) {
        Xenon xenon = singleton.getInstance();
        // TODO use xenon.jobs().getAdaptorDescriptions(), when https://github.com/NLeSC/Xenon/issues/430 is completed
        AdaptorStatus[] statuses = xenon.getAdaptorStatuses();

        XenonProto.JobAdaptorDescriptions.Builder setBuilder = XenonProto.JobAdaptorDescriptions.newBuilder();
        XenonProto.PropertyDescription.Builder propBuilder = XenonProto.PropertyDescription.newBuilder();
        XenonProto.JobAdaptorDescription.Builder builder = XenonProto.JobAdaptorDescription.newBuilder();
        for (AdaptorStatus status : statuses) {
            List<XenonProto.PropertyDescription> supportedProperties = Arrays.stream(status.getSupportedProperties())
                .filter(p -> p.getLevels().contains(XenonPropertyDescription.Component.SCHEDULER))
                .map(p -> propBuilder
                    .setName(p.getName())
                    .setDescription(p.getDescription())
                    .setDefaultValue(p.getDefaultValue())
                    // TODO map p.getType() to XenonProto.PropertyDescription.Type
                    //.setType(p.getType())
                    .build()
                ).collect(Collectors.toList());

            XenonProto.JobAdaptorDescription description = builder
                .setName(status.getName())
                .setDescription(status.getDescription())
                .addAllSupportedLocations(Arrays.asList(status.getSupportedLocations()))
                .addAllSupportedProperties(supportedProperties)
                .build();
            setBuilder.addDescriptions(description);
        }
        responseObserver.onNext(setBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void listSchedulers(XenonProto.Empty request, StreamObserver<XenonProto.Schedulers> responseObserver) {
        XenonProto.Schedulers.Builder setBuilder = XenonProto.Schedulers.newBuilder();
        XenonProto.Scheduler.Builder builder = XenonProto.Scheduler.newBuilder();

        for (Map.Entry<String, SchedulerContainer> entry : schedulers.entrySet()) {
            XenonProto.NewSchedulerRequest schedulerRequest = entry.getValue().getRequest();
            setBuilder.addSchedulers(builder
                .setId(entry.getKey())
                .setRequest(schedulerRequest)
            );
        }

        responseObserver.onNext(setBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void close(XenonProto.Scheduler request, StreamObserver<XenonProto.Empty> responseObserver) {
        if (schedulers.containsKey(request.getId())) {
            responseObserver.onError(Status.NOT_FOUND.asException());
        }
        SchedulerContainer container = schedulers.get(request.getId());
        try {
            singleton.getInstance().jobs().close(container.getScheduler());
            schedulers.remove(request.getId());
        } catch (XenonException e) {
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asException());
        }
        responseObserver.onNext(XenonProto.Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void localScheduler(XenonProto.Empty request, StreamObserver<XenonProto.Scheduler> responseObserver) {
        XenonProto.NewSchedulerRequest schedulerRequest = XenonProto.NewSchedulerRequest.newBuilder().setAdaptor("local").build();
        newScheduler(schedulerRequest, responseObserver);
    }
}
