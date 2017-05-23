package nl.esciencecenter.xenon.grpc.jobs;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import nl.esciencecenter.xenon.AdaptorStatus;
import nl.esciencecenter.xenon.Xenon;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.XenonPropertyDescription;
import nl.esciencecenter.xenon.credentials.Credential;
import nl.esciencecenter.xenon.grpc.Parsers;
import nl.esciencecenter.xenon.grpc.XenonJobsGrpc;
import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.grpc.XenonSingleton;
import nl.esciencecenter.xenon.jobs.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static nl.esciencecenter.xenon.grpc.jobs.Writers.*;


public class JobsService extends XenonJobsGrpc.XenonJobsImplBase {
    private final XenonSingleton singleton;
    private final Map<String, SchedulerContainer> schedulers = new ConcurrentHashMap<>();
    private final Map<String, JobContainer> currentJobs = new ConcurrentHashMap<>();

    public JobsService(XenonSingleton singleton) {
        super();
        this.singleton = singleton;
    }

    @Override
    public void newScheduler(XenonProto.NewSchedulerRequest request, StreamObserver<XenonProto.Scheduler> responseObserver) {
        Jobs jobs = singleton.getInstance().jobs();
        try {
            Credential credential = Parsers.parseCredential(singleton.getInstance(), request.getPassword(), request.getCertificate());
            Scheduler scheduler = jobs.newScheduler(
                    request.getAdaptor(),
                    request.getLocation(),
                    credential,
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
        } catch (StatusException e) {
            responseObserver.onError(e);
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

    @Override
    public void getDefaultQueueName(XenonProto.Scheduler request, StreamObserver<XenonProto.Queue> responseObserver) {
        Jobs jobs = singleton.getInstance().jobs();
        try {
            Scheduler scheduler = getScheduler(request);
            String queue = jobs.getDefaultQueueName(scheduler);
            responseObserver.onNext(XenonProto.Queue.newBuilder().setName(queue).build());
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    private Scheduler getScheduler(XenonProto.Scheduler request) throws StatusException {
        String id = request.getId();
        if (!schedulers.containsKey(id)) {
            throw Status.NOT_FOUND.augmentDescription(id).asException();
        }
        return schedulers.get(id).getScheduler();
    }

    @Override
    public void getQueues(XenonProto.Scheduler request, StreamObserver<XenonProto.Queues> responseObserver) {
        try {
            Scheduler scheduler = getScheduler(request);
            // TODO change to jobs.getQueueNames(scheduler) when Xenon v2 is released
            String[] queues = scheduler.getQueueNames();
            responseObserver.onNext(XenonProto.Queues.newBuilder().addAllName(Arrays.asList(queues)).build());
            responseObserver.onCompleted();
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void isOpen(XenonProto.Scheduler request, StreamObserver<XenonProto.Is> responseObserver) {
        Jobs jobs = singleton.getInstance().jobs();
        try {
            Scheduler scheduler = getScheduler(request);
            boolean open = jobs.isOpen(scheduler);
            responseObserver.onNext(XenonProto.Is.newBuilder().setIs(open).build());
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getQueueStatus(XenonProto.SchedulerAndQueue request, StreamObserver<XenonProto.QueueStatus> responseObserver) {
        Jobs jobs = singleton.getInstance().jobs();
        try {
            Scheduler scheduler = getScheduler(request.getScheduler());
            QueueStatus status = jobs.getQueueStatus(scheduler, request.getQueue());
            XenonProto.QueueStatus response = mapQueueStatus(status, request.getScheduler());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getQueueStatuses(XenonProto.SchedulerAndQueues request, StreamObserver<XenonProto.QueueStatuses> responseObserver) {
        Jobs jobs = singleton.getInstance().jobs();
        try {
            Scheduler scheduler = getScheduler(request.getScheduler());
            String[] queues = request.getQueuesList().toArray(new String[0]);
            QueueStatus[] statuses = jobs.getQueueStatuses(scheduler, queues);
            XenonProto.QueueStatuses.Builder builder = XenonProto.QueueStatuses.newBuilder();
            for (QueueStatus status : statuses) {
                builder.addStatuses(mapQueueStatus(status, request.getScheduler()));
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void submitJob(XenonProto.SubmitJobRequest request, StreamObserver<XenonProto.Job> responseObserver) {
        Jobs jobs = singleton.getInstance().jobs();
        try {
            Scheduler scheduler = getScheduler(request.getScheduler());
            JobDescription description = mapJobDescription(request);

            Job job = jobs.submitJob(scheduler, description);
            currentJobs.put(job.getIdentifier(), new JobContainer(request, job));

            XenonProto.Job response = XenonProto.Job.newBuilder()
                    .setId(job.getIdentifier())
                    .setDescription(request.getDescription())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void cancelJob(XenonProto.Job request, StreamObserver<XenonProto.JobStatus> responseObserver) {
        Jobs jobs = singleton.getInstance().jobs();
        try {
            Job job = getJob(request);

            JobStatus status = jobs.cancelJob(job);

            XenonProto.JobStatus response = mapJobStatus(status, request.getDescription());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    private Job getJob(XenonProto.Job request) throws StatusException {
        String id = request.getId();
        if (!currentJobs.containsKey(id)) {
            throw Status.NOT_FOUND.augmentDescription(id).asException();
        }
        return currentJobs.get(id).getJob();
    }

    @Override
    public void getJobStatus(XenonProto.Job request, StreamObserver<XenonProto.JobStatus> responseObserver) {
        Jobs jobs = singleton.getInstance().jobs();
        try {
            Job job = getJob(request);

            JobStatus status = jobs.getJobStatus(job);

            XenonProto.JobStatus response = mapJobStatus(status, request.getDescription());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void waitUntilDone(XenonProto.Job request, StreamObserver<XenonProto.JobStatus> responseObserver) {
        Jobs jobs = singleton.getInstance().jobs();
        try {
            Job job = getJob(request);

            JobStatus status = jobs.waitUntilDone(job, Long.MAX_VALUE);

            XenonProto.JobStatus response = mapJobStatus(status, request.getDescription());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void waitUntilRunning(XenonProto.Job request, StreamObserver<XenonProto.JobStatus> responseObserver) {
        Jobs jobs = singleton.getInstance().jobs();
        try {
            Job job = getJob(request);

            JobStatus status = jobs.waitUntilRunning(job, Long.MAX_VALUE);

            XenonProto.JobStatus response = mapJobStatus(status, request.getDescription());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }
}
