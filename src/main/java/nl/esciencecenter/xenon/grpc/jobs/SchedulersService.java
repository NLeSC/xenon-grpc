package nl.esciencecenter.xenon.grpc.jobs;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import nl.esciencecenter.xenon.UnknownAdaptorException;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.credentials.Credential;
import nl.esciencecenter.xenon.grpc.Parsers;
import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.grpc.XenonSchedulersGrpc;
import nl.esciencecenter.xenon.schedulers.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static nl.esciencecenter.xenon.grpc.MapUtils.empty;
import static nl.esciencecenter.xenon.grpc.jobs.MapUtils.*;


public class SchedulersService extends XenonSchedulersGrpc.XenonSchedulersImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchedulersService.class);

    private final Map<String, SchedulerContainer> schedulers = new ConcurrentHashMap<>();
    private final Map<String, XenonProto.Scheduler> currentJobs = new ConcurrentHashMap<>();

    @Override
    public void createScheduler(XenonProto.CreateSchedulerRequest request, StreamObserver<XenonProto.Scheduler> responseObserver) {
        try {
            Credential credential = Parsers.parseCredential(request.getDefaultCred(), request.getPassword(), request.getCertificate());
            Scheduler scheduler = Scheduler.create(
                request.getAdaptor(),
                request.getLocation(),
                credential,
                request.getPropertiesMap()
            );

            String id = scheduler.getAdaptorName() + "://" + credential.getUsername() + "@" + scheduler.getLocation();
            schedulers.put(id, new SchedulerContainer(request, scheduler));

            XenonProto.Scheduler value = XenonProto.Scheduler.newBuilder()
                .setId(id)
                .setRequest(request)
                .build();
            responseObserver.onNext(value);
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getAdaptorDescription(XenonProto.AdaptorName request, StreamObserver<XenonProto.SchedulerAdaptorDescription> responseObserver) {
        try {
            SchedulerAdaptorDescription descIn = Scheduler.getAdaptorDescription(request.getName());
            XenonProto.SchedulerAdaptorDescription description = mapJobAdaptorDescription(descIn);
            responseObserver.onNext(description);
            responseObserver.onCompleted();
        } catch (UnknownAdaptorException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asException());
        }
    }

    @Override
    public void getAdaptorDescriptions(XenonProto.Empty request, StreamObserver<XenonProto.SchedulerAdaptorDescriptions> responseObserver) {
        SchedulerAdaptorDescription[] descriptions = Scheduler.getAdaptorDescriptions();

        XenonProto.SchedulerAdaptorDescriptions.Builder setBuilder = XenonProto.SchedulerAdaptorDescriptions.newBuilder();
        for (SchedulerAdaptorDescription descriptionIn : descriptions) {
            XenonProto.SchedulerAdaptorDescription description = mapJobAdaptorDescription(descriptionIn);
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
            XenonProto.CreateSchedulerRequest schedulerRequest = entry.getValue().getRequest();
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
        try {
            Scheduler scheduler = getScheduler(request);
            scheduler.close();
            schedulers.remove(request.getId());
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
        responseObserver.onNext(empty());
        responseObserver.onCompleted();
    }

    @Override
    public void localScheduler(XenonProto.Empty request, StreamObserver<XenonProto.Scheduler> responseObserver) {
        XenonProto.CreateSchedulerRequest schedulerRequest = XenonProto.CreateSchedulerRequest.newBuilder().setAdaptor("local").build();
        createScheduler(schedulerRequest, responseObserver);
    }

    @Override
    public void getDefaultQueueName(XenonProto.Scheduler request, StreamObserver<XenonProto.Queue> responseObserver) {
        try {
            Scheduler scheduler = getScheduler(request);
            String queue = scheduler.getDefaultQueueName();
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
            String[] queues = scheduler.getQueueNames();
            responseObserver.onNext(XenonProto.Queues.newBuilder().addAllName(Arrays.asList(queues)).build());
            responseObserver.onCompleted();
        } catch (StatusException e) {
            responseObserver.onError(e);
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        }
    }

    @Override
    public void isOpen(XenonProto.Scheduler request, StreamObserver<XenonProto.Is> responseObserver) {
        try {
            Scheduler scheduler = getScheduler(request);
            boolean open = scheduler.isOpen();
            responseObserver.onNext(XenonProto.Is.newBuilder().setValue(open).build());
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getQueueStatus(XenonProto.SchedulerAndQueue request, StreamObserver<XenonProto.QueueStatus> responseObserver) {
        try {
            Scheduler scheduler = getScheduler(request.getScheduler());
            QueueStatus status = scheduler.getQueueStatus(request.getQueue());
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
        try {
            Scheduler scheduler = getScheduler(request.getScheduler());
            String[] queues = request.getQueuesList().toArray(new String[0]);
            QueueStatus[] statuses = scheduler.getQueueStatuses(queues);
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
    public void submitBatchJob(XenonProto.SubmitBatchJobRequest request, StreamObserver<XenonProto.Job> responseObserver) {
        try {
            XenonProto.Scheduler requestScheduler = request.getScheduler();
            Scheduler scheduler = getScheduler(requestScheduler);
            XenonProto.JobDescription requestDescription = request.getDescription();
            JobDescription description = mapJobDescription(requestDescription);

            String jobIdentifier = scheduler.submitBatchJob(description);
            currentJobs.put(jobIdentifier, requestScheduler);

            XenonProto.Job response = XenonProto.Job.newBuilder()
                .setId(jobIdentifier)
                .setScheduler(request.getScheduler())
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
        try {
            Scheduler scheduler = getScheduler(request.getScheduler());

            JobStatus status = scheduler.cancelJob(request.getId());

            XenonProto.JobStatus response = mapJobStatus(status, request.getScheduler());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (NoSuchJobException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asException());
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getJobStatus(XenonProto.Job request, StreamObserver<XenonProto.JobStatus> responseObserver) {
        try {
            Scheduler scheduler = getScheduler(request.getScheduler());

            JobStatus status = scheduler.getJobStatus(request.getId());

            XenonProto.JobStatus response = mapJobStatus(status, request.getScheduler());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (NoSuchJobException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asException());
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void waitUntilDone(XenonProto.Job request, StreamObserver<XenonProto.JobStatus> responseObserver) {
        try {
            Scheduler scheduler = getScheduler(request.getScheduler());

            JobStatus status = scheduler.waitUntilDone(request.getId(), Long.MAX_VALUE);

            XenonProto.JobStatus response = mapJobStatus(status, request.getScheduler());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (NoSuchJobException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asException());
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void waitUntilRunning(XenonProto.Job request, StreamObserver<XenonProto.JobStatus> responseObserver) {
        try {
            Scheduler scheduler = getScheduler(request.getScheduler());

            JobStatus status = scheduler.waitUntilRunning(request.getId(), Long.MAX_VALUE);

            XenonProto.JobStatus response = mapJobStatus(status, request.getScheduler());
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (NoSuchJobException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asException());
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getJobStatuses(XenonProto.Jobs request, StreamObserver<XenonProto.JobStatuses> responseObserver) {
        Jobs jobs = singleton.getInstance().jobs();

        List<JobContainer> jobContainers = request.getJobsList()
            .stream()
            .filter(d -> currentJobs.containsKey(d.getId()))
            .map(d -> currentJobs.get(d.getId()))
            .collect(Collectors.toList());
        Job[] myjobs = jobContainers.stream()
            .map(JobContainer::getJob)
            .collect(Collectors.toList())
            .toArray(new Job[0]);
        Iterator<XenonProto.JobDescription> descriptions = jobContainers.stream()
            .map(JobContainer::getRequest)
            .map(XenonProto.SubmitJobRequest::getDescription)
            .iterator();
        Iterator<JobStatus> statuses = Arrays.asList(jobs.getJobStatuses(myjobs)).iterator();

        XenonProto.JobStatuses.Builder builder = XenonProto.JobStatuses.newBuilder();
        // TODO check statuses are returned in same order as array of argument
        while (statuses.hasNext() && descriptions.hasNext()) {
            builder.addStatuses(mapJobStatus(statuses.next(), descriptions.next(), scheduler));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteJob(XenonProto.Job request, StreamObserver<XenonProto.Empty> responseObserver) {
        try {
            Job job = getJob(request);
            cancelJob(job);
            currentJobs.remove(request.getId());
            responseObserver.onNext(empty());
            responseObserver.onCompleted();
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    private void cancelJob(Job job) throws StatusException {
        try {
            Jobs jobs = singleton.getInstance().jobs();
            JobStatus status = jobs.getJobStatus(job);
            if (!status.isDone()) {
                jobs.cancelJob(job);
            }
        } catch (XenonException e) {
            // when job is done then Xenon has forgotten about job,
            // so only if xenon knows about job can it be canceled
            if (!e.getMessage().contains("Job not found")) {
                throw Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException();
            }
        }
    }

    @Override
    public void listJobs(XenonProto.Empty request, StreamObserver<XenonProto.Jobs> responseObserver) {
        XenonProto.Jobs.Builder builder = XenonProto.Jobs.newBuilder();
        for (Map.Entry<String, JobContainer> entry : currentJobs.entrySet()) {
            builder.addJobs(mapJob(entry.getKey(), entry.getValue().getRequest().getDescription()));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getJobs(XenonProto.SchedulerAndQueues request, StreamObserver<XenonProto.Jobs> responseObserver) {
        try {
            XenonProto.Scheduler schedulerProto = request.getScheduler();
            Scheduler scheduler = getScheduler(schedulerProto);
            String[] queues = request.getQueuesList().toArray(new String[0]);
            String[] jobsOfScheduler = scheduler.getJobs(queues);

            XenonProto.Jobs response = getJobs(schedulerProto, jobsOfScheduler);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    private XenonProto.Jobs getJobs(XenonProto.Scheduler schedulerProto, String[] jobsOfScheduler) throws StatusException {
        XenonProto.Jobs.Builder builder = XenonProto.Jobs.newBuilder();
        XenonProto.Job.Builder jobBuilder = XenonProto.Job.newBuilder();
        XenonProto.SubmitJobRequest.Builder jobRequestBuilder = XenonProto.SubmitJobRequest.newBuilder().setScheduler(schedulerProto);
        for (String job : jobsOfScheduler) {
            XenonProto.JobDescription description;
            if (currentJobs.containsKey(job) && schedulerProto.equals(getSchedulerOfJob(job))) {
                // Use existing job from currentJobs map
                description = currentJobs.get(job).getRequest().getDescription();
            } else {
                description = mapJobDescription(job, schedulerProto);
                XenonProto.SubmitJobRequest jobRequest = jobRequestBuilder.setDescription(description).build();
                // Register jobs of jobsOfScheduler into myjobs
                currentJobs.put(job, schedulerProto);
            }
            XenonProto.Job protoJob = jobBuilder.setId(job).setDescription(description).build();
            builder.addJobs(protoJob);
        }
        return builder.build();
    }

    private XenonProto.Scheduler getSchedulerOfJob(String job) throws StatusException {
        if (!currentJobs.containsKey(job)) {
            throw Status.NOT_FOUND.augmentDescription(job).asException();
        }
        return currentJobs.get(job);
    }

    @Override
    public StreamObserver<XenonProto.SubmitInteractiveJobRequest> submitInteractiveJob(StreamObserver<XenonProto.SubmitInteractiveJobResponse> responseObserver) {
        return new StreamObserver<XenonProto.SubmitInteractiveJobRequest>() {
            private JobOutputStreamsForwarder forwarder;
            private Streams streams;

            @Override
            public void onNext(XenonProto.SubmitInteractiveJobRequest value) {
                try {
                    if (streams == null) {
                        Scheduler scheduler = getScheduler(value.getScheduler());
                        XenonProto.JobDescription requestDescription = value.getDescription();
                        JobDescription description = mapJobDescription(requestDescription);
                        streams = scheduler.submitInteractiveJob(description);
                        XenonProto.Job job = XenonProto.Job.newBuilder().setScheduler(value.getScheduler()).setId(streams.getJobIdentifier()).build();
                        forwarder = new JobOutputStreamsForwarder(responseObserver, streams.getStderr(), streams.getStdout(), job);
                    }
                    // write incoming stdin to xenons stdin
                    streams.getStdin().write(value.getStdin().toByteArray());
                    streams.getStdin().flush();
                } catch (XenonException | IOException e) {
                    responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
                } catch (StatusException e) {
                    responseObserver.onError(e);
                }
            }

            @Override
            public void onError(Throwable t) {
                if (streams != null) {
                    try {
                        LOGGER.warn("Error from client", t);
                        streams.getStdin().close();
                        forwarder.close();
                    } catch (IOException e) {
                        LOGGER.warn("Error from server", e);
                    }
                }
            }

            @Override
            public void onCompleted() {
                if (streams != null) {
                    try {
                        streams.getStdin().close();
                    } catch (IOException e) {
                        LOGGER.warn("Error from server", e);
                    }
                }
            }
        };
    }
}
