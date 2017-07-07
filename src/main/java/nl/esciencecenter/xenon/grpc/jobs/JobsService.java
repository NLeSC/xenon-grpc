package nl.esciencecenter.xenon.grpc.jobs;

import static nl.esciencecenter.xenon.grpc.MapUtils.empty;
import static nl.esciencecenter.xenon.grpc.jobs.MapUtils.mapJob;
import static nl.esciencecenter.xenon.grpc.jobs.MapUtils.mapJobAdaptorDescription;
import static nl.esciencecenter.xenon.grpc.jobs.MapUtils.mapJobDescription;
import static nl.esciencecenter.xenon.grpc.jobs.MapUtils.mapJobStatus;
import static nl.esciencecenter.xenon.grpc.jobs.MapUtils.mapQueueStatus;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import nl.esciencecenter.xenon.AdaptorStatus;
import nl.esciencecenter.xenon.Xenon;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.credentials.Credential;
import nl.esciencecenter.xenon.grpc.Parsers;
import nl.esciencecenter.xenon.grpc.XenonJobsGrpc;
import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.grpc.XenonSingleton;
import nl.esciencecenter.xenon.jobs.Job;
import nl.esciencecenter.xenon.jobs.JobDescription;
import nl.esciencecenter.xenon.jobs.JobStatus;
import nl.esciencecenter.xenon.jobs.Jobs;
import nl.esciencecenter.xenon.jobs.QueueStatus;
import nl.esciencecenter.xenon.jobs.Scheduler;
import nl.esciencecenter.xenon.jobs.Streams;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JobsService extends XenonJobsGrpc.XenonJobsImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobsService.class);

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
            Credential credential = Parsers.parseCredential(singleton.getInstance(), request.getAdaptor(), request.getPassword(), request.getCertificate());
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
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(e.getMessage()).withCause(e).asException());
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
        HashSet<String> jobBasedAdaptors = new HashSet<>(Arrays.asList("local", "ssh", "gridengine", "slurm", "torque"));
        for (AdaptorStatus status : statuses) {
            if (jobBasedAdaptors.contains(status.getName())) {
                XenonProto.JobAdaptorDescription description = mapJobAdaptorDescription(status);
                setBuilder.addDescriptions(description);
            }
        }
        responseObserver.onNext(setBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAdaptorDescription(XenonProto.AdaptorName request, StreamObserver<XenonProto.JobAdaptorDescription> responseObserver) {
        Xenon xenon = singleton.getInstance();
        try {
            // TODO use xenon.jobs().getAdaptorDescription(name), when https://github.com/NLeSC/Xenon/issues/430 is completed
            AdaptorStatus status = xenon.getAdaptorStatus(request.getName());
            XenonProto.JobAdaptorDescription description = mapJobAdaptorDescription(status);
            responseObserver.onNext(description);
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        }
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
        try {
            Scheduler scheduler = getScheduler(request);
            singleton.getInstance().jobs().close(scheduler);
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
            builder.addStatuses(mapJobStatus(statuses.next(), descriptions.next()));
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
        Jobs jobs = singleton.getInstance().jobs();
        try {
            XenonProto.Scheduler schedulerProto = request.getScheduler();
            Scheduler scheduler = getScheduler(schedulerProto);
            String[] queues = request.getQueuesList().toArray(new String[0]);
            Job[] jobsOfScheduler = jobs.getJobs(scheduler, queues);

            XenonProto.Jobs response = getJobs(schedulerProto, jobsOfScheduler);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    private XenonProto.Jobs getJobs(XenonProto.Scheduler schedulerProto, Job[] jobsOfScheduler) throws StatusException {
        XenonProto.Jobs.Builder builder = XenonProto.Jobs.newBuilder();
        XenonProto.Job.Builder jobBuilder = XenonProto.Job.newBuilder();
        XenonProto.SubmitJobRequest.Builder jobRequestBuilder = XenonProto.SubmitJobRequest.newBuilder().setScheduler(schedulerProto);
        for (Job job : jobsOfScheduler) {
            XenonProto.JobDescription description;
            if (currentJobs.containsKey(job.getIdentifier()) && schedulerProto.equals(getSchedulerOfJob(job))) {
                // Use existing job from currentJobs map
                description = currentJobs.get(job.getIdentifier()).getRequest().getDescription();
            } else {
                description = mapJobDescription(job, schedulerProto);
                XenonProto.SubmitJobRequest jobRequest = jobRequestBuilder.setDescription(description).build();
                // Register jobs of jobsOfScheduler into myjobs
                currentJobs.put(job.getIdentifier(), new JobContainer(jobRequest, job));
            }
            XenonProto.Job protoJob = jobBuilder.setId(job.getIdentifier()).setDescription(description).build();
            builder.addJobs(protoJob);
        }
        return builder.build();
    }

    private XenonProto.Scheduler getSchedulerOfJob(Job job) throws StatusException {
        String jid = job.getIdentifier();
        if (!currentJobs.containsKey(jid)) {
            throw Status.NOT_FOUND.augmentDescription(jid).asException();
        }
        return currentJobs.get(jid).getRequest().getScheduler();
    }

    @Override
    public StreamObserver<XenonProto.JobInputStream> getStreams(StreamObserver<XenonProto.JobOutputStreams> responseObserver) {
        return new StreamObserver<XenonProto.JobInputStream>() {
            private JobOutputStreamsForwarder forwarder;
            private Streams streams;

            @Override
            public void onNext(XenonProto.JobInputStream value) {
                try {
                    if (streams == null) {
                        if (XenonProto.Job.getDefaultInstance().equals(value.getJob())) {
                            throw Status.INVALID_ARGUMENT.augmentDescription("job value is required").asException();
                        }
                        Jobs jobs = singleton.getInstance().jobs();
                        Job job = getJob(value.getJob());
                        streams = jobs.getStreams(job);
                        forwarder = new JobOutputStreamsForwarder(responseObserver, streams.getStderr(), streams.getStdout());
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
