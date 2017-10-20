package nl.esciencecenter.xenon.grpc.schedulers;

import static nl.esciencecenter.xenon.grpc.MapUtils.empty;
import static nl.esciencecenter.xenon.grpc.MapUtils.mapCredential;
import static nl.esciencecenter.xenon.grpc.MapUtils.mapException;
import static nl.esciencecenter.xenon.grpc.filesystems.MapUtils.getFileSystemId;
import static nl.esciencecenter.xenon.grpc.schedulers.MapUtils.mapSchedulerAdaptorDescription;
import static nl.esciencecenter.xenon.grpc.schedulers.MapUtils.mapJobDescription;
import static nl.esciencecenter.xenon.grpc.schedulers.MapUtils.mapJobStatus;
import static nl.esciencecenter.xenon.grpc.schedulers.MapUtils.mapJobs;
import static nl.esciencecenter.xenon.grpc.schedulers.MapUtils.mapQueueStatus;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import nl.esciencecenter.xenon.credentials.DefaultCredential;
import nl.esciencecenter.xenon.filesystems.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.credentials.Credential;
import nl.esciencecenter.xenon.grpc.SchedulerServiceGrpc;
import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.schedulers.JobDescription;
import nl.esciencecenter.xenon.schedulers.JobStatus;
import nl.esciencecenter.xenon.schedulers.QueueStatus;
import nl.esciencecenter.xenon.schedulers.Scheduler;
import nl.esciencecenter.xenon.schedulers.SchedulerAdaptorDescription;
import nl.esciencecenter.xenon.schedulers.Streams;

public class SchedulerService extends SchedulerServiceGrpc.SchedulerServiceImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchedulerService.class);

    private final Map<String, Scheduler> schedulers = new ConcurrentHashMap<>();

    @Override
    public void create(XenonProto.CreateSchedulerRequest request, StreamObserver<XenonProto.Scheduler> responseObserver) {
        try {
            Credential credential = mapCredential(request);
            Scheduler scheduler = Scheduler.create(
                    request.getAdaptor(),
                    request.getLocation(),
                    credential,
                    request.getPropertiesMap()
            );

            String id = putScheduler(scheduler, credential.getUsername());

            XenonProto.Scheduler value = XenonProto.Scheduler.newBuilder()
                    .setId(id)
                    .build();
            responseObserver.onNext(value);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }

    String putScheduler(Scheduler scheduler, String username) throws StatusException {
        String id = scheduler.getAdaptorName() + "://" + username + "@" + scheduler.getLocation() + "#" + scheduler.hashCode();
        if (schedulers.containsKey(id)) {
            throw Status.ALREADY_EXISTS.augmentDescription("Scheduler with id: " + id).asException();
        } else {
            schedulers.put(id, scheduler);
        }
        return id;
    }

    @Override
    public void getAdaptorDescription(XenonProto.AdaptorName request, StreamObserver<XenonProto.SchedulerAdaptorDescription> responseObserver) {
        try {
            SchedulerAdaptorDescription descIn = Scheduler.getAdaptorDescription(request.getName());
            XenonProto.SchedulerAdaptorDescription description = mapSchedulerAdaptorDescription(descIn);
            responseObserver.onNext(description);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }

    @Override
    public void getAdaptorDescriptions(XenonProto.Empty request, StreamObserver<XenonProto.SchedulerAdaptorDescriptions> responseObserver) {
        SchedulerAdaptorDescription[] descriptions = Scheduler.getAdaptorDescriptions();

        XenonProto.SchedulerAdaptorDescriptions.Builder setBuilder = XenonProto.SchedulerAdaptorDescriptions.newBuilder();
        for (SchedulerAdaptorDescription descriptionIn : descriptions) {
            XenonProto.SchedulerAdaptorDescription description = mapSchedulerAdaptorDescription(descriptionIn);
            setBuilder.addDescriptions(description);
        }
        responseObserver.onNext(setBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAdaptorNames(XenonProto.Empty request, StreamObserver<XenonProto.AdaptorNames> responseObserver) {
        String[] names = Scheduler.getAdaptorNames();

        XenonProto.AdaptorNames response = XenonProto.AdaptorNames.newBuilder().addAllName(Arrays.asList(names)).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getAdaptorName(XenonProto.Scheduler request, StreamObserver<XenonProto.AdaptorName> responseObserver) {
        try {
            Scheduler scheduler = getScheduler(request);

            String adaptorName = scheduler.getAdaptorName();

            XenonProto.AdaptorName response = XenonProto.AdaptorName.newBuilder().setName(adaptorName).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }

    @Override
    public void getLocation(XenonProto.Scheduler request, StreamObserver<XenonProto.Location> responseObserver) {
        try {
            Scheduler scheduler = getScheduler(request);

            String location = scheduler.getLocation();

            XenonProto.Location response = XenonProto.Location.newBuilder().setLocation(location).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }

    @Override
    public void getProperties(XenonProto.Scheduler request, StreamObserver<XenonProto.Properties> responseObserver) {
        try {
            Scheduler scheduler = getScheduler(request);

            Map<String, String> props = scheduler.getProperties();

            XenonProto.Properties response = XenonProto.Properties.newBuilder().putAllProperties(props).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }

    @Override
    public void listSchedulers(XenonProto.Empty request, StreamObserver<XenonProto.Schedulers> responseObserver) {
        XenonProto.Schedulers.Builder setBuilder = XenonProto.Schedulers.newBuilder();
        XenonProto.Scheduler.Builder builder = XenonProto.Scheduler.newBuilder();

        for (String schedulerId : schedulers.keySet()) {
            setBuilder.addSchedulers(builder
                .setId(schedulerId)
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
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
        responseObserver.onNext(empty());
        responseObserver.onCompleted();
    }

    public void closeAllSchedulers() throws XenonException {
        for (Map.Entry<String, Scheduler> entry : schedulers.entrySet()) {
            entry.getValue().close();
            schedulers.remove(entry.getKey());
        }
    }

    @Override
    public void localScheduler(XenonProto.Empty request, StreamObserver<XenonProto.Scheduler> responseObserver) {
        XenonProto.CreateSchedulerRequest schedulerRequest = XenonProto.CreateSchedulerRequest.newBuilder().setAdaptor("local").build();
        create(schedulerRequest, responseObserver);
    }

    @Override
    public void getDefaultQueueName(XenonProto.Scheduler request, StreamObserver<XenonProto.Queue> responseObserver) {
        try {
            Scheduler scheduler = getScheduler(request);
            String queue = scheduler.getDefaultQueueName();
            responseObserver.onNext(XenonProto.Queue.newBuilder().setName(queue).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }

    private Scheduler getScheduler(XenonProto.Scheduler request) throws StatusException {
        String id = request.getId();
        if (!schedulers.containsKey(id)) {
            throw Status.NOT_FOUND.augmentDescription("Scheduler with id: " + id).asException();
        }
        return schedulers.get(id);
    }

    @Override
    public void getQueueNames(XenonProto.Scheduler request, StreamObserver<XenonProto.Queues> responseObserver) {
        try {
            Scheduler scheduler = getScheduler(request);
            String[] queues = scheduler.getQueueNames();
            responseObserver.onNext(XenonProto.Queues.newBuilder().addAllName(Arrays.asList(queues)).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }

    @Override
    public void isOpen(XenonProto.Scheduler request, StreamObserver<XenonProto.Is> responseObserver) {
        try {
            Scheduler scheduler = getScheduler(request);
            boolean open = scheduler.isOpen();
            responseObserver.onNext(XenonProto.Is.newBuilder().setValue(open).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }

    @Override
    public void getQueueStatus(XenonProto.GetQueueStatusRequest request, StreamObserver<XenonProto.QueueStatus> responseObserver) {
        try {
            Scheduler scheduler = getScheduler(request.getScheduler());
            QueueStatus status = scheduler.getQueueStatus(request.getQueue());
            XenonProto.QueueStatus response = mapQueueStatus(status);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
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
                builder.addStatuses(mapQueueStatus(status));
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
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

            XenonProto.Job response = XenonProto.Job.newBuilder()
                .setId(jobIdentifier)
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }

    @Override
    public void cancelJob(XenonProto.JobRequest request, StreamObserver<XenonProto.JobStatus> responseObserver) {
        try {
            Scheduler scheduler = getScheduler(request.getScheduler());

            JobStatus status = scheduler.cancelJob(request.getJob().getId());

            XenonProto.JobStatus response = mapJobStatus(status);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }

    @Override
    public void getJobStatus(XenonProto.JobRequest request, StreamObserver<XenonProto.JobStatus> responseObserver) {
        try {
            Scheduler scheduler = getScheduler(request.getScheduler());

            JobStatus status = scheduler.getJobStatus(request.getJob().getId());

            XenonProto.JobStatus response = mapJobStatus(status);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }

    @Override
    public void getJobStatuses(XenonProto.GetJobStatusesRequest request, StreamObserver<XenonProto.GetJobStatusesResponse> responseObserver) {
        try {
            XenonProto.GetJobStatusesResponse.Builder builder = XenonProto.GetJobStatusesResponse.newBuilder();

            // for each scheduler fetch statuses
            Scheduler scheduler = getScheduler(request.getScheduler());
            List<String> jobIdentifiers = request.getJobsList().stream().map(XenonProto.Job::getId).collect(Collectors.toList());
            JobStatus[] statuses = scheduler.getJobStatuses(jobIdentifiers.toArray(new String[0]));
            for (JobStatus status: statuses) {
                XenonProto.JobStatus statusResponse = mapJobStatus(status);
                builder.addStatuses(statusResponse);
            }

            XenonProto.GetJobStatusesResponse response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }

    @Override
    public void waitUntilDone(XenonProto.WaitRequest request, StreamObserver<XenonProto.JobStatus> responseObserver) {
        try {
            Scheduler scheduler = getScheduler(request.getScheduler());

            JobStatus status = scheduler.waitUntilDone(request.getJob().getId(), request.getTimeout());

            XenonProto.JobStatus response = mapJobStatus(status);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }

    @Override
    public void waitUntilRunning(XenonProto.WaitRequest request, StreamObserver<XenonProto.JobStatus> responseObserver) {
        try {
            Scheduler scheduler = getScheduler(request.getScheduler());

            JobStatus status = scheduler.waitUntilRunning(request.getJob().getId(), request.getTimeout());

            XenonProto.JobStatus response = mapJobStatus(status);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }

    @Override
    public void getJobs(XenonProto.SchedulerAndQueues request, StreamObserver<XenonProto.Jobs> responseObserver) {
        try {
            XenonProto.Scheduler schedulerRequest = request.getScheduler();
            Scheduler scheduler = getScheduler(schedulerRequest);

            String[] queues = request.getQueuesList().toArray(new String[0]);
            String[] jobIdentifiers = scheduler.getJobs(queues);

            XenonProto.Jobs response = mapJobs(jobIdentifiers);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
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
                        XenonProto.Job job = XenonProto.Job.newBuilder().setId(streams.getJobIdentifier()).build();
                        forwarder = new JobOutputStreamsForwarder(responseObserver, streams.getStderr(), streams.getStdout(), job);
                    }
                    // write incoming stdin to xenons stdin
                    streams.getStdin().write(value.getStdin().toByteArray());
                    streams.getStdin().flush();
                } catch (Exception e) {
                    responseObserver.onError(mapException(e));
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
                        responseObserver.onError(mapException(e));
                    }
                }
                responseObserver.onError(Status.CANCELLED.withCause(t).withDescription(t.getMessage()).asException());
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

    @Override
    public void getFileSystem(XenonProto.Scheduler request, StreamObserver<XenonProto.FileSystem> responseObserver) {
        try {
            Scheduler scheduler = getScheduler(request);
            FileSystem fileSystem = scheduler.getFileSystem();

            // TODO use username of scheduler instead of local user name
            DefaultCredential cred = new DefaultCredential();
            String fileSystemId = getFileSystemId(fileSystem, cred.getUsername());
            // TODO fileSystemId is probably unknown in FileSystemService, so check if fs service knows fs and if not add the fs to the fs list

            XenonProto.FileSystem value = XenonProto.FileSystem.newBuilder()
                    .setId(fileSystemId)
                    .build();
            responseObserver.onNext(value);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }
}
