package nl.esciencecenter.xenon.grpc.schedulers;

import static nl.esciencecenter.xenon.grpc.MapUtils.mapPropertyDescriptions;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.adaptors.NotConnectedException;
import nl.esciencecenter.xenon.adaptors.schedulers.JobCanceledException;
import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.schedulers.JobDescription;
import nl.esciencecenter.xenon.schedulers.JobStatus;
import nl.esciencecenter.xenon.schedulers.NoSuchJobException;
import nl.esciencecenter.xenon.schedulers.NoSuchQueueException;
import nl.esciencecenter.xenon.schedulers.QueueStatus;
import nl.esciencecenter.xenon.schedulers.Scheduler;
import nl.esciencecenter.xenon.schedulers.SchedulerAdaptorDescription;

public class MapUtils {
    private MapUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static XenonProto.QueueStatus mapQueueStatus(QueueStatus status) {
        XenonProto.QueueStatus.Builder builder = XenonProto.QueueStatus.newBuilder()
                .setName(status.getQueueName())
                ;
        Map<String, String> info = status.getSchedulerSpecificInformation();
        if (info != null) {
            builder.putAllSchedulerSpecificInformation(info);
        }
        if (status.hasException()) {
            builder.setErrorMessage(status.getException().getMessage());
            builder.setErrorType(mapQueueStatusErrorType(status.getException()));
        }
        return builder.build();
    }

    private static XenonProto.QueueStatus.ErrorType mapQueueStatusErrorType(Exception exception) {
        if (exception instanceof NoSuchQueueException) {
            return XenonProto.QueueStatus.ErrorType.NOT_FOUND;
        } else if (exception instanceof NotConnectedException) {
            return XenonProto.QueueStatus.ErrorType.NOT_CONNECTED;
        } else if (exception instanceof XenonException) {
            return XenonProto.QueueStatus.ErrorType.XENON;
        } else if (exception instanceof IOException) {
            return XenonProto.QueueStatus.ErrorType.IO;
        }
        return XenonProto.QueueStatus.ErrorType.OTHER;
    }

    private static String defaultValue(String value) {
        if (value.isEmpty()) {
            return null;
        }
        return value;
    }

    public static JobDescription mapJobDescription(XenonProto.JobDescription d) {
        JobDescription description = new JobDescription();
        description.setExecutable(d.getExecutable());
        description.setArguments(d.getArgumentsList().toArray(new String[0]));
        if (!"".equals(d.getWorkingDirectory())) {
            description.setWorkingDirectory(d.getWorkingDirectory());
        }
        description.setEnvironment(d.getEnvironmentMap());
        description.setQueueName(defaultValue(d.getQueueName()));
        if (d.getMaxRuntime() != 0) {
            description.setMaxRuntime(d.getMaxRuntime());
        }
        if (d.getNodeCount() != 0) {
            description.setNodeCount(d.getNodeCount());
        }
        if (d.getProcessesPerNode() != 0) {
            description.setProcessesPerNode(d.getProcessesPerNode());
        }
        description.setStartSingleProcess(d.getStartSingleProcess());
        description.setStderr(defaultValue(d.getStderr()));
        description.setStdin(defaultValue(d.getStdin()));
        description.setStdout(defaultValue(d.getStdout()));
        description.setJobOptions(d.getOptionsMap());
        return description;
    }

    public static XenonProto.JobStatus mapJobStatus(JobStatus status) {
        XenonProto.JobStatus.Builder builder = XenonProto.JobStatus.newBuilder()
            .setJob(XenonProto.Job.newBuilder().setId(status.getJobIdentifier()))
            .setState(status.getState())
            .setRunning(status.isRunning())
            .setDone(status.isDone())
            ;
        Integer exitCode = status.getExitCode();
        if (exitCode != null) {
            builder.setExitCode(exitCode);
        }
        Map<String, String> info = status.getSchedulerSpecificInformation();
        if (info != null) {
            builder.putAllSchedulerSpecificInformation(info);
        }
        if (status.hasException()) {
            builder
                    .setErrorMessage(status.getException().getMessage())
                    .setErrorType(mapJobStatusErrorType(status.getException()));
        }

        return builder.build();
    }

    public static XenonProto.JobStatus.ErrorType mapJobStatusErrorType(Exception exception) {
        if (exception instanceof JobCanceledException) {
            return XenonProto.JobStatus.ErrorType.CANCELLED;
        } else if (exception instanceof NoSuchJobException) {
            return XenonProto.JobStatus.ErrorType.NOT_FOUND;
        } else if (exception instanceof XenonException) {
            return XenonProto.JobStatus.ErrorType.XENON;
        } else if (exception instanceof IOException) {
            return XenonProto.JobStatus.ErrorType.IO;
        }
        return XenonProto.JobStatus.ErrorType.OTHER;
    }

    public static XenonProto.SchedulerAdaptorDescription mapSchedulerAdaptorDescription(SchedulerAdaptorDescription desc) {
        List<XenonProto.PropertyDescription> supportedProperties = mapPropertyDescriptions(desc.getSupportedProperties());
        return XenonProto.SchedulerAdaptorDescription.newBuilder()
                .setName(desc.getName())
                .setDescription(desc.getDescription())
                .addAllSupportedLocations(Arrays.asList(desc.getSupportedLocations()))
                .addAllSupportedProperties(supportedProperties)
                .setIsEmbedded(desc.isEmbedded())
                .setSupportsBatch(desc.supportsBatch())
                .setSupportsInteractive(desc.supportsInteractive())
                .setUsesFileSystem(desc.usesFileSystem())
                .build();
    }

    static XenonProto.Jobs mapJobs(String[] jobIdentifiers) {
        XenonProto.Jobs.Builder builder = XenonProto.Jobs.newBuilder();
        XenonProto.Job.Builder jobBuilder = XenonProto.Job.newBuilder();
        for (String jobId : jobIdentifiers) {
            XenonProto.Job job = jobBuilder.setId(jobId).build();
            builder.addJobs(job);
        }
        return builder.build();
    }
}
