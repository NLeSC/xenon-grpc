package nl.esciencecenter.xenon.grpc.schedulers;

import static nl.esciencecenter.xenon.grpc.MapUtils.mapPropertyDescriptions;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.adaptors.schedulers.JobCanceledException;
import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.schedulers.JobDescription;
import nl.esciencecenter.xenon.schedulers.JobStatus;
import nl.esciencecenter.xenon.schedulers.NoSuchJobException;
import nl.esciencecenter.xenon.schedulers.QueueStatus;
import nl.esciencecenter.xenon.schedulers.SchedulerAdaptorDescription;

public class MapUtils {
    public static XenonProto.QueueStatus mapQueueStatus(QueueStatus status, XenonProto.Scheduler scheduler) {
        XenonProto.QueueStatus.Builder builder = XenonProto.QueueStatus.newBuilder()
                .setName(status.getQueueName())
                .setScheduler(scheduler);
        Map<String, String> info = status.getSchedulerSpecficInformation();
        if (info != null) {
            builder.putAllSchedulerSpecificInformation(info);
        }
        if (status.hasException()) {
            builder.setError(status.getException().getMessage());
        }
        return builder.build();
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
        if (d.getMaxTime() != 0) {
            description.setMaxTime(d.getMaxTime());
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

    public static XenonProto.JobStatus mapJobStatus(JobStatus status, XenonProto.Scheduler scheduler) {
        XenonProto.JobStatus.Builder builder = XenonProto.JobStatus.newBuilder()
            .setState(status.getState())
            .setRunning(status.isRunning())
            .setDone(status.isDone())
            .setJob(XenonProto.Job.newBuilder()
                .setId(status.getJobIdentifier())
                .setScheduler(scheduler)
                .build())
            ;
        Integer exitCode = status.getExitCode();
        if (exitCode != null) {
            builder.setExitCode(exitCode);
        }
        Map<String, String> info = status.getSchedulerSpecficInformation();
        if (info != null) {
            builder.putAllSchedulerSpecificInformation(info);
        }
        if (status.hasException()) {
            builder
                    .setErrorMessage(status.getException().getMessage())
                    .setErrorType(mapErrorType(status.getException()));
        }

        return builder.build();
    }

    public static XenonProto.JobStatus.ErrorType mapErrorType(Exception exception) {
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

    static XenonProto.SchedulerAdaptorDescription mapJobAdaptorDescription(SchedulerAdaptorDescription desc) {
        List<XenonProto.PropertyDescription> supportedProperties = mapPropertyDescriptions(desc.getSupportedProperties());
        return XenonProto.SchedulerAdaptorDescription.newBuilder()
                .setName(desc.getName())
                .setDescription(desc.getDescription())
                .addAllSupportedLocations(Arrays.asList(desc.getSupportedLocations()))
                .addAllSupportedProperties(supportedProperties)
                .build();
    }

    static XenonProto.Jobs mapJobs(XenonProto.Scheduler scheduler, String[] jobIdentifiers) {
        XenonProto.Jobs.Builder builder = XenonProto.Jobs.newBuilder();
        XenonProto.Job.Builder jobBuilder = XenonProto.Job.newBuilder().setScheduler(scheduler);
        for (String jobId : jobIdentifiers) {
            XenonProto.Job job = jobBuilder.setId(jobId).build();
            builder.addJobs(job);
        }
        return builder.build();
    }
}
