package nl.esciencecenter.xenon.grpc.jobs;

import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.jobs.*;

class Writers {
    private Writers() {
    }

    static XenonProto.QueueStatus mapQueueStatus(QueueStatus status, XenonProto.Scheduler scheduler) {
        XenonProto.QueueStatus.Builder builder = XenonProto.QueueStatus.newBuilder()
                .setName(status.getQueueName())
                .setScheduler(scheduler)
                .putAllSchedulerSpecificInformation(status.getSchedulerSpecficInformation())
                ;
        if (status.hasException()) {
            builder.setError(status.getException().getMessage());
        }
        return builder.build();
    }

    static  JobDescription mapJobDescription(XenonProto.SubmitJobRequest request) {
        XenonProto.JobDescription d = request.getDescription();
        JobDescription description = new JobDescription();
        description.setExecutable(d.getExecutable());
        description.setArguments(d.getArgumentsList().toArray(new String[0]));
        description.setWorkingDirectory(d.getWorkingDirectory());
        description.setEnvironment(d.getEnvironmentMap());
        description.setQueueName(d.getQueueName());
        description.setInteractive(d.getInteractive());
        description.setMaxTime(d.getMaxTime());
        description.setNodeCount(d.getNodeCount());
        description.setProcessesPerNode(d.getProcessesPerNode());
        description.setStartSingleProcess(d.getStartSingleProcess());
        description.setStderr(d.getStdErr());
        description.setStdin(d.getStdIn());
        description.setStdout(d.getStdOut());
        description.setJobOptions(d.getOptionsMap());
        return description;
    }

    static XenonProto.JobStatus mapJobStatus(JobStatus status, XenonProto.JobDescription description) {
        XenonProto.JobStatus.Builder builder = XenonProto.JobStatus.newBuilder()
                .setState(status.getState())
                .setRunning(status.isRunning())
                .setDone(status.isDone())
                .putAllSchedulerSpecificInformation(status.getSchedulerSpecficInformation())
                .setJob(XenonProto.Job.newBuilder()
                        .setId(status.getJob().getIdentifier())
                        .setDescription(description)
                        .build())
                .setExitCode(status.getExitCode())
                ;
        if (status.hasException()) {
            builder
                    .setErrorMessage(status.getException().getMessage())
                    .setErrorType(mapErrorType(status.getException()));
        }
        
        return builder.build();
    }

    private static XenonProto.JobStatus.ErrorType mapErrorType(Exception exception) {
        if (exception instanceof JobCanceledException) {
            return XenonProto.JobStatus.ErrorType.CANCELLED;
        } else if (exception instanceof NoSuchJobException) {
            return XenonProto.JobStatus.ErrorType.NOT_FOUND;
        } else if (exception instanceof NoSuchSchedulerException) {
            return XenonProto.JobStatus.ErrorType.SCHEDULER_NOT_FOUND;
        }
        // map other (Xenon) exceptions to own type
        return XenonProto.JobStatus.ErrorType.OTHER;
    }

}
