package nl.esciencecenter.xenon.grpc.jobs;

import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.jobs.Job;

public class JobContainer {
    private final XenonProto.SubmitJobRequest request;
    private final Job job;

    JobContainer(XenonProto.SubmitJobRequest request, Job job) {
        this.request = request;
        this.job = job;
    }

    Job getJob() {
        return job;
    }

    public XenonProto.SubmitJobRequest getRequest() {
        return request;
    }
}


