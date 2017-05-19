package nl.esciencecenter.xenon.grpc.jobs;

import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.jobs.Scheduler;

public class SchedulerContainer {
    private final Scheduler scheduler;
    private final XenonProto.NewSchedulerRequest request;

    SchedulerContainer(XenonProto.NewSchedulerRequest request, Scheduler scheduler) {
        this.request = request;
        this.scheduler = scheduler;
    }

    public XenonProto.NewSchedulerRequest getRequest() {
        return request;
    }

    Scheduler getScheduler() {
        return scheduler;
    }
}
