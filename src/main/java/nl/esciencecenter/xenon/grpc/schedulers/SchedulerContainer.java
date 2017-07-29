package nl.esciencecenter.xenon.grpc.schedulers;

import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.schedulers.Scheduler;

public class SchedulerContainer {
    private final Scheduler scheduler;
    private final XenonProto.CreateSchedulerRequest request;

    SchedulerContainer(XenonProto.CreateSchedulerRequest request, Scheduler scheduler) {
        this.request = request;
        this.scheduler = scheduler;
    }

    public XenonProto.CreateSchedulerRequest getRequest() {
        return request;
    }

    Scheduler getScheduler() {
        return scheduler;
    }
}
