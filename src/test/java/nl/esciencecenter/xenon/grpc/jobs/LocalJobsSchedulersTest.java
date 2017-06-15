package nl.esciencecenter.xenon.grpc.jobs;

import static nl.esciencecenter.xenon.grpc.MapUtils.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import nl.esciencecenter.xenon.grpc.XenonProto;

import io.grpc.StatusRuntimeException;
import org.junit.Test;

public class LocalJobsSchedulersTest extends LocalJobsServiceTestBase {
    @Test
    public void listSchedulers_empty() {
        XenonProto.Schedulers response = client.listSchedulers(empty());

        assertTrue(response.getSchedulersList().isEmpty());
    }

    @Test
    public void listSchedulers_justlocal() {
        XenonProto.Scheduler scheduler = getScheduler();

        XenonProto.Schedulers response = client.listSchedulers(empty());

        XenonProto.Schedulers expected = XenonProto.Schedulers.newBuilder()
            .addSchedulers(scheduler)
            .build();
        assertEquals(expected, response);
    }

    @Test
    public void isOpen() {
        XenonProto.Scheduler scheduler = getScheduler();

        XenonProto.Is response = client.isOpen(scheduler);

        assertTrue(response.getValue());
    }

    @Test
    public void getQueues_notfound() {
        client.getQueues(getSchedulerNotFound());
    }

    @Test
    public void getDefaultQueueName_notfound() {
        client.getDefaultQueueName(getSchedulerNotFound());
    }

    @Test
    public void isOpen_notfound() {
        client.isOpen(getSchedulerNotFound());
    }

    @Test
    public void close_notfound() {
        client.close(getSchedulerNotFound());
    }

    @Test
    public void close() {
        client.close(getScheduler());

        XenonProto.Schedulers currentSchedulers = client.listSchedulers(empty());
        assertTrue(currentSchedulers.getSchedulersList().isEmpty());
    }

    private XenonProto.Scheduler getSchedulerNotFound() {
        String someId = "some-id-that-does-not-exist";
        thrown.expect(StatusRuntimeException.class);
        thrown.expectMessage("NOT_FOUND: " + someId);
        return XenonProto.Scheduler.newBuilder()
            .setId(someId)
            .build();
    }
}
