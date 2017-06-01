package nl.esciencecenter.xenon.grpc.jobs;

import static nl.esciencecenter.xenon.grpc.files.LocalFilesTestBase.empty;
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

    XenonProto.Scheduler getSchedulerNotFound() {
        String someId = "some-id-that-does-not-exist";
        thrown.expect(StatusRuntimeException.class);
        thrown.expectMessage("NOT_FOUND: " + someId);
        return XenonProto.Scheduler.newBuilder()
            .setId(someId)
            .build();
    }
}
