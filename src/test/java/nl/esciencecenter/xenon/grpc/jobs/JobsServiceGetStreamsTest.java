package nl.esciencecenter.xenon.grpc.jobs;

import static java.lang.Thread.sleep;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.grpc.Status;
import io.grpc.StatusException;
import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.grpc.XenonSingleton;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

/**
 * The LocalJobsServiceTestBase uses in-process server which added even more a-synchronicity
 * This caused unpredictable behavior, so getStreams is tested directly against the service class.
 */
public class JobsServiceGetStreamsTest {
    @Rule
    public TemporaryFolder myfolder = new TemporaryFolder();
    private XenonSingleton singleton;
    private JobsService service;
    private XenonProto.Scheduler scheduler;

    @Before
    public void setUp() {
        singleton = new XenonSingleton();
        service = new JobsService(singleton);
        scheduler = getScheduler(service);
    }

    @After
    public void tearDown() {
        service.close(scheduler, new EmptyObserver());
        singleton.close();
    }

    @Test
    public void getStreams_singlerequestmessage() throws InterruptedException {
        // submit job
        XenonProto.JobDescription description = XenonProto.JobDescription.newBuilder()
                .setExecutable("wc")
                .setQueueName("multi")
                .setWorkingDirectory(myfolder.getRoot().getAbsolutePath())
                .setInteractive(true)
                .build();
        XenonProto.Job job = submit(service, scheduler, description);

        // getStreams
        ResponseObserver responseObserver = new ResponseObserver();
        StreamObserver<XenonProto.JobInputStream> requestWriter = service.getStreams(responseObserver);

        // send
        ByteString stdin = ByteString.copyFromUtf8("a piece of text");
        XenonProto.JobInputStream request = XenonProto.JobInputStream.newBuilder()
                .setJob(job)
                .setStdin(stdin)
                .build();

        requestWriter.onNext(request);
        requestWriter.onCompleted();

        // allow wc and xenon to work
        sleep(100);

        // receive
        XenonProto.JobOutputStreams expected = XenonProto.JobOutputStreams.newBuilder().setStdout(ByteString.copyFromUtf8("      0       4      15\n")).build();
        assertThat(responseObserver.value, equalTo(expected));

        // response end
        assertNull(responseObserver.error);
        assertTrue(responseObserver.completed);
    }

    @Test
    public void getStreams_sendrecievesendrecieve() throws Exception {
        // submit job
        XenonProto.JobDescription description = XenonProto.JobDescription.newBuilder()
            .setExecutable("cat")
            .setQueueName("multi")
            .setWorkingDirectory(myfolder.getRoot().getAbsolutePath())
            .setInteractive(true)
            .build();
        XenonProto.Job job = submit(service, scheduler, description);

        // getStreams
        ResponseObserver responseObserver = new ResponseObserver();
        StreamObserver<XenonProto.JobInputStream> requestWriter = service.getStreams(responseObserver);

        // send first message
        XenonProto.JobInputStream.Builder builder = XenonProto.JobInputStream.newBuilder()
            .setJob(job);
        ByteString line1 = ByteString.copyFromUtf8("first line\n");
        XenonProto.JobInputStream request1 = builder.setStdin(line1).build();
        requestWriter.onNext(request1);

        // allow cat and xenon to work
        sleep(100);

        // receive first message
        XenonProto.JobOutputStreams expected1 = XenonProto.JobOutputStreams.newBuilder().setStdout(line1).build();
        assertThat(responseObserver.value, equalTo(expected1));

        // send second and last message
        ByteString line2 = ByteString.copyFromUtf8("second line\n");
        XenonProto.JobInputStream request2 = builder.setStdin(line2).build();
        requestWriter.onNext(request2);
        requestWriter.onCompleted();

        // allow cat and xenon to work
        sleep(100);

        // receive first message
        XenonProto.JobOutputStreams expected2 = XenonProto.JobOutputStreams.newBuilder().setStdout(line2).build();
        assertThat(responseObserver.value, equalTo(expected2));

        // response end
        assertNull(responseObserver.error);
        assertTrue(responseObserver.completed);
    }

    @Test
    public void getStreams_nojobsend_responseerror() {
        // submit job
        XenonProto.JobDescription description = XenonProto.JobDescription.newBuilder()
                .setExecutable("cat")
                .setQueueName("multi")
                .setWorkingDirectory(myfolder.getRoot().getAbsolutePath())
                .setInteractive(true)
                .build();
        XenonProto.Job job = submit(service, scheduler, description);

        // getStreams
        ResponseObserver responseObserver = new ResponseObserver();
        StreamObserver<XenonProto.JobInputStream> requestWriter = service.getStreams(responseObserver);

        // send first message
        XenonProto.JobInputStream.Builder builder = XenonProto.JobInputStream.newBuilder();
        ByteString line1 = ByteString.copyFromUtf8("first line\n");
        XenonProto.JobInputStream request1 = builder.setStdin(line1).build();
        requestWriter.onNext(request1);

        String expected = "INVALID_ARGUMENT: job value is required";
        assertThat(responseObserver.error.getMessage(), equalTo(expected));
        assertNull(responseObserver.value);
        assertFalse(responseObserver.completed);
    }

    private XenonProto.Job submit(JobsService service, XenonProto.Scheduler scheduler, XenonProto.JobDescription description) {
        XenonProto.SubmitJobRequest jobRequest = XenonProto.SubmitJobRequest.newBuilder()
            .setDescription(description)
            .setScheduler(scheduler)
            .build();

        JobObserver jobResponseObserver = new JobObserver();
        service.submitJob(jobRequest, jobResponseObserver);
        return jobResponseObserver.job;
    }

    private XenonProto.Scheduler getScheduler(JobsService service) {
        // scheduler
        XenonProto.Empty empty = XenonProto.Empty.getDefaultInstance();
        LocalSchedulerObserver localSchedulerObserver = new LocalSchedulerObserver();
        service.localScheduler(empty, localSchedulerObserver);
        return localSchedulerObserver.scheduler;
    }

    private class LocalSchedulerObserver implements StreamObserver<XenonProto.Scheduler> {
        XenonProto.Scheduler scheduler;

        @Override
        public void onNext(XenonProto.Scheduler value) {
            scheduler = value;
        }

        @Override
        public void onError(Throwable t) {

        }

        @Override
        public void onCompleted() {

        }
    }

    private class JobObserver implements StreamObserver<XenonProto.Job> {
        XenonProto.Job job;

        @Override
        public void onNext(XenonProto.Job value) {
            job = value;
        }

        @Override
        public void onError(Throwable t) {

        }

        @Override
        public void onCompleted() {

        }
    }

    private class ResponseObserver implements StreamObserver<XenonProto.JobOutputStreams> {
        XenonProto.JobOutputStreams value = null;
        Throwable error = null;
        boolean completed = false;

        @Override
        public void onNext(XenonProto.JobOutputStreams value) {
            this.value = value;
        }

        @Override
        public void onError(Throwable t) {
            error = t;
        }

        @Override
        public void onCompleted() {
            completed = true;
        }
    }

    private class EmptyObserver implements StreamObserver<XenonProto.Empty> {
        @Override
        public void onNext(XenonProto.Empty value) {

        }

        @Override
        public void onError(Throwable t) {

        }

        @Override
        public void onCompleted() {

        }
    }
}