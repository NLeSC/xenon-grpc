package nl.esciencecenter.xenon.grpc.jobs;

import static java.lang.Thread.sleep;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.grpc.XenonSingleton;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class JobsServiceGetStreamsTest {
    @Rule
    public TemporaryFolder myfolder = new TemporaryFolder();

    @Test
    public void getStreams() throws Exception {
        XenonSingleton singleton = new XenonSingleton();
        JobsService service = new JobsService(singleton);

        // scheduler
        XenonProto.Empty empty = XenonProto.Empty.getDefaultInstance();
        LocalSchedulerObserver localSchedulerObserver = new LocalSchedulerObserver();
        service.localScheduler(empty, localSchedulerObserver);
        XenonProto.Scheduler scheduler = localSchedulerObserver.scheduler;


        // submit job
        XenonProto.JobDescription description = XenonProto.JobDescription.newBuilder()
            .setExecutable("cat")
            .setQueueName("multi")
            .setWorkingDirectory(myfolder.getRoot().getAbsolutePath())
            .setInteractive(true)
            .build();
        XenonProto.SubmitJobRequest jobRequest = XenonProto.SubmitJobRequest.newBuilder()
            .setDescription(description)
            .setScheduler(scheduler)
            .build();

        JobObserver jobResponseObserver = new JobObserver();
        service.submitJob(jobRequest, jobResponseObserver);
        XenonProto.Job job = jobResponseObserver.job;

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

        singleton.close();
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
        XenonProto.JobOutputStreams value;

        @Override
        public void onNext(XenonProto.JobOutputStreams value) {
            this.value = value;
        }

        @Override
        public void onError(Throwable t) {

        }

        @Override
        public void onCompleted() {

        }
    }
}