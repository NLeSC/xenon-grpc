package nl.esciencecenter.xenon.grpc.jobs;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import nl.esciencecenter.xenon.grpc.XenonJobsGrpc;
import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.grpc.XenonProto.JobOutputStreams;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class LocalJobsStreamsTest extends LocalJobsServiceTestBase {
    private XenonJobsGrpc.XenonJobsStub aclient;

    @Before
    @Override
    public void setUp() throws IOException {
        super.setUp();
        // The default client does not have XenonFiles.write method
        // because it only works in blocking synchronous mode, but the XenonFiles.write must be called asynchronously
        // so use an asynchronous client
        aclient = XenonJobsGrpc.newStub(channel);
    }

    @Test
    public void getStreams_wc() {
        // submit job
        XenonProto.JobDescription description = XenonProto.JobDescription.newBuilder()
                .setExecutable("wc")
                .setQueueName("multi")
                .setWorkingDirectory(myfolder.getRoot().getAbsolutePath())
                .setInteractive(true)
                .build();
        
        XenonProto.SubmitJobRequest jobRequest = XenonProto.SubmitJobRequest.newBuilder()
                .setDescription(description)
                .setScheduler(getScheduler())
                .build();
        
        XenonProto.Job job = client.submitJob(jobRequest);
        // mock receiver
        @SuppressWarnings("unchecked")
        StreamObserver<JobOutputStreams> responseObserver = mock(StreamObserver.class);

        // call method under test
        StreamObserver<XenonProto.JobInputStream> requestObserver = aclient.getStreams(responseObserver);

        // send
        ByteString stdin = ByteString.copyFromUtf8("a piece of text");
        XenonProto.JobInputStream request = XenonProto.JobInputStream.newBuilder()
                .setJob(job)
                .setStdin(stdin)
                .build();
        
        requestObserver.onNext(request);
        requestObserver.onCompleted();
               
        // receive
        ArgumentCaptor<JobOutputStreams> responseCapturer = ArgumentCaptor.forClass(JobOutputStreams.class);
        verify(responseObserver, timeout(100)).onNext(responseCapturer.capture());
        ByteString expectedStdout = ByteString.copyFromUtf8("      0       4      15\n");
        JobOutputStreams response = responseCapturer.getValue();
        
        JobOutputStreams expected = JobOutputStreams.newBuilder()
                .setStdout(expectedStdout)
                .build();
        assertEquals(expected, response);
        verify(responseObserver, timeout(100)).onCompleted();
        verify(responseObserver, never()).onError(any(Throwable.class));
    }

    //@Ignore("request.onCompleted is called too soon causing failure")
    @Test
    public void getStreams_cat_multiline() {
        // submit job
        XenonProto.JobDescription description = XenonProto.JobDescription.newBuilder()
                .setExecutable("cat")
                .setQueueName("multi")
                .setWorkingDirectory(myfolder.getRoot().getAbsolutePath())
                .setInteractive(true)
                .build();

        XenonProto.SubmitJobRequest jobRequest = XenonProto.SubmitJobRequest.newBuilder()
                .setDescription(description)
                .setScheduler(getScheduler())
                .build();

        XenonProto.Job job = client.submitJob(jobRequest);
        // mock receiver
        @SuppressWarnings("unchecked")
        StreamObserver<JobOutputStreams> responseObserver = mock(StreamObserver.class);
        ArgumentCaptor<JobOutputStreams> responseCapturer = ArgumentCaptor.forClass(JobOutputStreams.class);
        InOrder inorder = inOrder(responseObserver);
        // call method under test
        StreamObserver<XenonProto.JobInputStream> requestWriter = aclient.getStreams(responseObserver);

        // send first message
        XenonProto.JobInputStream.Builder builder = XenonProto.JobInputStream.newBuilder()
                .setJob(job);
        ByteString line1 = ByteString.copyFromUtf8("first line\n");
        XenonProto.JobInputStream request1 = builder.setStdin(line1).build();
        requestWriter.onNext(request1);

        // receive first message
        inorder.verify(responseObserver, timeout(100)).onNext(responseCapturer.capture());
        JobOutputStreams expected1 = JobOutputStreams.newBuilder().setStdout(line1).build();
        assertEquals(expected1, responseCapturer.getValue());

        // send second message
        ByteString line2 = ByteString.copyFromUtf8("second line\n");
        XenonProto.JobInputStream request2 = builder.setStdin(line2).build();
        requestWriter.onNext(request2);


        // receive second message
        inorder.verify(responseObserver, timeout(100)).onNext(responseCapturer.capture());
        JobOutputStreams expected2 = JobOutputStreams.newBuilder().setStdout(line2).build();
        assertEquals(expected2, responseCapturer.getValue());

        requestWriter.onCompleted();
        // no surprises
        verify(responseObserver, timeout(100)).onCompleted();
        verify(responseObserver, never()).onError(any(Throwable.class));
    }
}
