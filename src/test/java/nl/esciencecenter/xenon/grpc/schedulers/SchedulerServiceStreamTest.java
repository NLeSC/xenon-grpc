package nl.esciencecenter.xenon.grpc.schedulers;

import static java.lang.Thread.sleep;
import static nl.esciencecenter.xenon.grpc.MapUtils.empty;
import static nl.esciencecenter.xenon.grpc.schedulers.MapUtils.mapJobDescription;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.schedulers.IncompleteJobDescriptionException;
import nl.esciencecenter.xenon.schedulers.JobDescription;
import nl.esciencecenter.xenon.schedulers.Scheduler;
import nl.esciencecenter.xenon.schedulers.Streams;

import com.google.protobuf.ByteString;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

public class SchedulerServiceStreamTest {
    private SchedulersService service;
    private Scheduler scheduler;
    @Captor
    private ArgumentCaptor<StatusException> captor;

    private XenonProto.Scheduler createScheduler() {
        return XenonProto.Scheduler.newBuilder()
                .setId("local://someone@local://")
                .build();
    }

    private XenonProto.Job buildJob(String jobId) {
        return XenonProto.Job.newBuilder()
                .setId(jobId)
                .setScheduler(createScheduler())
                .build();
    }

    @Before
    public void setUp() throws Exception {
        service = new SchedulersService();
        // register mocked scheduler to service
        scheduler = mock(Scheduler.class);
        when(scheduler.getAdaptorName()).thenReturn("local");
        when(scheduler.getLocation()).thenReturn("local://");
        service.putScheduler(scheduler, "someone");
        MockitoAnnotations.initMocks(this);
    }

    @After
    public void tearDown() throws Exception {
        service.closeAllSchedulers();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void submitInteractiveJob_singleRequestNoStdinNoError() throws XenonException {
        String jobId = "JOBID-1";
        byte[] stdout = "Hello".getBytes();
        JobDescription description = new JobDescription();
        description.setExecutable("myexecutable");
        Streams streams = mock(Streams.class);
        when(streams.getJobIdentifier()).thenReturn(jobId);
        when(streams.getStdin()).thenReturn(new ByteArrayOutputStream());
        when(streams.getStderr()).thenReturn(new ByteArrayInputStream(new byte[0]));
        when(streams.getStdout()).thenReturn(new ByteArrayInputStream(stdout));
        when(scheduler.submitInteractiveJob(description)).thenReturn(streams);
        StreamObserver<XenonProto.SubmitInteractiveJobResponse> responseObserver = (StreamObserver<XenonProto.SubmitInteractiveJobResponse>) mock(StreamObserver.class);

        StreamObserver<XenonProto.SubmitInteractiveJobRequest> requestBroadcaster = service.submitInteractiveJob(responseObserver);

        // send request
        XenonProto.JobDescription descriptionRequest = XenonProto.JobDescription.newBuilder()
                .setExecutable("myexecutable")
                .build();
        XenonProto.SubmitInteractiveJobRequest request = XenonProto.SubmitInteractiveJobRequest.newBuilder()
                .setScheduler(createScheduler())
                .setDescription(descriptionRequest)
                .build();
        requestBroadcaster.onNext(request);
        requestBroadcaster.onCompleted();

        // verify response
        XenonProto.SubmitInteractiveJobResponse response = XenonProto.SubmitInteractiveJobResponse.newBuilder()
                .setJob(buildJob(jobId))
                .setStdout(ByteString.copyFrom(stdout))
                .build();
        verify(responseObserver, timeout(1000)).onNext(response);
        verify(responseObserver, timeout(1000)).onCompleted();
        verify(responseObserver, never()).onError(any(Throwable.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void submitInteractiveJob_singleRequestNoJobDescription_errors() throws XenonException {
        JobDescription description = mapJobDescription(XenonProto.JobDescription.getDefaultInstance());
        IncompleteJobDescriptionException exception = new IncompleteJobDescriptionException("file", "No executable");
        when(scheduler.submitInteractiveJob(description)).thenThrow(exception);
        StreamObserver<XenonProto.SubmitInteractiveJobResponse> responseObserver = (StreamObserver<XenonProto.SubmitInteractiveJobResponse>) mock(StreamObserver.class);

        StreamObserver<XenonProto.SubmitInteractiveJobRequest> requestBroadcaster = service.submitInteractiveJob(responseObserver);

        // send request
        XenonProto.SubmitInteractiveJobRequest request = XenonProto.SubmitInteractiveJobRequest.newBuilder()
                .setScheduler(createScheduler())
                .build();
        requestBroadcaster.onNext(request);
        requestBroadcaster.onCompleted();

        // verify response
        verify(responseObserver, timeout(1000)).onError(captor.capture());
        StatusException error = captor.getValue();
        assertEquals("INVALID_ARGUMENT: file adaptor: No executable", error.getMessage());
        verify(responseObserver, never()).onNext(any(XenonProto.SubmitInteractiveJobResponse.class));
        verify(responseObserver, never()).onCompleted();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void submitInteractiveJob_submitSendReceiveSendReceive() throws Exception {
        final XenonProto.Scheduler[] scheduler = new XenonProto.Scheduler[1];
        StreamObserver<XenonProto.Scheduler> responseSchedulerObserver = new StreamObserver<XenonProto.Scheduler>() {
            @Override
            public void onNext(XenonProto.Scheduler value) {
                scheduler[0] = value;
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {

            }
        };
        service.localScheduler(empty(), responseSchedulerObserver);
        StreamObserver<XenonProto.SubmitInteractiveJobResponse> responseObserver = (StreamObserver<XenonProto.SubmitInteractiveJobResponse>) mock(StreamObserver.class);
        StreamObserver<XenonProto.SubmitInteractiveJobRequest> requestBroadcaster = service.submitInteractiveJob(responseObserver);

        // submit job
        XenonProto.SubmitInteractiveJobRequest.Builder requestBuilder = XenonProto.SubmitInteractiveJobRequest.newBuilder()
            .setDescription(
                XenonProto.JobDescription.newBuilder()
                    .setExecutable("cat")
                    .setQueueName("multi")
            ).setScheduler(scheduler[0]);
        XenonProto.SubmitInteractiveJobRequest request1 = requestBuilder.build();
        requestBroadcaster.onNext(request1);

        // send first line to stdIn
        ByteString line1 = ByteString.copyFromUtf8("first line\n");
        XenonProto.SubmitInteractiveJobRequest request2 = requestBuilder.setStdin(line1).build();
        requestBroadcaster.onNext(request2);

        // allow cat and xenon to work
        sleep(100);

        // receive first line on stdOut
        XenonProto.SubmitInteractiveJobResponse.Builder responseBuilder = XenonProto.SubmitInteractiveJobResponse.newBuilder()
            .setJob(
                XenonProto.Job.newBuilder()
                    .setId("local-0")
                    .setScheduler(scheduler[0])
            );
        XenonProto.SubmitInteractiveJobResponse expected1 = responseBuilder.setStdout(line1).build();
        verify(responseObserver).onNext(expected1);
        reset(responseObserver);

        // send second line to stdIn
        ByteString line2 = ByteString.copyFromUtf8("second line\n");
        XenonProto.SubmitInteractiveJobRequest request3 = requestBuilder.setStdin(line2).build();
        requestBroadcaster.onNext(request3);
        requestBroadcaster.onCompleted();

        // allow cat and xenon to work
        sleep(100);

        // receive second line on stdOut
        XenonProto.SubmitInteractiveJobResponse expected2 = responseBuilder.setStdout(line2).build();
        verify(responseObserver).onNext(expected2);
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void submitInteractiveJob_cancelNothingSubmitted() {
        StreamObserver<XenonProto.SubmitInteractiveJobResponse> responseObserver = (StreamObserver<XenonProto.SubmitInteractiveJobResponse>) mock(StreamObserver.class);

        StreamObserver<XenonProto.SubmitInteractiveJobRequest> requestBroadcaster = service.submitInteractiveJob(responseObserver);

        Exception error = new Exception("Client cancelled");
        requestBroadcaster.onError(error);

        String expected = "CANCELLED: Client cancelled";
        verify(responseObserver).onError(captor.capture());
        assertEquals(expected, captor.getValue().getMessage());
    }
}
