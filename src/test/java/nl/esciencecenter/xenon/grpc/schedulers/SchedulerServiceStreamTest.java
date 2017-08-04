package nl.esciencecenter.xenon.grpc.schedulers;

import com.google.protobuf.ByteString;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.schedulers.IncompleteJobDescriptionException;
import nl.esciencecenter.xenon.schedulers.JobDescription;
import nl.esciencecenter.xenon.schedulers.Scheduler;
import nl.esciencecenter.xenon.schedulers.Streams;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static nl.esciencecenter.xenon.grpc.schedulers.MapUtils.mapJobDescription;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SchedulerServiceStreamTest {
    private SchedulersService service;
    private Scheduler scheduler;
    @Captor
    private ArgumentCaptor<StatusException> captor;

    private XenonProto.CreateSchedulerRequest createSchedulerRequest() {
        return XenonProto.CreateSchedulerRequest.newBuilder()
                .setAdaptor("local")
                .setLocation("local://")
                .setDefaultCred(XenonProto.DefaultCredential.newBuilder().setUsername("someone").build())
                .build();
    }

    private XenonProto.Scheduler createScheduler() {
        return XenonProto.Scheduler.newBuilder()
                .setRequest(createSchedulerRequest())
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
        service.putScheduler(createSchedulerRequest(), scheduler, "someone");
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
}
