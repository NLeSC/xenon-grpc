package nl.esciencecenter.xenon.grpc.jobs;

import io.grpc.stub.StreamObserver;
import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.grpc.XenonSingleton;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class JobsServiceTest {
    private XenonSingleton singleton;
    private JobsService service;

    @Before
    public void SetUp() {
        singleton = new XenonSingleton();
        service = new JobsService(singleton);
    }

    @Test
    public void getAdaptorDescriptions() throws Exception {
        XenonProto.Empty empty = XenonProto.Empty.getDefaultInstance();
        StreamObserver<XenonProto.JobAdaptorDescriptions> observer = mock(StreamObserver.class);

        service.getAdaptorDescriptions(empty, observer);

        ArgumentCaptor<XenonProto.JobAdaptorDescriptions> capturer = ArgumentCaptor.forClass(XenonProto.JobAdaptorDescriptions.class);
        verify(observer).onNext(capturer.capture());
        assertTrue("onNext has some descriptions",capturer.getValue().getDescriptionsCount() > 1);
        verify(observer, times(1)).onCompleted();
        verify(observer, times(0)).onError(any(Throwable.class));
    }

    @Test
    public void listSchedulers_noschedulersregisterd_emptyresponse() throws Exception {
        XenonProto.Empty empty = XenonProto.Empty.getDefaultInstance();
        StreamObserver<XenonProto.Schedulers> observer = mock(StreamObserver.class);

        service.listSchedulers(empty, observer);

        XenonProto.Schedulers expected = XenonProto.Schedulers.getDefaultInstance();
        verify(observer).onNext(expected);
        verify(observer, times(1)).onCompleted();
        verify(observer, times(0)).onError(any(Throwable.class));
    }

    @Test
    public void localScheduler() throws Exception {
        XenonProto.Empty empty = XenonProto.Empty.getDefaultInstance();
        StreamObserver<XenonProto.Scheduler> observer = mock(StreamObserver.class);

        service.localScheduler(empty, observer);

        // TODO improve verification
        verify(observer).onNext(any(XenonProto.Scheduler.class));
        verify(observer, times(1)).onCompleted();
        verify(observer, times(0)).onError(any(Throwable.class));
    }
}