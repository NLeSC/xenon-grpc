package nl.esciencecenter.xenon.grpc.schedulers;

import static java.util.UUID.randomUUID;
import static nl.esciencecenter.xenon.grpc.MapUtils.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.grpc.XenonSchedulersGrpc;
import nl.esciencecenter.xenon.schedulers.Scheduler;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SchedulersServiceTest {
    private SchedulersService service;
    private Server server;
    private ManagedChannel channel;
    private XenonSchedulersGrpc.XenonSchedulersBlockingStub client;

    @Before
    public void setUp() throws Exception {
        service = new SchedulersService();
        // register mocked scheduler to service
        Scheduler scheduler = mock(Scheduler.class);
        when(scheduler.getAdaptorName()).thenReturn("local");
        when(scheduler.getLocation()).thenReturn("local://");
        service.putScheduler(createSchedulerRequest(),  scheduler, "someone");
        // setup server
        String name = service.getClass().getName() + "-" + randomUUID().toString();
        server = InProcessServerBuilder.forName(name).directExecutor().addService(service).build();
        server.start();
        // setup client
        channel = InProcessChannelBuilder.forName(name).directExecutor().usePlaintext(true).build();
        client = XenonSchedulersGrpc.newBlockingStub(channel);
    }

    private XenonProto.CreateSchedulerRequest createSchedulerRequest() {
        return XenonProto.CreateSchedulerRequest.newBuilder()
            .setAdaptor("local")
            .setLocation("local://")
            .setDefaultCred(XenonProto.DefaultCredential.newBuilder().setUsername("someone").build())
            .build();
    }

    @After
    public void tearDown() throws Exception {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    public void getAdaptorDescription() throws Exception {
        XenonProto.AdaptorName request = XenonProto.AdaptorName.newBuilder()
            .setName("local")
            .build();

        XenonProto.SchedulerAdaptorDescription response = client.getAdaptorDescription(request);

        assertEquals("local", response.getName());
    }

    @Test
    public void getAdaptorDescriptions() throws Exception {
        XenonProto.SchedulerAdaptorDescriptions response = client.getAdaptorDescriptions(empty());

        assertTrue("Some descriptions", response.getDescriptionsCount() > 0);
    }

    @Test
    public void listSchedulers() throws Exception {
    }

    @Test
    public void close() throws Exception {
    }

    @Test
    public void closeAllSchedulers() throws Exception {
    }

    @Test
    public void localScheduler() throws Exception {
    }

    @Test
    public void getDefaultQueueName() throws Exception {
    }

    @Test
    public void getQueues() throws Exception {
    }

    @Test
    public void isOpen() throws Exception {
    }

    @Test
    public void getQueueStatus() throws Exception {
    }

    @Test
    public void getQueueStatuses() throws Exception {
    }

    @Test
    public void submitBatchJob() throws Exception {
    }

    @Test
    public void cancelJob() throws Exception {
    }

    @Test
    public void getJobStatus() throws Exception {
    }

    @Test
    public void getJobStatuses() throws Exception {
    }

    @Test
    public void waitUntilDone() throws Exception {
    }

    @Test
    public void waitUntilRunning() throws Exception {
    }

    @Test
    public void getJobs() throws Exception {
    }

    @Test
    public void submitInteractiveJob() throws Exception {
    }

}