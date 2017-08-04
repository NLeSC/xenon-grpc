package nl.esciencecenter.xenon.grpc.schedulers;

import static java.util.UUID.randomUUID;
import static nl.esciencecenter.xenon.grpc.MapUtils.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import nl.esciencecenter.xenon.adaptors.schedulers.JobStatusImplementation;
import nl.esciencecenter.xenon.adaptors.schedulers.QueueStatusImplementation;
import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.grpc.XenonSchedulersGrpc;
import nl.esciencecenter.xenon.schedulers.JobDescription;
import nl.esciencecenter.xenon.schedulers.JobStatus;
import nl.esciencecenter.xenon.schedulers.QueueStatus;
import nl.esciencecenter.xenon.schedulers.Scheduler;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SchedulersServiceBlockingTest {
    private SchedulersService service;
    private Server server;
    private ManagedChannel channel;
    private XenonSchedulersGrpc.XenonSchedulersBlockingStub client;
    private Scheduler scheduler;

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
        // setup server
        String name = service.getClass().getName() + "-" + randomUUID().toString();
        server = InProcessServerBuilder.forName(name).directExecutor().addService(service).build();
        server.start();
        // setup client
        channel = InProcessChannelBuilder.forName(name).directExecutor().usePlaintext(true).build();
        client = XenonSchedulersGrpc.newBlockingStub(channel);
    }

    @After
    public void tearDown() throws Exception {
        channel.shutdownNow();
        server.shutdownNow();
        service.closeAllSchedulers();
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
        XenonProto.Schedulers response = client.listSchedulers(empty());

        XenonProto.Schedulers expected = XenonProto.Schedulers.newBuilder()
                .addSchedulers(createScheduler())
                .build();

        assertEquals(expected, response);
    }

    @Test
    public void close() throws Exception {
        XenonProto.Scheduler request = createScheduler();

        client.close(request);

        verify(scheduler).close();
        XenonProto.Schedulers result = client.listSchedulers(empty());
        assertEquals("No schedulers registered", 0, result.getSchedulersCount());
    }

    @Test
    public void closeAllSchedulers() throws Exception {
        service = new SchedulersService();
        service.putScheduler(createSchedulerRequest(), scheduler, "someone");
    }

    @Test
    public void localScheduler() throws Exception {
    }

    @Test
    public void getDefaultQueueName() throws Exception {
        when(scheduler.getDefaultQueueName()).thenReturn("default");

        XenonProto.Queue response = client.getDefaultQueueName(createScheduler());

        XenonProto.Queue expected = XenonProto.Queue.newBuilder()
                .setName("default")
                .build();
        assertEquals(expected, response);
    }

    @Test
    public void getQueues() throws Exception {
        String[] queues = new String[]{"default", "other"};
        when(scheduler.getQueueNames()).thenReturn(queues);

        XenonProto.Queues response = client.getQueues(createScheduler());

        XenonProto.Queues expected = XenonProto.Queues.newBuilder().addAllName(Arrays.asList(queues)).build();
        assertEquals(expected, response);
    }

    @Test
    public void isOpen() throws Exception {
        when(scheduler.isOpen()).thenReturn(true);

        XenonProto.Is response = client.isOpen(createScheduler());

        assertTrue(response.getValue());
    }

    @Test
    public void getQueueStatus() throws Exception {
        String queueName = "somequeue";
        Map<String, String> info = new HashMap<>();
        info.put("state", "idle");
        XenonProto.SchedulerAndQueue request = XenonProto.SchedulerAndQueue.newBuilder()
                .setScheduler(createScheduler())
                .setQueue(queueName)
                .build();
        QueueStatus status = new QueueStatusImplementation(scheduler, queueName, null, info);
        when(scheduler.getQueueStatus(queueName)).thenReturn(status);

        XenonProto.QueueStatus response = client.getQueueStatus(request);

        XenonProto.QueueStatus expected = XenonProto.QueueStatus.newBuilder()
                .setName(queueName)
                .setScheduler(createScheduler())
                .putAllSchedulerSpecificInformation(info)
                .build();
        assertEquals(expected, response);
    }

    @Test
    public void getQueueStatuses() throws Exception {
        String queueName = "somequeue";
        Map<String, String> info = new HashMap<>();
        info.put("state", "idle");
        XenonProto.SchedulerAndQueues request = XenonProto.SchedulerAndQueues.newBuilder()
                .addQueues(queueName)
                .setScheduler(createScheduler())
                .build();
        QueueStatus[] statuses = new QueueStatus[]{
                new QueueStatusImplementation(scheduler, queueName, null, info)
        };
        when(scheduler.getQueueStatuses(queueName)).thenReturn(statuses);

        XenonProto.QueueStatuses response = client.getQueueStatuses(request);

        XenonProto.QueueStatuses expected = XenonProto.QueueStatuses.newBuilder()
                .addStatuses(XenonProto.QueueStatus.newBuilder()
                        .setName(queueName)
                        .setScheduler(createScheduler())
                        .putAllSchedulerSpecificInformation(info)
                )
                .build();
        assertEquals(expected, response);
    }

    @Test
    public void submitBatchJob() throws Exception {
        XenonProto.JobDescription descriptionRequest = XenonProto.JobDescription.newBuilder()
                .setExecutable("myexecutable")
                .build();
        XenonProto.SubmitBatchJobRequest request = XenonProto.SubmitBatchJobRequest.newBuilder()
                .setScheduler(createScheduler())
                .setDescription(descriptionRequest)
                .build();
        JobDescription description = new JobDescription();
        description.setExecutable("myexecutable");
        when(scheduler.submitBatchJob(description)).thenReturn("JOBID-1");

        XenonProto.Job response = client.submitBatchJob(request);

        XenonProto.Job expected = buildJob("JOBID-1");
        assertEquals(expected, response);
    }

    @Test
    public void cancelJob() throws Exception {
        String jobId = "JOBID-1";
        XenonProto.Job request = buildJob(jobId);
        JobStatus status = new JobStatusImplementation(jobId, "COMPLETED", 0, null, false, true, new HashMap<>());
        when(scheduler.cancelJob(jobId)).thenReturn(status);

        XenonProto.JobStatus response = client.cancelJob(request);

        XenonProto.JobStatus expected = buildJobStatus(request);
        assertEquals(expected, response);
    }

    private XenonProto.JobStatus buildJobStatus(XenonProto.Job request) {
        return XenonProto.JobStatus.newBuilder()
                .setJob(request)
                .setRunning(false)
                .setDone(true)
                .setExitCode(0)
                .setState("COMPLETED")
                .build();
    }

    @Test
    public void getJobStatus() throws Exception {
        String jobId = "JOBID-1";
        XenonProto.Job request = buildJob(jobId);
        JobStatus status = new JobStatusImplementation(jobId, "COMPLETED", 0, null, false, true, new HashMap<>());
        when(scheduler.getJobStatus(jobId)).thenReturn(status);

        XenonProto.JobStatus response = client.getJobStatus(request);

        XenonProto.JobStatus expected = buildJobStatus(request);
        assertEquals(expected, response);
    }

    @Test
    public void getJobStatuses() throws Exception {
        String jobId = "JOBID-1";
        XenonProto.Jobs request = XenonProto.Jobs.newBuilder()
                .addJobs(buildJob(jobId))
                .build();
        JobStatus status = new JobStatusImplementation(jobId, "COMPLETED", 0, null, false, true, new HashMap<>());
        when(scheduler.getJobStatuses(jobId)).thenReturn(new JobStatus[]{status});

        XenonProto.JobStatuses response = client.getJobStatuses(request);

        XenonProto.JobStatuses expected = XenonProto.JobStatuses.newBuilder()
                .addStatuses(buildJobStatus(buildJob(jobId)))
                .build();
        assertEquals(expected, response);
    }

    @Test
    public void waitUntilDone() throws Exception {
        String jobId = "JOBID-1";
        XenonProto.JobWithTimeout request = XenonProto.JobWithTimeout.newBuilder()
                .setId(jobId)
                .setScheduler(createScheduler())
                .setTimeout(42L)
                .build();
        JobStatus status = new JobStatusImplementation(jobId, "COMPLETED", 0, null, false, true, new HashMap<>());
        when(scheduler.waitUntilDone(jobId, 42L)).thenReturn(status);

        XenonProto.JobStatus response = client.waitUntilDone(request);

        XenonProto.JobStatus expected = buildJobStatus(buildJob(jobId));
        assertEquals(expected, response);
    }

    @Test
    public void waitUntilRunning() throws Exception {
        String jobId = "JOBID-1";
        XenonProto.JobWithTimeout request = XenonProto.JobWithTimeout.newBuilder()
                .setId(jobId)
                .setScheduler(createScheduler())
                .setTimeout(42L)
                .build();
        JobStatus status = new JobStatusImplementation(jobId, "COMPLETED", 0, null, false, true, new HashMap<>());
        when(scheduler.waitUntilRunning(jobId, 42L)).thenReturn(status);

        XenonProto.JobStatus response = client.waitUntilRunning(request);

        XenonProto.JobStatus expected = buildJobStatus(buildJob(jobId));
        assertEquals(expected, response);
    }

    @Test
    public void getJobs() throws Exception {
        String queueName = "somequeue";
        XenonProto.SchedulerAndQueues request = XenonProto.SchedulerAndQueues.newBuilder()
                .setScheduler(createScheduler())
                .addQueues(queueName)
                .build();
        String[] jobs = new String[]{"JOBID-1"};
        when(scheduler.getJobs(queueName)).thenReturn(jobs);

        XenonProto.Jobs response = client.getJobs(request);

        XenonProto.Jobs expected = XenonProto.Jobs.newBuilder()
                .addJobs(buildJob("JOBID-1"))
                .build();
        assertEquals(expected, response);
    }

    @Test
    public void create() {
        XenonProto.CreateSchedulerRequest request = XenonProto.CreateSchedulerRequest.newBuilder()
                .setAdaptor("local")
                .setDefaultCred(XenonProto.DefaultCredential.newBuilder().setUsername("user1"))
                .build();

        XenonProto.Scheduler response = client.createScheduler(request);

        String schedId = "local://user1@local://";
        XenonProto.Scheduler expected = XenonProto.Scheduler.newBuilder()
                .setId(schedId)
                .setRequest(request)
                .build();
        assertEquals(expected, response);
    }
}