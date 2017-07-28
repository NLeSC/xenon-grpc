package nl.esciencecenter.xenon.grpc.jobs;

import static nl.esciencecenter.xenon.grpc.MapUtils.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import nl.esciencecenter.xenon.grpc.XenonProto;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests which at start has a job pending or running job in the local scheduler
 */
public class LocalJobSleeperTest extends LocalSchedulersServiceTestBase {
    private XenonProto.Job job;

    @Before
    public void submitJob() {
        XenonProto.JobDescription description = XenonProto.JobDescription.newBuilder()
            .setExecutable("sleep")
            .addArguments("60")
            .setQueueName("multi")
            .setWorkingDirectory(myfolder.getRoot().getAbsolutePath())
            .build();
        XenonProto.SubmitJobRequest request = XenonProto.SubmitJobRequest.newBuilder()
            .setDescription(description)
            .setScheduler(getScheduler())
            .build();
        job = client.submitJob(request);
    }

    @After
    public void cleanupJob() {
        client.deleteJob(job);
    }

    @Test
    public void listJobs() {
        XenonProto.Jobs jobs = client.listJobs(empty());

        XenonProto.Jobs expected = XenonProto.Jobs.newBuilder().addJobs(job).build();
        assertEquals(expected, jobs);
    }

    @Test
    public void getJobs_anyqueue_present() {
        XenonProto.SchedulerAndQueues request = XenonProto.SchedulerAndQueues.newBuilder()
            .setScheduler(getScheduler())
            .build();

        XenonProto.Jobs jobs = client.getJobs(request);

        XenonProto.Jobs expected = XenonProto.Jobs.newBuilder().addJobs(job).build();
        assertEquals(expected, jobs);
    }

    @Test
    public void getJobs_multiqueue_present() {
        XenonProto.SchedulerAndQueues request = XenonProto.SchedulerAndQueues.newBuilder()
            .setScheduler(getScheduler())
            .addQueues("multi")
            .build();

        XenonProto.Jobs jobs = client.getJobs(request);

        XenonProto.Jobs expected = XenonProto.Jobs.newBuilder().addJobs(job).build();
        assertEquals(expected, jobs);
    }

    @Test
    public void getJobStatuses() {
        XenonProto.Jobs request = XenonProto.Jobs.newBuilder()
            .addJobs(job)
            .build();

        XenonProto.JobStatuses response = client.getJobStatuses(request);

        assertEquals("has single job", 1, response.getStatusesCount());
        assertEquals(job, response.getStatuses(0).getJob());
    }

    @Test
    public void getJobStatus_pendingorrunning() {
        XenonProto.JobStatus status = client.getJobStatus(job);

        assertTrue(
            "Job is running or pending",
            (status.getState().equals("PENDING") || status.getState().equals("RUNNING"))
        );
    }

    @Test
    public void cancelJob_cancelled() {
        XenonProto.JobStatus status = client.cancelJob(job);

        assertEquals(XenonProto.JobStatus.ErrorType.CANCELLED, status.getErrorType());
    }
}
