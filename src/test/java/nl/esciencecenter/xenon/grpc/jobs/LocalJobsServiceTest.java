package nl.esciencecenter.xenon.grpc.jobs;

import static nl.esciencecenter.xenon.grpc.MapUtils.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import nl.esciencecenter.xenon.adaptors.local.LocalAdaptor;
import nl.esciencecenter.xenon.grpc.XenonProto;

import io.grpc.StatusRuntimeException;
import org.junit.Test;

public class LocalJobsServiceTest extends LocalJobsServiceTestBase {
    @Test
    public void listJobs_empty() {
        XenonProto.Jobs response = client.listJobs(empty());

        assertTrue(response.getJobsList().isEmpty());
    }

    @Test
    public void getJobs_empty() {
        XenonProto.SchedulerAndQueues request = XenonProto.SchedulerAndQueues.newBuilder()
            .setScheduler(getScheduler())
            .build();
        XenonProto.Jobs response = client.getJobs(request);

        assertTrue(response.getJobsList().isEmpty());
    }

    @Test
    public void getJobStatuses_empty() {
        XenonProto.Jobs request = XenonProto.Jobs.newBuilder().build();

        XenonProto.JobStatuses response = client.getJobStatuses(request);

        assertTrue(response.getStatusesList().isEmpty());
    }

    @Test
    public void getDefaultQueueName() {
        XenonProto.Queue response = client.getDefaultQueueName(getScheduler());

        assertEquals("single", response.getName());
    }

    @Test
    public void getQueues() {
        XenonProto.Queues response = client.getQueues(getScheduler());

        HashSet<String> expected = new HashSet<>(Arrays.asList("single", "multi", "unlimited"));
        assertEquals(expected, new HashSet<>(response.getNameList()));
    }

    @Test
    public void getQueueStatus() {
        XenonProto.SchedulerAndQueue request = XenonProto.SchedulerAndQueue.newBuilder()
            .setScheduler(getScheduler())
            .setQueue("multi")
            .build();

        XenonProto.QueueStatus response = client.getQueueStatus(request);

        XenonProto.QueueStatus expected = expectedQueueStatus();
        assertEquals(expected, response);
    }

    private XenonProto.QueueStatus expectedQueueStatus() {
        return XenonProto.QueueStatus.newBuilder()
                .setName("multi")
                .setScheduler(getScheduler())
                .build();
    }

    @Test
    public void getQueueStatuses() {
        XenonProto.SchedulerAndQueues request = XenonProto.SchedulerAndQueues.newBuilder()
            .setScheduler(getScheduler())
            .addQueues("multi")
            .build();

        XenonProto.QueueStatuses response = client.getQueueStatuses(request);

        XenonProto.QueueStatuses expected = XenonProto.QueueStatuses.newBuilder()
            .addStatuses(expectedQueueStatus())
            .build();
        assertEquals(expected, response);
    }

    @Test
    public void getJobStatus_notfound() {
        client.getJobStatus(getNotFoundJob());
    }

    @Test
    public void cancelJob_notfound() {
        client.cancelJob(getNotFoundJob());
    }

    @Test
    public void deleteJob_notfound() {
        client.deleteJob(getNotFoundJob());
    }

    @Test
    public void waitUntilDone_notfound() {
        client.waitUntilDone(getNotFoundJob());
    }

    @Test
    public void waitUntilRunning_notfound() {
        client.waitUntilRunning(getNotFoundJob());
    }

    private XenonProto.Job getNotFoundJob() {
        String someId = "some-id-that-does-not-exist";
        thrown.expect(StatusRuntimeException.class);
        thrown.expectMessage("NOT_FOUND: " + someId);
        return XenonProto.Job.newBuilder()
            .setId(someId)
            .build();
    }

    @Test
    public void submitJob() {
        XenonProto.JobDescription description = XenonProto.JobDescription.newBuilder()
                .setExecutable("hostname")
                .build();
        XenonProto.SubmitJobRequest request = XenonProto.SubmitJobRequest.newBuilder()
                .setDescription(description)
                .setScheduler(getScheduler())
                .build();
        XenonProto.Job job = client.submitJob(request);
        try {
            XenonProto.JobStatus doneStatus = client.waitUntilDone(job);

            assertEquals(0, doneStatus.getExitCode());
            // TODO assert more
        } finally {
            client.deleteJob(job);
        }
    }


    @Test
    public void submitJob_storeouterr2files() throws IOException {
        XenonProto.JobDescription description = XenonProto.JobDescription.newBuilder()
                .setExecutable("bash")
                .addAllArguments(Arrays.asList("-c", "echo Hello World!"))
                .setStdOut("stdout.txt")
                .setStdErr("stderr.txt")
                .setWorkingDirectory(myfolder.getRoot().getAbsolutePath())
                .build();
        XenonProto.SubmitJobRequest request = XenonProto.SubmitJobRequest.newBuilder()
                .setDescription(description)
                .setScheduler(getScheduler())
                .build();
        XenonProto.Job job = client.submitJob(request);
        try {
            XenonProto.JobStatus doneStatus = client.waitUntilDone(job);

            assertEquals("Exit code",0, doneStatus.getExitCode());
            List<String> stdOutLines = Files.readAllLines(new File(myfolder.getRoot(), "stdout.txt").toPath());
            assertEquals("stdout.txt", Collections.singletonList("Hello World!"), stdOutLines);
            List<String> stdErrLines = Files.readAllLines(new File(myfolder.getRoot(), "stderr.txt").toPath());
            assertTrue("stderr.txt", stdErrLines.isEmpty());
        } finally {
            client.deleteJob(job);
        }
    }

    @Test
    public void getAdaptorDescriptions() {
        XenonProto.JobAdaptorDescriptions response = client.getAdaptorDescriptions(empty());

        assertEquals(5, response.getDescriptionsCount());
    }

    @Test
    public void getAdaptorDescription() {
        XenonProto.AdaptorName request = XenonProto.AdaptorName.newBuilder().setName("local").build();

        XenonProto.JobAdaptorDescription response = client.getAdaptorDescription(request);

        XenonProto.JobAdaptorDescription expected = XenonProto.JobAdaptorDescription.newBuilder()
            .setName("local")
            .setDescription(LocalAdaptor.ADAPTOR_DESCRIPTION)
            .addAllSupportedLocations(Arrays.asList("(null)", "(empty string)", "/"))
            .build();
        assertEquals(expected, response);
    }
}
