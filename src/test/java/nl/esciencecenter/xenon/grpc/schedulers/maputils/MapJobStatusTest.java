package nl.esciencecenter.xenon.grpc.schedulers.maputils;

import static nl.esciencecenter.xenon.grpc.schedulers.MapUtils.mapJobStatus;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import nl.esciencecenter.xenon.adaptors.schedulers.JobCanceledException;
import nl.esciencecenter.xenon.adaptors.schedulers.JobStatusImplementation;
import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.schedulers.JobStatus;

import org.junit.Before;
import org.junit.Test;

public class MapJobStatusTest {

    private XenonProto.JobStatus.Builder builder;
    private XenonProto.Scheduler scheduler;

    @Before
    public void setUp() {
        scheduler = XenonProto.Scheduler.getDefaultInstance();
        builder = XenonProto.JobStatus.newBuilder();
    }

    @Test
    public void minimal() {
        JobStatus request = new JobStatusImplementation("JOBID-1", "COMPLETED", null, null, false, false, null);
        XenonProto.JobStatus response = mapJobStatus(request, scheduler);

        XenonProto.JobStatus expected = builder
            .setState("COMPLETED")
            .setJob(
                XenonProto.Job.newBuilder()
                    .setScheduler(scheduler)
                    .setId("JOBID-1")
            )
            .build();
        assertEquals(expected, response);
    }

    @Test
    public void completedOKWithInfo() {
        Map<String, String> info = new HashMap<>();
        info.put("runtime", "00:12:34");
        JobStatus request = new JobStatusImplementation("JOBID-1", "COMPLETED", 0, null, false, true, info);
        XenonProto.JobStatus response = mapJobStatus(request, scheduler);

        XenonProto.JobStatus expected = builder
            .setState("COMPLETED")
            .setDone(true)
            .setExitCode(0)
            .setJob(
                XenonProto.Job.newBuilder()
                    .setScheduler(scheduler)
                    .setId("JOBID-1")
            )
            .putSchedulerSpecificInformation("runtime", "00:12:34")
            .build();
        assertEquals(expected, response);
    }

    @Test
    public void completedWithException() {
        JobStatus request = new JobStatusImplementation("JOBID-1", "ERROR", 1, new JobCanceledException("slurm", "Killed"), false, true, new HashMap<>());
        XenonProto.JobStatus response = mapJobStatus(request, scheduler);

        XenonProto.JobStatus expected = builder
            .setState("ERROR")
            .setDone(true)
            .setExitCode(1)
            .setErrorMessage("slurm adaptor: Killed")
            .setErrorType(XenonProto.JobStatus.ErrorType.CANCELLED)
            .setJob(
                XenonProto.Job.newBuilder()
                    .setScheduler(scheduler)
                    .setId("JOBID-1")
            )
            .build();
        assertEquals(expected, response);
    }
}