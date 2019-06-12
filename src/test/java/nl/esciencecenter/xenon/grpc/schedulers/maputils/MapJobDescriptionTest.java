package nl.esciencecenter.xenon.grpc.schedulers.maputils;

import static nl.esciencecenter.xenon.grpc.schedulers.MapUtils.mapJobDescription;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.schedulers.JobDescription;

public class MapJobDescriptionTest {
    private XenonProto.JobDescription.Builder builder;

    @Before
    public void setUp() {
        builder = XenonProto.JobDescription.newBuilder();
    }

    @Test
    public void minimal() {
        builder.setExecutable("calc");

        XenonProto.JobDescription request = builder.build();
        JobDescription response = mapJobDescription(request);

        JobDescription expected = new JobDescription();
        expected.setExecutable("calc");
        assertEquals(expected, response);
    }

    @Test
    public void custom() {
        List<String> schedulerArgs = Arrays.asList("schedarg1", "schedarg2");
        builder.setExecutable("calc")
            .addAllArguments(Arrays.asList("myarg1", "myarg2"))
            .setWorkingDirectory("/tmp/work/dir")
            .putEnvironment("MYKEY", "myvalue")
            .setQueueName("important")
            .setMaxRuntime(1024)
            .setCoresPerTask(16)
            .setTasksPerNode(4)
            .setStartPerTask(true)
            .setStdin("/tmp/stdin.txt")
            .setStderr("/tmp/stderr.txt")
            .setStdout("/tmp/stdout.txt")
            .setName("myjobname")
            .setMaxMemory(4096)
            .addAllSchedulerArguments(schedulerArgs)
        ;

        XenonProto.JobDescription request = builder.build();
        JobDescription response = mapJobDescription(request);

        JobDescription expected = new JobDescription();
        expected.setExecutable("calc");
        expected.setArguments("myarg1", "myarg2");
        expected.setWorkingDirectory("/tmp/work/dir");
        Map<String, String> envs = new HashMap<>();
        envs.put("MYKEY", "myvalue");
        expected.setEnvironment(envs);
        expected.setQueueName("important");
        expected.setMaxRuntime(1024);
        expected.setCoresPerTask(16);
        expected.setTasksPerNode(4);
        expected.setStartPerTask();
        expected.setStdin("/tmp/stdin.txt");
        expected.setStdout("/tmp/stdout.txt");
        expected.setStderr("/tmp/stderr.txt");
        expected.setName("myjobname");
        expected.setMaxMemory(4096);
        expected.setSchedulerArguments("schedarg1", "schedarg2");
        assertEquals(expected, response);
        assertEquals(schedulerArgs, response.getSchedulerArguments());
    }
}
