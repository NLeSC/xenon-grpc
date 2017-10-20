package nl.esciencecenter.xenon.grpc.schedulers.maputils;

import static nl.esciencecenter.xenon.grpc.schedulers.MapUtils.mapJobDescription;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.schedulers.JobDescription;

import org.junit.Before;
import org.junit.Test;

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
        builder.setExecutable("calc")
            .addAllArguments(Arrays.asList("myarg1", "myarg2"))
            .setWorkingDirectory("/tmp/work/dir")
            .putEnvironment("MYKEY", "myvalue")
            .setQueueName("important")
            .setMaxRuntime(1024)
            .setNodeCount(16)
            .setProcessesPerNode(4)
            .setStartSingleProcess(true)
            .setStdin("/tmp/stdin.txt")
            .setStderr("/tmp/stderr.txt")
            .setStdout("/tmp/stdout.txt")
            .putOptions("xenon.adaptors.schedulers.ssh.agent", "true")
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
        expected.setNodeCount(16);
        expected.setProcessesPerNode(4);
        expected.setStartSingleProcess(true);
        expected.setStdin("/tmp/stdin.txt");
        expected.setStdout("/tmp/stdout.txt");
        expected.setStderr("/tmp/stderr.txt");
        Map<String, String> opts = new HashMap<>();
        opts.put("xenon.adaptors.schedulers.ssh.agent", "true");
        expected.setJobOptions(opts);
        assertEquals(expected, response);
    }
}
