package nl.esciencecenter.xenon.grpc.jobs;

import java.io.IOException;

import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.XenonFactory;
import nl.esciencecenter.xenon.grpc.XenonJobsGrpc;
import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.grpc.XenonSingleton;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.internal.ServerImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

abstract public class LocalJobsServiceTestBase {
    @Rule
    public TemporaryFolder myfolder = new TemporaryFolder();
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    XenonSingleton singleton;
    ServerImpl server;
    ManagedChannel channel;
    XenonJobsGrpc.XenonJobsBlockingStub client;

    @Before
    public void setUp() throws IOException {
        singleton = new XenonSingleton();
        JobsService service = new JobsService(singleton);
        String uniqueServerName = "in-process server for " + getClass();
        server = InProcessServerBuilder.forName(uniqueServerName).directExecutor().addService(service).build();
        server.start();
        channel = InProcessChannelBuilder.forName(uniqueServerName).directExecutor().usePlaintext(true).build();
        client = XenonJobsGrpc.newBlockingStub(channel);
    }

    @After
    public void tearDown() throws XenonException {
        singleton.close();
        channel.shutdownNow();
        server.shutdownNow();
    }

    /**
     * @return local scheduler
     */
    XenonProto.Scheduler getScheduler() {
        XenonProto.Empty empty = XenonProto.Empty.getDefaultInstance();
        return client.localScheduler(empty);
    }
}
