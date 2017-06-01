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

public class LocalJobsServiceTestBase {
    @Rule
    public TemporaryFolder myfolder = new TemporaryFolder();
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    XenonSingleton singleton;
    ServerImpl server;
    ManagedChannel channel;
    XenonJobsGrpc.XenonJobsBlockingStub client;

    @Before
    public void SetUp() throws IOException {
        singleton = new XenonSingleton();
        JobsService service = new JobsService(singleton);
        server = InProcessServerBuilder.forName("test").addService(service).build();
        server.start();
        channel = InProcessChannelBuilder.forName("test").directExecutor().usePlaintext(true).build();
        client = XenonJobsGrpc.newBlockingStub(channel);
    }

    @After
    public void tearDown() throws XenonException {
        XenonFactory.endXenon(singleton.getInstance());
        channel.shutdownNow();
        server.shutdownNow();
    }

    /**
     * @return local scheduler
     */
    public XenonProto.Scheduler getScheduler() {
        XenonProto.Empty empty = XenonProto.Empty.getDefaultInstance();
        return client.localScheduler(empty);
    }
}
