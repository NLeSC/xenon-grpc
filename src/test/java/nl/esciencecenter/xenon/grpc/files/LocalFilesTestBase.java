package nl.esciencecenter.xenon.grpc.files;

import java.io.IOException;

import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.grpc.MapUtils;
import nl.esciencecenter.xenon.grpc.XenonFilesGrpc;
import nl.esciencecenter.xenon.grpc.XenonProto;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.internal.ServerImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

abstract public class LocalFilesTestBase {
    @Rule
    public TemporaryFolder myfolder = new TemporaryFolder();
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    XenonSingleton singleton;
    ServerImpl server;
    ManagedChannel channel;
    XenonFilesGrpc.XenonFilesBlockingStub client;

    @Before
    public void setUp() throws IOException {
        singleton = new XenonSingleton();
        FileSystemsService service = new FileSystemsService(singleton);
        String uniqueServerName = "in-process server for " + getClass();
        server = InProcessServerBuilder.forName(uniqueServerName).directExecutor().addService(service).build();
        server.start();
        channel = InProcessChannelBuilder.forName(uniqueServerName).directExecutor().usePlaintext(true).build();
        client = XenonFilesGrpc.newBlockingStub(channel);
    }

    @After
    public void tearDown() throws XenonException {
        singleton.close();
        channel.shutdownNow();
        server.shutdownNow();
    }

    /**
     * @return first local fs
     */
    XenonProto.FileSystem getFs() {
        return client.localFileSystems(MapUtils.empty()).getFilesystems(0);
    }

    XenonProto.Path getLocalPath(String path) {
        XenonProto.FileSystem fs = getFs();
        return XenonProto.Path.newBuilder().setFilesystem(fs).setPath(path).build();
    }
}
