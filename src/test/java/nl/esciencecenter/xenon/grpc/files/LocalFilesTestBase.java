package nl.esciencecenter.xenon.grpc.files;

import java.io.IOException;

import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.XenonFactory;
import nl.esciencecenter.xenon.grpc.XenonFilesGrpc;
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

public class LocalFilesTestBase {
    @Rule
    public TemporaryFolder myfolder = new TemporaryFolder();
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    XenonSingleton singleton;
    ServerImpl server;
    ManagedChannel channel;
    XenonFilesGrpc.XenonFilesBlockingStub client;

    @Before
    public void SetUp() throws IOException {
        singleton = new XenonSingleton();
        FilesService service = new FilesService(singleton);
        server = InProcessServerBuilder.forName("test").addService(service).build();
        server.start();
        channel = InProcessChannelBuilder.forName("test").directExecutor().usePlaintext(true).build();
        client = XenonFilesGrpc.newBlockingStub(channel);
    }

    @After
    public void tearDown() throws XenonException {
        XenonFactory.endXenon(singleton.getInstance());
        channel.shutdownNow();
        server.shutdownNow();
    }

    /**
     * @return first local fs
     */
    XenonProto.FileSystem getFs() {
        return client.localFileSystems(empty()).getFilesystems(0);
    }

    public static XenonProto.Empty empty() {
        return XenonProto.Empty.getDefaultInstance();
    }

    XenonProto.Path getLocalPath(String path) {
        XenonProto.FileSystem fs =getFs();
        return XenonProto.Path.newBuilder().setFilesystem(fs).setPath(path).build();
    }
}
