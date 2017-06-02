package nl.esciencecenter.xenon.grpc;

import io.grpc.ManagedChannel;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.internal.ServerImpl;
import nl.esciencecenter.xenon.XenonException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import static nl.esciencecenter.xenon.grpc.MapUtils.empty;
import static org.junit.Assert.*;

public class GlobalServiceTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    XenonSingleton singleton;
    ServerImpl server;
    ManagedChannel channel;
    XenonGlobalGrpc.XenonGlobalBlockingStub client;

    @Before
    public void setUp() throws IOException {
        singleton = new XenonSingleton();
        GlobalService service = new GlobalService(singleton);
        String uniqueServerName = "in-process server for " + getClass();
        server = InProcessServerBuilder.forName(uniqueServerName).directExecutor().addService(service).build();
        server.start();
        channel = InProcessChannelBuilder.forName(uniqueServerName).directExecutor().usePlaintext(true).build();
        client = XenonGlobalGrpc.newBlockingStub(channel);
    }

    @After
    public void tearDown() throws XenonException {
        singleton.close();
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    public void newXenon_uninitialized() throws Exception {
        XenonProto.Properties request = XenonProto.Properties.getDefaultInstance();
        XenonProto.Empty response = client.newXenon(request);

        assertEquals(empty(), response);
    }

    @Test
    public void newXenon_initialized() throws Exception {
        thrown.expect(StatusRuntimeException.class);
        thrown.expectMessage("FAILED_PRECONDITION: Unable to set properties after other calls");

        // initialize xenon by doing something
        client.getSupportedProperties(empty());

        XenonProto.Properties request = XenonProto.Properties.getDefaultInstance();
        client.newXenon(request);
    }

    @Test
    public void getSupportedProperties() {
        XenonProto.PropertyDescriptions props = client.getSupportedProperties(empty());

        assertEquals(9, props.getPropertiesCount());
    }
}