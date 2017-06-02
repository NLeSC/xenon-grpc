package nl.esciencecenter.xenon.grpc;

import io.grpc.StatusException;
import nl.esciencecenter.xenon.Xenon;
import nl.esciencecenter.xenon.adaptors.ssh.SshAdaptor;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class XenonSingletonTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private XenonSingleton single;

    @Before
    public void setUp() {
        single = new XenonSingleton();
    }

    @After
    public void tearDown() throws IOException {
        single.close();
    }

    @Test
    public void getInstance() throws Exception {
        Xenon xenon = single.getInstance();

        Map<String, String> props = xenon.getProperties();
        assertEquals(new HashMap<>(), props);
    }

    @Test
    public void setProperties_uninitialized() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("xenon.adaptors.ssh.loadSshConfig", "false");

        single.setProperties(props);

        Xenon xenon = single.getInstance();
        assertEquals(props, xenon.getProperties());
    }

    @Test
    public void setProperties_initialized() throws Exception {
        single.getInstance();
        Map<String, String> props = new HashMap<>();
        props.put("xenon.adaptors.ssh.loadSshConfig", "false");
        thrown.expect(StatusException.class);
        thrown.expectMessage("Unable to set properties after other calls");

        single.setProperties(props);
    }
}