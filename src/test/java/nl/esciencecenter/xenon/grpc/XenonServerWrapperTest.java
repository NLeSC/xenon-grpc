package nl.esciencecenter.xenon.grpc;

import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import org.junit.Test;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.*;

public class XenonServerWrapperTest {

    @Test
    public void buildArgumentParser() {
        XenonServerWrapper wrapper = new XenonServerWrapper();

        ArgumentParser parser = wrapper.buildArgumentParser();

        String help = parser.formatHelp();
        assertThat(help, containsString("port"));
        assertThat(help, containsString("TLS"));
    }

    @Test
    public void parseArgs_noargs() throws ArgumentParserException {
        XenonServerWrapper wrapper = new XenonServerWrapper();

        wrapper.parseArgs(new String[0]);

        assertEquals("Default port", XenonServerWrapper.DEFAULT_PORT, wrapper.getPort());
        assertFalse("no TLS", wrapper.getUseTLS());
    }
}