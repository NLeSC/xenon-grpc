package nl.esciencecenter.xenon.grpc;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.contrib.java.lang.system.SystemOutRule;

public class XenonServerWrapperTest {
    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();
    @Rule
    public final SystemOutRule systemOutRule = new SystemOutRule().enableLog();

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

    @Test
    public void parseArgs_proto() throws ArgumentParserException {
        exit.expectSystemExitWithStatus(0);
        XenonServerWrapper wrapper = new XenonServerWrapper();

        wrapper.parseArgs(new String[]{"--proto"});

        assertThat(systemOutRule.getLog(), containsString("package xenon"));
        assertThat(systemOutRule.getLog(), containsString("service XenonGlobal"));
        assertThat(systemOutRule.getLog(), containsString("service XenonFiles"));
        assertThat(systemOutRule.getLog(), containsString("service XenonJobs"));
    }

}