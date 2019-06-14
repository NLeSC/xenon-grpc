package nl.esciencecenter.xenon.grpc;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.contrib.java.lang.system.SystemOutRule;

import java.io.IOException;

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
        exit.checkAssertionAfterwards(() -> {
            assertThat(systemOutRule.getLog(), containsString("package xenon"));
            assertThat(systemOutRule.getLog(), containsString("service FileSystemService"));
            assertThat(systemOutRule.getLog(), containsString("service SchedulerService"));
        });
        XenonServerWrapper wrapper = new XenonServerWrapper();

        wrapper.parseArgs(new String[]{"--proto"});
    }

    @Test
    public void getPort() {
        XenonServerWrapper wrapper = new XenonServerWrapper();

        Integer expected = 50051;
        assertEquals(expected, wrapper.getPort());
    }

    @Test
    public void getUseTLS() {
        XenonServerWrapper wrapper = new XenonServerWrapper();

        assertFalse(wrapper.getUseTLS());
    }

    @Test
    public void startstop() throws IOException {
        exit.checkAssertionAfterwards(() -> assertThat(systemOutRule.getLog(), containsString("Server started, listening on port")));

        XenonServerWrapper wrapper = new XenonServerWrapper();
        wrapper.start(new String[] {});
        wrapper.stop();
    }

    @Test
    public void run_version() throws IOException {
        exit.expectSystemExitWithStatus(0);
        exit.checkAssertionAfterwards(() -> Assert.assertThat(systemOutRule.getLog(), CoreMatchers.containsString("Xenon gRPC v")));

        XenonServerWrapper wrapper = new XenonServerWrapper();
        wrapper.start(new String[]{"--version"});
    }
}
