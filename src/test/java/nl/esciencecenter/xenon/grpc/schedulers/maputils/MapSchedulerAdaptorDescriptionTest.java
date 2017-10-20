package nl.esciencecenter.xenon.grpc.schedulers.maputils;

import static nl.esciencecenter.xenon.grpc.schedulers.MapUtils.mapSchedulerAdaptorDescription;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import nl.esciencecenter.xenon.UnknownAdaptorException;
import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.schedulers.Scheduler;
import nl.esciencecenter.xenon.schedulers.SchedulerAdaptorDescription;

public class MapSchedulerAdaptorDescriptionTest {
    @Test
    public void test_it() throws UnknownAdaptorException {
        SchedulerAdaptorDescription xenon_description = Scheduler.getAdaptorDescription("local");

        XenonProto.SchedulerAdaptorDescription grpc_description = mapSchedulerAdaptorDescription(xenon_description);

        assertEquals("local", grpc_description.getName());
        assertThat(grpc_description.getDescription(), containsString("local queue"));
        assertFalse("supported locations not empty", grpc_description.getSupportedLocationsList().isEmpty());
        assertFalse("supported properties not empty", grpc_description.getSupportedPropertiesList().isEmpty());
        assertTrue("is embedded", grpc_description.getIsEmbedded());
        assertTrue("supports batch", grpc_description.getSupportsBatch());
        assertTrue("supports interactive", grpc_description.getSupportsInteractive());
        assertTrue("uses fs", grpc_description.getUsesFileSystem());
    }
}
