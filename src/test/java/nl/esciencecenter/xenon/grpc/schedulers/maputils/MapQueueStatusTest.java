package nl.esciencecenter.xenon.grpc.schedulers.maputils;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.adaptors.schedulers.QueueStatusImplementation;
import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.grpc.schedulers.MapUtils;
import nl.esciencecenter.xenon.schedulers.QueueStatus;
import nl.esciencecenter.xenon.schedulers.Scheduler;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MapQueueStatusTest {
    private XenonProto.QueueStatus.Builder builder;
    private XenonProto.Scheduler pScheduler;
    private Scheduler xScheduler;

    @Before
    public void setUp() throws XenonException {
        builder = XenonProto.QueueStatus.newBuilder();
        pScheduler = XenonProto.Scheduler.getDefaultInstance();
        xScheduler = Scheduler.create("local");
    }

    @After
    public void tearDown() throws XenonException {
        xScheduler.close();
    }

    @Test
    public void minimal() {
        QueueStatus request = new QueueStatusImplementation(xScheduler, "somequeue", null, null);

        XenonProto.QueueStatus response = MapUtils.mapQueueStatus(request, pScheduler);

        XenonProto.QueueStatus expected = builder
            .setName("somequeue")
            .setScheduler(pScheduler)
            .build();
        assertEquals(expected, response);
    }

    @Test
    public void withInfo() {
        Map<String, String> info = new HashMap<>();
        info.put("slots", "1024");
        QueueStatus request = new QueueStatusImplementation(xScheduler, "somequeue", null, info);

        XenonProto.QueueStatus response = MapUtils.mapQueueStatus(request, pScheduler);

        XenonProto.QueueStatus expected = builder
            .setName("somequeue")
            .setScheduler(pScheduler)
            .putSchedulerSpecificInformation("slots", "1024")
            .build();
        assertEquals(expected, response);
    }

    @Test
    public void withException() {

        QueueStatus request = new QueueStatusImplementation(xScheduler, "somequeue", new Exception("Something bad"), null);

        XenonProto.QueueStatus response = MapUtils.mapQueueStatus(request, pScheduler);

        XenonProto.QueueStatus expected = builder
            .setName("somequeue")
            .setScheduler(pScheduler)
            .setError("Something bad")
            .build();
        assertEquals(expected, response);
    }
}
