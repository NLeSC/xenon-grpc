package nl.esciencecenter.xenon.grpc.schedulers.maputils;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.adaptors.schedulers.QueueStatusImplementation;
import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.grpc.schedulers.MapUtils;
import nl.esciencecenter.xenon.schedulers.QueueStatus;
import nl.esciencecenter.xenon.schedulers.Scheduler;

public class MapQueueStatusTest {
    private XenonProto.QueueStatus.Builder builder;
    private Scheduler xScheduler;

    @Before
    public void setUp() throws XenonException {
        builder = XenonProto.QueueStatus.newBuilder();
        xScheduler = Scheduler.create("local");
    }

    @After
    public void tearDown() throws XenonException {
        xScheduler.close();
    }

    @Test
    public void minimal() {
        QueueStatus request = new QueueStatusImplementation(xScheduler, "somequeue", null, null);

        XenonProto.QueueStatus response = MapUtils.mapQueueStatus(request);

        XenonProto.QueueStatus expected = builder
            .setName("somequeue")
            .setErrorType(XenonProto.QueueStatus.ErrorType.NONE)
            .build();
        assertEquals(expected, response);
    }

    @Test
    public void withInfo() {
        Map<String, String> info = new HashMap<>();
        info.put("slots", "1024");
        QueueStatus request = new QueueStatusImplementation(xScheduler, "somequeue", null, info);

        XenonProto.QueueStatus response = MapUtils.mapQueueStatus(request);

        XenonProto.QueueStatus expected = builder
            .setName("somequeue")
            .putSchedulerSpecificInformation("slots", "1024")
            .setErrorType(XenonProto.QueueStatus.ErrorType.NONE)
            .build();
        assertEquals(expected, response);
    }

    @Test
    public void withException() {

        QueueStatus request = new QueueStatusImplementation(xScheduler, "somequeue", new Exception("Something bad"), null);

        XenonProto.QueueStatus response = MapUtils.mapQueueStatus(request);

        XenonProto.QueueStatus expected = builder
            .setName("somequeue")
            .setErrorMessage("Something bad")
            .setErrorType(XenonProto.QueueStatus.ErrorType.OTHER)
            .build();
        assertEquals(expected, response);
    }
}
