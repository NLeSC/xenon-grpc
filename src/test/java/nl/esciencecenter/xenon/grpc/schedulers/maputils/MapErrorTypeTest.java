package nl.esciencecenter.xenon.grpc.schedulers.maputils;

import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.adaptors.schedulers.JobCanceledException;
import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.schedulers.NoSuchJobException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static nl.esciencecenter.xenon.grpc.schedulers.MapUtils.mapErrorType;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class MapErrorTypeTest {
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                { new JobCanceledException("local", "Job cancelled"), XenonProto.JobStatus.ErrorType.CANCELLED},
                { new NoSuchJobException("local", "Not found"), XenonProto.JobStatus.ErrorType.NOT_FOUND },
                { new XenonException("local", "Something bad"), XenonProto.JobStatus.ErrorType.XENON},
                { new IOException(), XenonProto.JobStatus.ErrorType.IO },
                { new Exception(), XenonProto.JobStatus.ErrorType.OTHER }
        });
    }

    @Parameterized.Parameter()
    public Exception input;
    @Parameterized.Parameter(1)
    public XenonProto.JobStatus.ErrorType expected;

    @Test
    public void test_mapErrorType() {
        XenonProto.JobStatus.ErrorType result = mapErrorType(input);

        assertEquals(expected, result);
    }
}
