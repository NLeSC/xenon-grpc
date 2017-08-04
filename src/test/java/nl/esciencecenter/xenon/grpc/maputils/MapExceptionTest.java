package nl.esciencecenter.xenon.grpc.maputils;

import io.grpc.Status;
import io.grpc.StatusException;
import nl.esciencecenter.xenon.InvalidCredentialException;
import nl.esciencecenter.xenon.InvalidLocationException;
import nl.esciencecenter.xenon.InvalidPropertyException;
import nl.esciencecenter.xenon.UnknownAdaptorException;
import nl.esciencecenter.xenon.UnknownPropertyException;
import nl.esciencecenter.xenon.UnsupportedOperationException;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.adaptors.NotConnectedException;
import nl.esciencecenter.xenon.filesystems.InvalidPathException;
import nl.esciencecenter.xenon.filesystems.NoSuchPathException;
import nl.esciencecenter.xenon.filesystems.PathAlreadyExistsException;
import nl.esciencecenter.xenon.schedulers.IncompleteJobDescriptionException;
import nl.esciencecenter.xenon.schedulers.InvalidJobDescriptionException;
import nl.esciencecenter.xenon.schedulers.UnsupportedJobDescriptionException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static nl.esciencecenter.xenon.grpc.MapUtils.mapException;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class MapExceptionTest {
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { Status.NOT_FOUND.asException(), "NOT_FOUND" },
                { new NoSuchPathException("file", "/some/file"), "NOT_FOUND: file adaptor: /some/file" },
                { new UnknownAdaptorException("foobar", "Unknown adaptor"), "NOT_FOUND: foobar adaptor: Unknown adaptor"},
                { new UnknownPropertyException("file", "Unknown property 'x'") , "NOT_FOUND: file adaptor: Unknown property 'x'"},
                { new PathAlreadyExistsException("file", "/some/file"), "ALREADY_EXISTS: file adaptor: /some/file" },
                { new UnsupportedOperationException("file", "Unsupported operation"), "UNIMPLEMENTED: file adaptor: Unsupported operation" },
                { new UnsupportedJobDescriptionException("local", "Cannot apply max time"), "UNIMPLEMENTED: local adaptor: Cannot apply max time"},
                { new IllegalArgumentException("x can not be null"), "INVALID_ARGUMENT: x can not be null" },
                { new IncompleteJobDescriptionException("local", "executable is required"), "INVALID_ARGUMENT: local adaptor: executable is required" },
                { new NotConnectedException("slurm", "SSH Connection is closed"), "UNAVAILABLE: slurm adaptor: SSH Connection is closed" },
                { new InvalidPathException("webdav", "'?' character is not allowed"), "FAILED_PRECONDITION: webdav adaptor: '?' character is not allowed" },
                { new InvalidPropertyException("local", "slots property must be of size type"), "FAILED_PRECONDITION: local adaptor: slots property must be of size type" },
                { new InvalidCredentialException("webdav", "Webdav filesystem does not accept CertificateCredentail"), "FAILED_PRECONDITION: webdav adaptor: Webdav filesystem does not accept CertificateCredentail"},
                { new InvalidLocationException("webdav", "Location must be url"), "FAILED_PRECONDITION: webdav adaptor: Location must be url" },
                { new InvalidJobDescriptionException("local", "max time must be >=0"), "FAILED_PRECONDITION: local adaptor: max time must be >=0" },
                { new XenonException("slurm", "Something bad happened"), "INTERNAL: slurm adaptor: Something bad happened"},
                { new IOException("Disk is full"), "INTERNAL: Disk is full" },
                { new Exception("Fallback"), "INTERNAL: Fallback"}
        });
    }

    @Parameterized.Parameter()
    public Exception input;

    @Parameterized.Parameter(1)
    public String expected;

    @Test
    public void test_mapException() {
        StatusException result = mapException(input);

        assertEquals(expected, result.getMessage());
    }
}
