package nl.esciencecenter.xenon.grpc.maputils;

import static nl.esciencecenter.xenon.grpc.MapUtils.mapException;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import io.grpc.Status;
import io.grpc.StatusException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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

@RunWith(Parameterized.class)
public class MapExceptionTest {
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { Status.NOT_FOUND.asException(), "NOT_FOUND" },
                { new NoSuchPathException("file", "/some/file"), "NOT_FOUND: nl.esciencecenter.xenon.filesystems.NoSuchPathException: file adaptor: /some/file" },
                { new UnknownAdaptorException("foobar", "Unknown adaptor"), "NOT_FOUND: nl.esciencecenter.xenon.UnknownAdaptorException: foobar adaptor: Unknown adaptor"},
                { new UnknownPropertyException("file", "Unknown property 'x'") , "NOT_FOUND: nl.esciencecenter.xenon.UnknownPropertyException: file adaptor: Unknown property 'x'"},
                { new PathAlreadyExistsException("file", "/some/file"), "ALREADY_EXISTS: nl.esciencecenter.xenon.filesystems.PathAlreadyExistsException: file adaptor: /some/file" },
                { new UnsupportedOperationException("file", "Unsupported operation"), "UNIMPLEMENTED: nl.esciencecenter.xenon.UnsupportedOperationException: file adaptor: Unsupported operation" },
                { new UnsupportedJobDescriptionException("local", "Cannot apply max time"), "UNIMPLEMENTED: nl.esciencecenter.xenon.schedulers.UnsupportedJobDescriptionException: local adaptor: Cannot apply max time"},
                { new IllegalArgumentException("x can not be null"), "INVALID_ARGUMENT: java.lang.IllegalArgumentException: x can not be null" },
                { new IncompleteJobDescriptionException("local", "executable is required"), "INVALID_ARGUMENT: nl.esciencecenter.xenon.schedulers.IncompleteJobDescriptionException: local adaptor: executable is required" },
                { new NotConnectedException("slurm", "SSH Connection is closed"), "UNAVAILABLE: nl.esciencecenter.xenon.adaptors.NotConnectedException: slurm adaptor: SSH Connection is closed" },
                { new InvalidPathException("webdav", "'?' character is not allowed"), "FAILED_PRECONDITION: nl.esciencecenter.xenon.filesystems.InvalidPathException: webdav adaptor: '?' character is not allowed" },
                { new InvalidPropertyException("local", "slots property must be of size type"), "FAILED_PRECONDITION: nl.esciencecenter.xenon.InvalidPropertyException: local adaptor: slots property must be of size type" },
                { new InvalidCredentialException("webdav", "Webdav filesystem does not accept CertificateCredentail"), "FAILED_PRECONDITION: nl.esciencecenter.xenon.InvalidCredentialException: webdav adaptor: Webdav filesystem does not accept CertificateCredentail"},
                { new InvalidLocationException("webdav", "Location must be url"), "FAILED_PRECONDITION: nl.esciencecenter.xenon.InvalidLocationException: webdav adaptor: Location must be url" },
                { new InvalidJobDescriptionException("local", "max time must be >=0"), "FAILED_PRECONDITION: nl.esciencecenter.xenon.schedulers.InvalidJobDescriptionException: local adaptor: max time must be >=0" },
                { new XenonException("slurm", "Something bad happened"), "INTERNAL: nl.esciencecenter.xenon.XenonException: slurm adaptor: Something bad happened"},
                { new IOException("Disk is full"), "INTERNAL: java.io.IOException: Disk is full" },
                { new Exception("Fallback"), "INTERNAL: java.lang.Exception: Fallback"}
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
