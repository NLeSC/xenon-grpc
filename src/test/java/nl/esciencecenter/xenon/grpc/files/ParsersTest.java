package nl.esciencecenter.xenon.grpc.files;

import static nl.esciencecenter.xenon.grpc.files.Parsers.parseCopyOptions;
import static nl.esciencecenter.xenon.grpc.files.Parsers.parseOpenOptions;
import static nl.esciencecenter.xenon.grpc.files.Parsers.parsePermissions;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.grpc.StatusException;
import nl.esciencecenter.xenon.files.CopyOption;
import nl.esciencecenter.xenon.files.OpenOption;
import nl.esciencecenter.xenon.files.PosixFilePermission;
import nl.esciencecenter.xenon.grpc.XenonProto;

import org.junit.Test;

public class ParsersTest {
    @Test
    public void test_parseOpenOptions_default_writecreate() throws Exception {
        // Protobuf defaults to first enum
        List<XenonProto.WriteRequest.OpenOption> request = Arrays.asList(XenonProto.WriteRequest.OpenOption.CREATE);

        OpenOption[] response = parseOpenOptions(request);

        OpenOption[] expected = new OpenOption[] {OpenOption.WRITE, OpenOption.CREATE};
        assertArrayEquals(expected, response);
    }

    @Test
    public void test_parseOpenOptions_all() throws Exception {
        // Protobuf defaults to first enum
        List<XenonProto.WriteRequest.OpenOption> request = Arrays.asList(
                XenonProto.WriteRequest.OpenOption.CREATE,
                XenonProto.WriteRequest.OpenOption.OPEN,
                XenonProto.WriteRequest.OpenOption.OPEN_OR_CREATE,
                XenonProto.WriteRequest.OpenOption.APPEND,
                XenonProto.WriteRequest.OpenOption.TRUNCATE
        );

        OpenOption[] response = parseOpenOptions(request);

        OpenOption[] expected = new OpenOption[] {
                OpenOption.WRITE,
                OpenOption.CREATE,
                OpenOption.OPEN,
                OpenOption.OPEN_OR_CREATE,
                OpenOption.APPEND,
                OpenOption.TRUNCATE
        };
        assertArrayEquals(expected, response);
    }

    @Test
    public void test_parsePermissions_default_empty() throws Exception {
        List<XenonProto.PosixFilePermission> request = Arrays.asList(XenonProto.PosixFilePermission.NONE);

        Set<PosixFilePermission> response = parsePermissions(request);

        Set<PosixFilePermission> expected = new HashSet<>();
        assertEquals(expected, response);
    }

    @Test
    public void test_parsePermissions_all_all() throws Exception {
        List<XenonProto.PosixFilePermission> request = Arrays.asList(
            XenonProto.PosixFilePermission.OWNER_READ,
            XenonProto.PosixFilePermission.OWNER_WRITE,
            XenonProto.PosixFilePermission.OWNER_EXECUTE,
            XenonProto.PosixFilePermission.GROUP_READ,
            XenonProto.PosixFilePermission.GROUP_WRITE,
            XenonProto.PosixFilePermission.GROUP_EXECUTE,
            XenonProto.PosixFilePermission.OTHERS_READ,
            XenonProto.PosixFilePermission.OTHERS_WRITE,
            XenonProto.PosixFilePermission.OTHERS_EXECUTE
        );

        Set<PosixFilePermission> response = parsePermissions(request);

        assertEquals(9, response.size());
    }

    @Test
    public void test_parseCopyOptions_default_create() throws Exception {
        List<XenonProto.CopyRequest.CopyOption> request = Arrays.asList(XenonProto.CopyRequest.CopyOption.CREATE);

        CopyOption[] response = parseCopyOptions(request);

        CopyOption[] expected = new CopyOption[]{CopyOption.CREATE};
        assertArrayEquals(expected, response);
    }

    @Test
    public void test_parseCopyOptions_all() throws StatusException {
        List<XenonProto.CopyRequest.CopyOption> request = Arrays.asList(
                XenonProto.CopyRequest.CopyOption.CREATE,
                XenonProto.CopyRequest.CopyOption.REPLACE,
                XenonProto.CopyRequest.CopyOption.IGNORE,
                XenonProto.CopyRequest.CopyOption.APPEND,
                XenonProto.CopyRequest.CopyOption.RESUME,
                XenonProto.CopyRequest.CopyOption.VERIFY
        );

        CopyOption[] response = parseCopyOptions(request);

        CopyOption[] expected = new CopyOption[]{
                CopyOption.CREATE,
                CopyOption.REPLACE,
                CopyOption.IGNORE,
                CopyOption.APPEND,
                CopyOption.RESUME,
                CopyOption.VERIFY,
        };
        assertArrayEquals(expected, response);
    }
}