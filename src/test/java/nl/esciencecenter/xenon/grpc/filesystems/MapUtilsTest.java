package nl.esciencecenter.xenon.grpc.filesystems;

import io.grpc.StatusException;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.filesystems.CopyMode;
import nl.esciencecenter.xenon.filesystems.FileSystem;
import nl.esciencecenter.xenon.filesystems.PosixFilePermission;
import nl.esciencecenter.xenon.grpc.XenonProto;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static nl.esciencecenter.xenon.grpc.filesystems.MapUtils.mapCopyMode;
import static nl.esciencecenter.xenon.grpc.filesystems.MapUtils.parsePermissions;
import static nl.esciencecenter.xenon.grpc.filesystems.MapUtils.writeFileSystems;
import static nl.esciencecenter.xenon.grpc.filesystems.MapUtils.writePermissions;
import static org.junit.Assert.assertEquals;

public class MapUtilsTest {
    @Test
    public void test_parsePermissions_allValid() throws StatusException {
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

        Set<PosixFilePermission> expected = new HashSet<>();
        expected.addAll(Arrays.asList(
                PosixFilePermission.OWNER_READ,
                PosixFilePermission.OWNER_WRITE,
                PosixFilePermission.OWNER_EXECUTE,
                PosixFilePermission.GROUP_READ,
                PosixFilePermission.GROUP_WRITE,
                PosixFilePermission.GROUP_EXECUTE,
                PosixFilePermission.OTHERS_READ,
                PosixFilePermission.OTHERS_WRITE,
                PosixFilePermission.OTHERS_EXECUTE
        ));
        assertEquals(response, expected);
    }

    @Test(expected = StatusException.class)
    public void test_parsePermissions_unrecognized_invalid() throws StatusException {
        List<XenonProto.PosixFilePermission> request = Collections.singletonList(
                XenonProto.PosixFilePermission.UNRECOGNIZED
        );

        parsePermissions(request);
    }

    @Test
    public void test_mapCopyMode_create() throws StatusException {
        CopyMode result = mapCopyMode(XenonProto.CopyMode.CREATE);

        assertEquals(CopyMode.CREATE, result);
    }

    @Test
    public void test_mapCopyMode_replace() throws StatusException {
        CopyMode result = mapCopyMode(XenonProto.CopyMode.REPLACE);

        assertEquals(CopyMode.REPLACE, result);
    }

    @Test
    public void test_mapCopyMode_ignore() throws StatusException {
        CopyMode result = mapCopyMode(XenonProto.CopyMode.IGNORE);

        assertEquals(CopyMode.IGNORE, result);
    }

    @Test(expected = StatusException.class)
    public void test_mapCopyMode_unrecognized_invalid() throws StatusException {
        mapCopyMode(XenonProto.CopyMode.UNRECOGNIZED);
    }

    @Test
    public void test_writePermissions_allValid() {
        Set<PosixFilePermission> request = new HashSet<>();
        request.addAll(Arrays.asList(
                PosixFilePermission.OWNER_READ,
                PosixFilePermission.OWNER_WRITE,
                PosixFilePermission.OWNER_EXECUTE,
                PosixFilePermission.GROUP_READ,
                PosixFilePermission.GROUP_WRITE,
                PosixFilePermission.GROUP_EXECUTE,
                PosixFilePermission.OTHERS_READ,
                PosixFilePermission.OTHERS_WRITE,
                PosixFilePermission.OTHERS_EXECUTE
        ));

        Set<XenonProto.PosixFilePermission> response = writePermissions(request);

        Set<XenonProto.PosixFilePermission> expected = new HashSet<>();
        expected.addAll(Arrays.asList(
                XenonProto.PosixFilePermission.OWNER_READ,
                XenonProto.PosixFilePermission.OWNER_WRITE,
                XenonProto.PosixFilePermission.OWNER_EXECUTE,
                XenonProto.PosixFilePermission.GROUP_READ,
                XenonProto.PosixFilePermission.GROUP_WRITE,
                XenonProto.PosixFilePermission.GROUP_EXECUTE,
                XenonProto.PosixFilePermission.OTHERS_READ,
                XenonProto.PosixFilePermission.OTHERS_WRITE,
                XenonProto.PosixFilePermission.OTHERS_EXECUTE
        ));
        assertEquals(expected, response);
    }

    @Test
    public void test_writeFileSystems() throws XenonException {
        String username = System.getProperty("user.name");
        FileSystem fs = FileSystem.create("file", "/");
        FileSystem[] request = new FileSystem[] { fs };

        XenonProto.FileSystems response = writeFileSystems(request);

        XenonProto.CreateFileSystemRequest pfsr = XenonProto.CreateFileSystemRequest.newBuilder()
                .setAdaptor("file")
                .setLocation("/")
                .build();
        XenonProto.FileSystem pfs = XenonProto.FileSystem.newBuilder()
                .setRequest(pfsr)
                .setId("file://" + username + "@/")
                .build();
        XenonProto.FileSystems expected = XenonProto.FileSystems.newBuilder()
                .addFilesystems(pfs)
                .build();
        try {
            assertEquals(expected, response);
        } finally {
            fs.close();
        }
    }
}
