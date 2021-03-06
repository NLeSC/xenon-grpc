package nl.esciencecenter.xenon.grpc.filesystems;

import static nl.esciencecenter.xenon.grpc.filesystems.MapUtils.mapCopyMode;
import static nl.esciencecenter.xenon.grpc.filesystems.MapUtils.mapCopyStatusErrorType;
import static nl.esciencecenter.xenon.grpc.filesystems.MapUtils.parsePermissions;
import static nl.esciencecenter.xenon.grpc.filesystems.MapUtils.writeFileAttributes;
import static nl.esciencecenter.xenon.grpc.filesystems.MapUtils.writeFileSystems;
import static nl.esciencecenter.xenon.grpc.filesystems.MapUtils.writePermissions;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.grpc.StatusException;
import org.junit.Test;

import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.adaptors.NotConnectedException;
import nl.esciencecenter.xenon.adaptors.filesystems.PathAttributesImplementation;
import nl.esciencecenter.xenon.filesystems.CopyCancelledException;
import nl.esciencecenter.xenon.filesystems.CopyMode;
import nl.esciencecenter.xenon.filesystems.FileSystem;
import nl.esciencecenter.xenon.filesystems.NoSuchPathException;
import nl.esciencecenter.xenon.filesystems.Path;
import nl.esciencecenter.xenon.filesystems.PathAlreadyExistsException;
import nl.esciencecenter.xenon.filesystems.PosixFilePermission;
import nl.esciencecenter.xenon.grpc.XenonProto;

public class MapUtilsTest {
    @Test
    public void test_parsePermissions_default() throws StatusException {
        List<XenonProto.PosixFilePermission> request = Collections.singletonList(
            XenonProto.PosixFilePermission.NONE
        );

        Set<PosixFilePermission> response = parsePermissions(request);

        Set<PosixFilePermission> expected = new HashSet<>();
        assertEquals(response, expected);
    }

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
        CopyMode result = mapCopyMode(XenonProto.CopyRequest.CopyMode.CREATE);

        assertEquals(CopyMode.CREATE, result);
    }

    @Test
    public void test_mapCopyMode_replace() throws StatusException {
        CopyMode result = mapCopyMode(XenonProto.CopyRequest.CopyMode.REPLACE);

        assertEquals(CopyMode.REPLACE, result);
    }

    @Test
    public void test_mapCopyMode_ignore() throws StatusException {
        CopyMode result = mapCopyMode(XenonProto.CopyRequest.CopyMode.IGNORE);

        assertEquals(CopyMode.IGNORE, result);
    }

    @Test(expected = StatusException.class)
    public void test_mapCopyMode_unrecognized_invalid() throws StatusException {
        mapCopyMode(XenonProto.CopyRequest.CopyMode.UNRECOGNIZED);
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
        FileSystem fs = FileSystem.create("file");
        FileSystem[] request = new FileSystem[] { fs };

        XenonProto.FileSystems response = writeFileSystems(request);

        XenonProto.FileSystem pfs = createFileSystem(username, String.valueOf(fs.hashCode()));
        XenonProto.FileSystems expected = XenonProto.FileSystems.newBuilder()
                .addFilesystems(pfs)
                .build();
        try {
            assertEquals(expected, response);
        } finally {
            fs.close();
        }
    }

    private XenonProto.FileSystem createFileSystem(String username, String uniqueFsId) {
        String root = getWorkingDirectory();
        return XenonProto.FileSystem.newBuilder()
            .setId("file://" + username + "@" + root + "#" + uniqueFsId)
            .build();
    }

    private static String getWorkingDirectory() {
        return System.getProperty("user.dir");
    }

    @Test
    public void test_writeFileAttributes_minimal() {

        PathAttributesImplementation attribs = new PathAttributesImplementation();
        attribs.setPath(new Path("/somefile"));
        attribs.setCreationTime(1L);
        attribs.setReadable(true);
        attribs.setRegular(true);
        attribs.setLastAccessTime(2L);
        attribs.setLastModifiedTime(3L);
        attribs.setSize(4L);

        XenonProto.PathAttributes response = writeFileAttributes(attribs);

        XenonProto.PathAttributes expected = XenonProto.PathAttributes.newBuilder()
                .setPath(XenonProto.Path.newBuilder()
                        .setPath("/somefile")
                )
                .setCreationTime(1L)
                .setIsReadable(true)
                .setIsRegular(true)
                .setIsReadable(true)
                .setLastAccessTime(2L)
                .setLastModifiedTime(3L)
                .setSize(4L)
                .build();
        assertEquals(expected, response);
    }

    @Test
    public void test_writeFileAttributes_complete() {

        PathAttributesImplementation attribs = new PathAttributesImplementation();
        attribs.setPath(new Path("/somefile"));
        attribs.setCreationTime(1L);
        attribs.setReadable(true);
        attribs.setRegular(true);
        attribs.setLastAccessTime(2L);
        attribs.setLastModifiedTime(3L);
        attribs.setSize(4L);
        Set<PosixFilePermission> perms = new HashSet<>();
        perms.add(PosixFilePermission.OWNER_READ);
        attribs.setPermissions(perms);
        attribs.setOwner("someuser");
        attribs.setGroup("users");

        XenonProto.PathAttributes response = writeFileAttributes(attribs);

        XenonProto.PathAttributes expected = XenonProto.PathAttributes.newBuilder()
                .setPath(XenonProto.Path.newBuilder()
                        .setPath("/somefile")
                )
                .setCreationTime(1L)
                .setIsReadable(true)
                .setIsRegular(true)
                .setIsReadable(true)
                .setLastAccessTime(2L)
                .setLastModifiedTime(3L)
                .setSize(4L)
                .addPermissions(XenonProto.PosixFilePermission.OWNER_READ)
                .setOwner("someuser")
                .setGroup("users")
                .build();
        assertEquals(expected, response);
    }

    @Test
    public void test_mapCopyStatusErrorType_NoSuchPathException() {
        XenonProto.CopyStatus.ErrorType result = mapCopyStatusErrorType(new NoSuchPathException("file", "No such path"));

        XenonProto.CopyStatus.ErrorType expected = XenonProto.CopyStatus.ErrorType.NOT_FOUND;
        assertEquals(expected, result);
    }

    @Test
    public void test_mapCopyStatusErrorType_CopyCancelledException() {
        XenonProto.CopyStatus.ErrorType result = mapCopyStatusErrorType(new CopyCancelledException("file", "Copy cancelled"));

        XenonProto.CopyStatus.ErrorType expected = XenonProto.CopyStatus.ErrorType.CANCELLED;
        assertEquals(expected, result);
    }

    @Test
    public void test_mapCopyStatusErrorType_PathAlreadyExistsException() {
        XenonProto.CopyStatus.ErrorType result = mapCopyStatusErrorType(new PathAlreadyExistsException("file", "Path already exists"));

        XenonProto.CopyStatus.ErrorType expected = XenonProto.CopyStatus.ErrorType.ALREADY_EXISTS;
        assertEquals(expected, result);
    }

    @Test
    public void test_mapCopyStatusErrorType_NotConnectedException() {
        XenonProto.CopyStatus.ErrorType result = mapCopyStatusErrorType(new NotConnectedException("sftp", "Not connected"));

        XenonProto.CopyStatus.ErrorType expected = XenonProto.CopyStatus.ErrorType.NOT_CONNECTED;
        assertEquals(expected, result);
    }

    @Test
    public void test_mapCopyStatusErrorType_XenonException() {
        XenonProto.CopyStatus.ErrorType result = mapCopyStatusErrorType(new XenonException("sftp", "Something bad"));

        XenonProto.CopyStatus.ErrorType expected = XenonProto.CopyStatus.ErrorType.XENON;
        assertEquals(expected, result);
    }
}
