package nl.esciencecenter.xenon.grpc.filesystems;

import static java.util.UUID.randomUUID;
import static nl.esciencecenter.xenon.grpc.MapUtils.empty;
import static nl.esciencecenter.xenon.utils.LocalFileSystemUtils.isWindows;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import com.google.protobuf.ByteString;
import com.google.protobuf.ProtocolStringList;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.MockitoAnnotations;

import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.adaptors.NotConnectedException;
import nl.esciencecenter.xenon.adaptors.filesystems.PathAttributesImplementation;
import nl.esciencecenter.xenon.filesystems.CopyCancelledException;
import nl.esciencecenter.xenon.filesystems.CopyMode;
import nl.esciencecenter.xenon.filesystems.CopyStatus;
import nl.esciencecenter.xenon.filesystems.FileSystem;
import nl.esciencecenter.xenon.filesystems.NoSuchPathException;
import nl.esciencecenter.xenon.filesystems.Path;
import nl.esciencecenter.xenon.filesystems.PathAttributes;
import nl.esciencecenter.xenon.filesystems.PosixFilePermission;
import nl.esciencecenter.xenon.grpc.FileSystemServiceGrpc;
import nl.esciencecenter.xenon.grpc.XenonProto;

public class FileSystemServiceBlockingTest {
    private Server server;
    private ManagedChannel channel;
    private FileSystemServiceGrpc.FileSystemServiceBlockingStub client;
    private FileSystem filesystem;
    private FileSystemService service;
    private String filesystemId;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private XenonProto.FileSystem createFileSystem() {
        return XenonProto.FileSystem.newBuilder()
            .setId(filesystemId)
            .build();
    }

    private XenonProto.PathRequest buildPathRequest(String path) {
        return XenonProto.PathRequest.newBuilder()
            .setFilesystem(createFileSystem())
            .setPath(buildPath(path))
            .build();
    }

    private XenonProto.Path buildPath(String path) {
        return XenonProto.Path.newBuilder()
            .setPath(path)
            .build();
    }

    @Before
    public void setUp() throws IOException, StatusException, XenonException {
        service = new FileSystemService();
        // register mocked filesystem to service
        filesystem = mock(FileSystem.class);
        when(filesystem.getAdaptorName()).thenReturn("file");
        when(filesystem.getLocation()).thenReturn("/");
        filesystemId = service.putFileSystem(filesystem, "someone");
        // setup server
        String name = service.getClass().getName() + "-" + randomUUID().toString();
        server = InProcessServerBuilder.forName(name).directExecutor().addService(service).build();
        server.start();
        // setup client
        channel = InProcessChannelBuilder.forName(name).directExecutor().usePlaintext(true).build();
        client = FileSystemServiceGrpc.newBlockingStub(channel);
        MockitoAnnotations.initMocks(this);
    }

    @After
    public void tearDown() throws XenonException {
        service.closeAllFileSystems();
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    public void listFileSystems_singleMockedFilesystem() {
        XenonProto.FileSystems response = client.listFileSystems(empty());

        XenonProto.FileSystems expected = XenonProto.FileSystems.newBuilder()
            .addFilesystems(createFileSystem())
            .build();
        assertEquals(expected, response);
    }

    @Test
    public void close() throws XenonException {
        XenonProto.FileSystem request = createFileSystem();

        client.close(request);

        verify(filesystem).close();
        XenonProto.FileSystems result = client.listFileSystems(empty());
        assertEquals("No filesystems registered", 0, result.getFilesystemsCount());
    }

    @Test
    public void close_unknownFilesystem() {
        thrown.expectMessage("NOT_FOUND: File system with id: sftp://someone@localhost/");

        XenonProto.FileSystem request = createUnknownFileSystem();

        client.close(request);
    }

    private XenonProto.FileSystem createUnknownFileSystem() {
        return XenonProto.FileSystem.newBuilder()
            .setId("sftp://someone@localhost/")
            .build();
    }

    @Test
    public void closeAllFileSystems() throws XenonException, StatusException {
        FileSystemService service = new FileSystemService();
        service.putFileSystem(filesystem, "someone");

        service.closeAllFileSystems();

        verify(filesystem).close();
    }

    @Test
    public void exists() throws XenonException {
        XenonProto.PathRequest request = buildPathRequest("/etc/passwd");
        when(filesystem.exists(new Path("/etc/passwd"))).thenReturn(true);

        XenonProto.Is response = client.exists(request);

        assertTrue(response.getValue());
    }


    @Test
    public void exists_unixSeparator() throws XenonException {
        XenonProto.PathRequest request = XenonProto.PathRequest.newBuilder()
                .setFilesystem(createFileSystem())
                .setPath(XenonProto.Path.newBuilder().setPath("/etc/passwd").setSeparator("/"))
                .build();
        when(filesystem.exists(new Path('/', "/etc/passwd"))).thenReturn(true);

        XenonProto.Is response = client.exists(request);

        assertTrue(response.getValue());
    }

    @Test
    public void exists_customSeparator() throws XenonException {
        XenonProto.PathRequest request = XenonProto.PathRequest.newBuilder()
                .setFilesystem(createFileSystem())
                .setPath(XenonProto.Path.newBuilder().setPath("@etc@passwd").setSeparator("@"))
                .build();
        when(filesystem.exists(new Path('@', "@etc@passwd"))).thenReturn(true);

        XenonProto.Is response = client.exists(request);

        assertTrue(response.getValue());
    }

    @Test
    public void exists_windowsSeparator() throws XenonException {
        XenonProto.PathRequest request = XenonProto.PathRequest.newBuilder()
                .setFilesystem(createFileSystem())
                .setPath(XenonProto.Path.newBuilder().setPath("\\etc\\passwd").setSeparator("\\\\"))
                .build();
        when(filesystem.exists(new Path('\\', "\\etc\\passwd"))).thenReturn(true);

        XenonProto.Is response = client.exists(request);

        assertTrue(response.getValue());
    }

    @Test
    public void exists_notConnected() throws XenonException {
        thrown.expectMessage("UNAVAILABLE: sftp adaptor: Not connected");

        XenonProto.PathRequest request = buildPathRequest("/etc/passwd");
        when(filesystem.exists(new Path("/etc/passwd"))).thenThrow(new NotConnectedException("sftp", "Not connected"));

        client.exists(request);
    }

    @Test
    public void createDirectory() throws XenonException {
        XenonProto.PathRequest request = buildPathRequest("/somedir");

        client.createDirectory(request);

        verify(filesystem).createDirectory(new Path("/somedir"));
    }

    @Test
    public void createDirectory_notConnected() throws XenonException {
        thrown.expectMessage("UNAVAILABLE: sftp adaptor: Not connected");

        XenonProto.PathRequest request = buildPathRequest("/somedir");
        doThrow(new NotConnectedException("sftp", "Not connected")).when(filesystem).createDirectory(new Path("/somedir"));

        client.createDirectory(request);
    }

    @Test
    public void createDirectories() throws XenonException {
        XenonProto.PathRequest request = buildPathRequest("/somedir");

        client.createDirectories(request);

        verify(filesystem).createDirectories(new Path("/somedir"));
    }

    @Test
    public void createDirectories_notConnected() throws XenonException {
        thrown.expectMessage("UNAVAILABLE: sftp adaptor: Not connected");

        XenonProto.PathRequest request = buildPathRequest("/somedir");
        doThrow(new NotConnectedException("sftp", "Not connected")).when(filesystem).createDirectories(new Path("/somedir"));

        client.createDirectories(request);
    }

    @Test
    public void createFile() throws XenonException {
        XenonProto.PathRequest request = buildPathRequest("/somefile");

        client.createFile(request);

        verify(filesystem).createFile(new Path("/somefile"));
    }

    @Test
    public void createFile_notConnected() throws XenonException {
        thrown.expectMessage("UNAVAILABLE: sftp adaptor: Not connected");

        XenonProto.PathRequest request = buildPathRequest("/somefile");
        doThrow(new NotConnectedException("sftp", "Not connected")).when(filesystem).createFile(new Path("/somefile"));

        client.createFile(request);
    }

    @Test
    public void delete() throws XenonException {
        XenonProto.DeleteRequest request = XenonProto.DeleteRequest.newBuilder()
            .setFilesystem(createFileSystem())
            .setPath(XenonProto.Path.newBuilder()
                .setPath("/somefile")
            )
            .build();

        client.delete(request);

        verify(filesystem).delete(new Path("/somefile"), false);
    }

    @Test
    public void delete_notConnected() throws XenonException {
        thrown.expectMessage("UNAVAILABLE: sftp adaptor: Not connected");

        XenonProto.DeleteRequest request = XenonProto.DeleteRequest.newBuilder()
            .setFilesystem(createFileSystem())
            .setPath(XenonProto.Path.newBuilder()
                .setPath("/somefile")
            )
            .build();
        doThrow(new NotConnectedException("sftp", "Not connected")).when(filesystem).delete(new Path("/somefile"), false);

        client.delete(request);
    }

    @Test
    public void delete_recursive() throws XenonException {
        XenonProto.DeleteRequest request = XenonProto.DeleteRequest.newBuilder()
            .setFilesystem(createFileSystem())
            .setPath(XenonProto.Path.newBuilder()
                .setPath("/somefile")
            )
            .setRecursive(true)
            .build();

        client.delete(request);

        verify(filesystem).delete(new Path("/somefile"), true);
    }

    @Test
    public void getAttributes() throws XenonException {
        String filename = "/etc/passwd";
        XenonProto.PathRequest request = buildPathRequest(filename);
        PathAttributesImplementation attribs = buildPathAttributesOfRegularFile(filename);
        when(filesystem.getAttributes(new Path(filename))).thenReturn(attribs);

        XenonProto.PathAttributes response = client.getAttributes(request);

        XenonProto.PathAttributes expected = XenonProto.PathAttributes.newBuilder()
            .setPath(buildPath(filename))
            .setIsRegular(true)
            .build();
        assertEquals(expected, response);
    }

    @Test
    public void getAttributes_notConnected() throws XenonException {
        thrown.expectMessage("UNAVAILABLE: sftp adaptor: Not connected");

        String filename = "/etc/passwd";
        XenonProto.PathRequest request = buildPathRequest(filename);
        when(filesystem.getAttributes(new Path(filename))).thenThrow(new NotConnectedException("sftp", "Not connected"));

        client.getAttributes(request);
    }

    private PathAttributesImplementation buildPathAttributesOfRegularFile(String filename) {
        PathAttributesImplementation attribs = new PathAttributesImplementation();
        attribs.setPath(new Path(filename));
        attribs.setRegular(true);
        return attribs;
    }

    @Test
    public void readSymbolicLink() throws XenonException {
        String path = "/var/run";
        XenonProto.PathRequest request = buildPathRequest(path);
        when(filesystem.readSymbolicLink(new Path(path))).thenReturn(new Path("/run"));

        XenonProto.Path response = client.readSymbolicLink(request);

        XenonProto.Path expected = buildPath("/run");
        assertEquals(expected, response);
    }

    @Test
    public void readSymbolicLink_notConnected() throws XenonException {
        thrown.expectMessage("UNAVAILABLE: sftp adaptor: Not connected");

        String path = "/var/run";
        XenonProto.PathRequest request = buildPathRequest(path);
        when(filesystem.readSymbolicLink(new Path(path))).thenThrow(new NotConnectedException("sftp", "Not connected"));

        client.readSymbolicLink(request);
    }


    @Test
    public void isOpen() throws XenonException {
        XenonProto.FileSystem request = createFileSystem();
        when(filesystem.isOpen()).thenReturn(true);

        XenonProto.Is response = client.isOpen(request);

        assertTrue(response.getValue());
    }

    @Test
    public void isOpen_exception() throws XenonException {
        thrown.expectMessage("INTERNAL: sftp adaptor: Something bad");

        XenonProto.FileSystem request = createFileSystem();
        when(filesystem.isOpen()).thenThrow(new XenonException("sftp", "Something bad"));

        client.isOpen(request);
    }

    @Test
    public void rename() throws XenonException {
        XenonProto.RenameRequest request = XenonProto.RenameRequest.newBuilder()
            .setFilesystem(createFileSystem())
            .setSource(buildPath("/var/run"))
            .setTarget(buildPath("/run"))
            .build();

        client.rename(request);

        verify(filesystem).rename(new Path("/var/run"), new Path("/run"));
    }

    @Test
    public void rename_notConnected() throws XenonException {
        thrown.expectMessage("UNAVAILABLE: sftp adaptor: Not connected");

        XenonProto.RenameRequest request = XenonProto.RenameRequest.newBuilder()
            .setFilesystem(createFileSystem())
            .setSource(buildPath("/var/run"))
            .setTarget(buildPath("/run"))
            .build();
        doThrow(new NotConnectedException("sftp", "Not connected")).when(filesystem).rename(new Path("/var/run"), new Path("/run"));

        client.rename(request);
    }

    @Test
    public void readFromFile() throws XenonException {
        String path = "/etc/pasword";
        byte[] content = "test data".getBytes();
        XenonProto.PathRequest request = buildPathRequest(path);
        InputStream stream = new ByteArrayInputStream(content);
        when(filesystem.readFromFile(new Path(path))).thenReturn(stream);

        Iterator<XenonProto.ReadFromFileResponse> iterator = client.readFromFile(request);

        List<XenonProto.ReadFromFileResponse> response = new ArrayList<>();
        iterator.forEachRemaining(response::add);

        List<XenonProto.ReadFromFileResponse> expected = Arrays.asList(
            XenonProto.ReadFromFileResponse.newBuilder()
                .setBuffer(ByteString.copyFrom(content))
                .build(),
            XenonProto.ReadFromFileResponse.getDefaultInstance()
        );
        assertEquals(expected, response);
    }

    @Test
    public void createSymbolicLink() throws XenonException {
        XenonProto.CreateSymbolicLinkRequest request = XenonProto.CreateSymbolicLinkRequest.newBuilder()
            .setFilesystem(createFileSystem())
            .setLink(buildPath("/var/run"))
            .setTarget(buildPath("/run"))
            .build();

        client.createSymbolicLink(request);

        verify(filesystem).createSymbolicLink(new Path("/var/run"), new Path("/run"));
    }

    @Test
    public void createSymbolicLink_notConnected() throws XenonException {
        thrown.expectMessage("UNAVAILABLE: sftp adaptor: Not connected");

        XenonProto.CreateSymbolicLinkRequest request = XenonProto.CreateSymbolicLinkRequest.newBuilder()
            .setFilesystem(createFileSystem())
            .setLink(buildPath("/var/run"))
            .setTarget(buildPath("/run"))
            .build();
        doThrow(new NotConnectedException("sftp", "Not connected")).when(filesystem).createSymbolicLink(new Path("/var/run"), new Path("/run"));

        client.createSymbolicLink(request);
    }

    @Test
    public void setPosixFilePermissions() throws XenonException {
        XenonProto.Path path = buildPath("/etc/passwd");
        XenonProto.SetPosixFilePermissionsRequest request = XenonProto.SetPosixFilePermissionsRequest.newBuilder()
            .setFilesystem(createFileSystem())
            .setPath(path)
            .addPermissions(XenonProto.PosixFilePermission.GROUP_EXECUTE)
            .build();

        client.setPosixFilePermissions(request);

        Set<PosixFilePermission> expected = new HashSet<>();
        expected.add(PosixFilePermission.GROUP_EXECUTE);
        verify(filesystem).setPosixFilePermissions(new Path("/etc/passwd"), expected);
    }

    @Test
    public void setPosixFilePermissions_notConnected() throws XenonException {
        thrown.expectMessage("UNAVAILABLE: sftp adaptor: Not connected");

        XenonProto.Path path = buildPath("/etc/passwd");
        XenonProto.SetPosixFilePermissionsRequest request = XenonProto.SetPosixFilePermissionsRequest.newBuilder()
            .setFilesystem(createFileSystem())
            .setPath(path)
            .addPermissions(XenonProto.PosixFilePermission.GROUP_EXECUTE)
            .build();
        Set<PosixFilePermission> expected = new HashSet<>();
        expected.add(PosixFilePermission.GROUP_EXECUTE);
        doThrow(new NotConnectedException("sftp", "Not connected")).when(filesystem).setPosixFilePermissions(new Path("/etc/passwd"), expected);

        client.setPosixFilePermissions(request);
    }

    @Test
    public void copy() throws XenonException {
        String source = "/etc/passwd";
        String target = "/etc/passwd.bak";
        XenonProto.CopyRequest request = XenonProto.CopyRequest.newBuilder()
            .setFilesystem(createFileSystem())
            .setSource(buildPath(source))
            .setDestinationFilesystem(createFileSystem())
            .setDestination(buildPath(target))
            .build();
        when(filesystem.copy(new Path(source), filesystem, new Path(target), CopyMode.CREATE, false)).thenReturn("COPY-1");

        XenonProto.CopyOperation response = client.copy(request);

        XenonProto.CopyOperation expected = XenonProto.CopyOperation.newBuilder()
            .setId("COPY-1")
            .build();
        assertEquals(expected, response);
    }

    @Test
    public void copy_illegalarg() throws XenonException {
        thrown.expectMessage("INVALID_ARGUMENT: Copy mode is invalid");

        String source = "/etc/passwd";
        String target = "/etc/passwd.bak";
        XenonProto.CopyRequest request = XenonProto.CopyRequest.newBuilder()
            .setFilesystem(createFileSystem())
            .setSource(buildPath(source))
            .setDestinationFilesystem(createFileSystem())
            .setDestination(buildPath(target))
            .build();
        when(filesystem.copy(new Path(source), filesystem, new Path(target), CopyMode.CREATE, false)).thenThrow(new IllegalArgumentException("Copy mode is invalid"));

        client.copy(request);
    }

    @Test
    public void cancel() throws XenonException {
        XenonProto.CopyOperationRequest request = XenonProto.CopyOperationRequest.newBuilder()
            .setFilesystem(createFileSystem())
            .setCopyOperation(XenonProto.CopyOperation.newBuilder().setId("COPY-1"))
            .build();
        CopyStatus status = buildCopyStatus();
        when(filesystem.cancel("COPY-1")).thenReturn(status);

        XenonProto.CopyStatus response = client.cancel(request);

        assertEquals("ID", "COPY-1", response.getCopyOperation().getId());
    }

    @Test
    public void cancel_notConnected() throws XenonException {
        thrown.expectMessage("UNAVAILABLE: sftp adaptor: Not connected");

        XenonProto.CopyOperationRequest request = XenonProto.CopyOperationRequest.newBuilder()
            .setFilesystem(createFileSystem())
            .setCopyOperation(XenonProto.CopyOperation.newBuilder().setId("COPY-1"))
            .build();
        when(filesystem.cancel("COPY-1")).thenThrow(new NotConnectedException("sftp", "Not connected"));

        client.cancel(request);
    }

    private CopyStatus buildCopyStatus() {
        CopyStatus status = mock(CopyStatus.class);
        when(status.getState()).thenReturn("COMPLETED");
        when(status.isDone()).thenReturn(true);
        when(status.isRunning()).thenReturn(false);
        when(status.bytesCopied()).thenReturn(1024L);
        when(status.bytesToCopy()).thenReturn(1024L);
        when(status.getCopyIdentifier()).thenReturn("COPY-1");
        when(status.hasException()).thenReturn(true);
        when(status.getException()).thenReturn(new CopyCancelledException("file", "Copy cancelled"));
        return status;
    }

    @Test
    public void getStatus() throws XenonException {
        XenonProto.CopyOperationRequest request = XenonProto.CopyOperationRequest.newBuilder()
            .setFilesystem(createFileSystem())
            .setCopyOperation(XenonProto.CopyOperation.newBuilder().setId("COPY-1"))
            .build();
        CopyStatus status = buildCopyStatus();
        when(filesystem.getStatus("COPY-1")).thenReturn(status);

        XenonProto.CopyStatus response = client.getStatus(request);

        assertEquals("ID", "COPY-1", response.getCopyOperation().getId());
    }


    @Test
    public void getStatus_notConnected() throws XenonException {
        thrown.expectMessage("UNAVAILABLE: sftp adaptor: Not connected");

        XenonProto.CopyOperationRequest request = XenonProto.CopyOperationRequest.newBuilder()
            .setFilesystem(createFileSystem())
            .setCopyOperation(XenonProto.CopyOperation.newBuilder().setId("COPY-1"))
            .build();
        when(filesystem.getStatus("COPY-1")).thenThrow(new NotConnectedException("sftp", "Not connected"));

        client.getStatus(request);
    }

    @Test
    public void waitUntilDone() throws XenonException {
        XenonProto.WaitUntilDoneRequest request = XenonProto.WaitUntilDoneRequest.newBuilder()
            .setFilesystem(createFileSystem())
            .setCopyOperation(XenonProto.CopyOperation.newBuilder().setId("COPY-1"))
            .setTimeout(1024L)
            .build();
        CopyStatus status = buildCopyStatus();
        when(filesystem.waitUntilDone("COPY-1", 1024L)).thenReturn(status);

        XenonProto.CopyStatus response = client.waitUntilDone(request);

        assertEquals("ID", "COPY-1", response.getCopyOperation().getId());
    }

    @Test
    public void waitUntilDone_notConnected() throws XenonException {
        thrown.expectMessage("UNAVAILABLE: sftp adaptor: Not connected");

        XenonProto.WaitUntilDoneRequest request = XenonProto.WaitUntilDoneRequest.newBuilder()
            .setFilesystem(createFileSystem())
            .setCopyOperation(XenonProto.CopyOperation.newBuilder().setId("COPY-1"))
            .setTimeout(1024L)
            .build();
        when(filesystem.waitUntilDone("COPY-1", 1024L)).thenThrow(new NotConnectedException("sftp", "Not connected"));

        client.waitUntilDone(request);
    }

    @Test
    public void getAdaptorDescription() throws XenonException {
        XenonProto.AdaptorName request = XenonProto.AdaptorName.newBuilder()
            .setName("file")
            .build();

        XenonProto.FileSystemAdaptorDescription response = client.getAdaptorDescription(request);

        assertEquals("file", response.getName());
    }

    @Test
    public void getAdaptorDescription_unknown() throws XenonException {
        thrown.expectMessage("NOT_FOUND: FileSystem adaptor: Adaptor 'bigstore' not found");

        XenonProto.AdaptorName request = XenonProto.AdaptorName.newBuilder()
            .setName("bigstore")
            .build();

        client.getAdaptorDescription(request);
    }

    @Test
    public void getAdaptorDescriptions() throws XenonException {
        XenonProto.FileSystemAdaptorDescriptions response = client.getAdaptorDescriptions(empty());

        assertTrue("Some descriptions", response.getDescriptionsCount() > 0);
    }

    @Test
    public void list() throws XenonException {
        XenonProto.ListRequest request = XenonProto.ListRequest.newBuilder()
            .setFilesystem(createFileSystem())
            .setDir(buildPath("/etc"))
            .build();
        Iterable<PathAttributes> listing = Arrays.asList(
            buildPathAttributesOfRegularFile("/etc/passwd"),
            buildPathAttributesOfRegularFile("/etc/group")
        );
        when(filesystem.list(new Path("/etc"), false)).thenReturn(listing);

        Iterator<XenonProto.PathAttributes> iterator = client.list(request);

        List<XenonProto.PathAttributes> response = new ArrayList<>();
        iterator.forEachRemaining(response::add);

        List<XenonProto.PathAttributes> expected = Arrays.asList(
            XenonProto.PathAttributes.newBuilder()
                .setPath(buildPath("/etc/passwd"))
                .setIsRegular(true)
                .build(),
            XenonProto.PathAttributes.newBuilder()
                .setPath(buildPath("/etc/group"))
                .setIsRegular(true)
                .build()
        );
        assertEquals(expected, response);
    }

    @Test
    public void getWorkingDirectory() {
        XenonProto.FileSystem request = createFileSystem();
        when(filesystem.getWorkingDirectory()).thenReturn(new Path("/home/someone"));

        XenonProto.Path response = client.getWorkingDirectory(request);

        XenonProto.Path expected = buildPath("/home/someone");
        assertEquals(expected, response);
    }

    @Test
    public void getWorkingDirectory_unknownFilesystem() {
        thrown.expectMessage("NOT_FOUND: File system with id: sftp://someone@localhost/");

        XenonProto.FileSystem request = createUnknownFileSystem();

        client.getWorkingDirectory(request);
    }

    @Test
    public void setWorkingDirectory() throws XenonException {
        XenonProto.PathRequest request = buildPathRequest("/tmp");

        client.setWorkingDirectory(request);

        verify(filesystem).setWorkingDirectory(new Path("/tmp"));
    }

    @Test
    public void setWorkingDirectory_unknownFilesystem() {
        thrown.expectMessage("NOT_FOUND: File system with id: sftp://someone@localhost/");

        XenonProto.FileSystem fs = createUnknownFileSystem();
        XenonProto.PathRequest request = XenonProto.PathRequest.newBuilder()
                .setFilesystem(fs)
                .setPath(XenonProto.Path.newBuilder().setPath("/tmp"))
                .build();

        client.setWorkingDirectory(request);
    }

    @Test
    public void localFileSystems() {
        XenonProto.FileSystems response = client.localFileSystems(empty());

        String username = System.getProperty("user.name");
        int i = 0;
        for (File root : File.listRoots()) {
            String xroot = root.getAbsolutePath();
            if (isWindows()) {
                xroot = xroot.substring(0, 2);
            }
            assertTrue(response.getFilesystems(i).getId().startsWith("file://" + username + "@" + xroot + "#"));
            i++;
        }
        assertEquals(i, response.getFilesystemsCount());
    }

    @Test
    public void create() {
        XenonProto.CreateFileSystemRequest request = XenonProto.CreateFileSystemRequest.newBuilder()
            .setAdaptor("file")
            .setDefaultCredential(XenonProto.DefaultCredential.newBuilder().setUsername("user1"))
            .build();

        XenonProto.FileSystem response = client.create(request);

        String expectedFilesystemId = "file://user1@";
        assertTrue("Received an id", response.getId().startsWith(expectedFilesystemId));
        Stream<XenonProto.FileSystem> registeredFss = client.listFileSystems(empty()).getFilesystemsList().stream();
        assertTrue("Registered file system", registeredFss.anyMatch(c -> c.getId().startsWith(expectedFilesystemId)));
    }

    @Test
    public void create_twiceSameRequest_shouldCreate2Filesystems() {
        XenonProto.CreateFileSystemRequest request = XenonProto.CreateFileSystemRequest.newBuilder()
            .setAdaptor("file")
            .setDefaultCredential(XenonProto.DefaultCredential.newBuilder().setUsername("user1"))
            .build();
        XenonProto.FileSystem response1 = client.create(request);

        XenonProto.FileSystem response2 = client.create(request);

        assertNotEquals(response1, response2);
    }

    @Test
    public void getAdaptorNames() {
        XenonProto.AdaptorNames response = client.getAdaptorNames(empty());

        ProtocolStringList names = response.getNameList();
        List<String> expectedNames = Arrays.asList("file", "sftp");
        assertTrue("Contains file and sftp", names.containsAll(expectedNames));
    }

    @Test
    public void getAdaptorName() {
        XenonProto.AdaptorName response = client.getAdaptorName(createFileSystem());

        String expected = "file";
        assertEquals(expected, response.getName());
    }

    @Test
    public void getAdaptorName_unknownFileSystem() {
        thrown.expectMessage("NOT_FOUND: File system with id: sftp://someone@localhost");

        XenonProto.FileSystem request = XenonProto.FileSystem.newBuilder()
            .setId("sftp://someone@localhost")
            .build();

        client.getAdaptorName(request);
    }
    @Test
    public void getLocation() {
        XenonProto.Location response = client.getLocation(createFileSystem());

        String expected = "/";
        assertEquals(expected, response.getLocation());
    }

    @Test
    public void getLocation_unknownScheduler() {
        thrown.expectMessage("NOT_FOUND: File system with id: sftp://someone@localhost");

        XenonProto.FileSystem request = XenonProto.FileSystem.newBuilder()
            .setId("sftp://someone@localhost")
            .build();

        client.getLocation(request);
    }

    @Test
    public void getProperties() {
        XenonProto.Properties response = client.getProperties(createFileSystem());

        Map<String, String> expected = new HashMap<>();
        assertEquals(expected, response.getPropertiesMap());
    }

    @Test
    public void getProperties_unknownScheduler() {
        thrown.expectMessage("NOT_FOUND: File system with id: sftp://someone@localhost");

        XenonProto.FileSystem request = XenonProto.FileSystem.newBuilder()
            .setId("sftp://someone@localhost")
            .build();

        client.getProperties(request);
    }

}