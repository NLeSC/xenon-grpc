package nl.esciencecenter.xenon.grpc.filesystems;

import static java.util.UUID.randomUUID;
import static nl.esciencecenter.xenon.grpc.MapUtils.empty;
import static org.junit.Assert.assertEquals;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.adaptors.NotConnectedException;
import nl.esciencecenter.xenon.adaptors.filesystems.PathAttributesImplementation;
import nl.esciencecenter.xenon.filesystems.CopyCancelledException;
import nl.esciencecenter.xenon.filesystems.CopyMode;
import nl.esciencecenter.xenon.filesystems.CopyStatus;
import nl.esciencecenter.xenon.filesystems.FileSystem;
import nl.esciencecenter.xenon.filesystems.Path;
import nl.esciencecenter.xenon.filesystems.PathAttributes;
import nl.esciencecenter.xenon.filesystems.PosixFilePermission;
import nl.esciencecenter.xenon.grpc.XenonFileSystemsGrpc;
import nl.esciencecenter.xenon.grpc.XenonProto;

import com.google.protobuf.ByteString;
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

public class FileSystemsServiceBlockingTest {
    private Server server;
    private ManagedChannel channel;
    private XenonFileSystemsGrpc.XenonFileSystemsBlockingStub client;
    private FileSystem filesystem;
    private FileSystemsService service;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private XenonProto.FileSystem createFileSystem() {
        return XenonProto.FileSystem.newBuilder()
            .setId("file://someone@/")
            .build();
    }

    @Before
    public void setUp() throws IOException, StatusException {
        service = new FileSystemsService();
        // register mocked filesystem to service
        filesystem = mock(FileSystem.class);
        when(filesystem.getAdaptorName()).thenReturn("file");
        String root = File.listRoots()[0].getAbsolutePath();
        when(filesystem.getLocation()).thenReturn(root);
        service.putFileSystem(filesystem, "someone");
        // setup server
        String name = service.getClass().getName() + "-" + randomUUID().toString();
        server = InProcessServerBuilder.forName(name).directExecutor().addService(service).build();
        server.start();
        // setup client
        channel = InProcessChannelBuilder.forName(name).directExecutor().usePlaintext(true).build();
        client = XenonFileSystemsGrpc.newBlockingStub(channel);
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
        FileSystemsService service = new FileSystemsService();
        service.putFileSystem(filesystem, "someone");

        service.closeAllFileSystems();

        verify(filesystem).close();
    }

    @Test
    public void exists() throws XenonException {
        XenonProto.Path request = buildPath("/etc/passwd");
        when(filesystem.exists(new Path("/etc/passwd"))).thenReturn(true);

        XenonProto.Is response = client.exists(request);

        assertTrue(response.getValue());
    }

    @Test
    public void exists_notConnected() throws XenonException {
        thrown.expectMessage("UNAVAILABLE: sftp adaptor: Not connected");

        XenonProto.Path request = buildPath("/etc/passwd");
        when(filesystem.exists(new Path("/etc/passwd"))).thenThrow(new NotConnectedException("sftp", "Not connected"));

        client.exists(request);
    }

    @Test
    public void createDirectory() throws XenonException {
        XenonProto.Path request = buildPath("/somedir");

        client.createDirectory(request);

        verify(filesystem).createDirectory(new Path("/somedir"));
    }

    @Test
    public void createDirectory_notConnected() throws XenonException {
        thrown.expectMessage("UNAVAILABLE: sftp adaptor: Not connected");

        XenonProto.Path request = buildPath("/somedir");
        doThrow(new NotConnectedException("sftp", "Not connected")).when(filesystem).createDirectory(new Path("/somedir"));

        client.createDirectory(request);
    }

    @Test
    public void createDirectories() throws XenonException {
        XenonProto.Path request = buildPath("/somedir");

        client.createDirectories(request);

        verify(filesystem).createDirectories(new Path("/somedir"));
    }

    @Test
    public void createDirectories_notConnected() throws XenonException {
        thrown.expectMessage("UNAVAILABLE: sftp adaptor: Not connected");

        XenonProto.Path request = buildPath("/somedir");
        doThrow(new NotConnectedException("sftp", "Not connected")).when(filesystem).createDirectories(new Path("/somedir"));

        client.createDirectories(request);
    }

    @Test
    public void createFile() throws XenonException {
        XenonProto.Path request = buildPath("/somefile");

        client.createFile(request);

        verify(filesystem).createFile(new Path("/somefile"));
    }

    @Test
    public void createFile_notConnected() throws XenonException {
        thrown.expectMessage("UNAVAILABLE: sftp adaptor: Not connected");

        XenonProto.Path request = buildPath("/somefile");
        doThrow(new NotConnectedException("sftp", "Not connected")).when(filesystem).createFile(new Path("/somefile"));

        client.createFile(request);
    }

    @Test
    public void delete() throws XenonException {
        XenonProto.DeleteRequest request = XenonProto.DeleteRequest.newBuilder()
            .setPath(XenonProto.Path.newBuilder()
                .setFilesystem(createFileSystem())
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
            .setPath(XenonProto.Path.newBuilder()
                .setFilesystem(createFileSystem())
                .setPath("/somefile")
            )
            .build();
        doThrow(new NotConnectedException("sftp", "Not connected")).when(filesystem).delete(new Path("/somefile"), false);

        client.delete(request);
    }

    @Test
    public void delete_recursive() throws XenonException {
        XenonProto.DeleteRequest request = XenonProto.DeleteRequest.newBuilder()
            .setPath(XenonProto.Path.newBuilder()
                .setFilesystem(createFileSystem())
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
        XenonProto.Path request = buildPath(filename);
        PathAttributesImplementation attribs = buildPathAttributesOfRegularFile(filename);
        when(filesystem.getAttributes(new Path(filename))).thenReturn(attribs);

        XenonProto.PathAttributes response = client.getAttributes(request);

        XenonProto.PathAttributes expected = XenonProto.PathAttributes.newBuilder()
            .setPath(request)
            .setIsRegularFile(true)
            .build();
        assertEquals(expected, response);
    }

    @Test
    public void getAttributes_notConnected() throws XenonException {
        thrown.expectMessage("UNAVAILABLE: sftp adaptor: Not connected");

        String filename = "/etc/passwd";
        XenonProto.Path request = buildPath(filename);
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
        XenonProto.Path request = buildPath(path);
        when(filesystem.readSymbolicLink(new Path(path))).thenReturn(new Path("/run"));

        XenonProto.Path response = client.readSymbolicLink(request);

        XenonProto.Path expected = buildPath("/run");
        assertEquals(expected, response);
    }

    @Test
    public void readSymbolicLink_notConnected() throws XenonException {
        thrown.expectMessage("UNAVAILABLE: sftp adaptor: Not connected");

        String path = "/var/run";
        XenonProto.Path request = buildPath(path);
        when(filesystem.readSymbolicLink(new Path(path))).thenThrow(new NotConnectedException("sftp", "Not connected"));

        client.readSymbolicLink(request);
    }

    private XenonProto.Path buildPath(String path) {
        return XenonProto.Path.newBuilder()
            .setFilesystem(createFileSystem())
            .setPath(path)
            .build();
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
            .setSource("/var/run")
            .setTarget("/run")
            .build();

        client.rename(request);

        verify(filesystem).rename(new Path("/var/run"), new Path("/run"));
    }

    @Test
    public void rename_notConnected() throws XenonException {
        thrown.expectMessage("UNAVAILABLE: sftp adaptor: Not connected");

        XenonProto.RenameRequest request = XenonProto.RenameRequest.newBuilder()
            .setFilesystem(createFileSystem())
            .setSource("/var/run")
            .setTarget("/run")
            .build();
        doThrow(new NotConnectedException("sftp", "Not connected")).when(filesystem).rename(new Path("/var/run"), new Path("/run"));

        client.rename(request);
    }

    @Test
    public void readFromFile() throws XenonException {
        String path = "/etc/pasword";
        byte[] content = "test data".getBytes();
        XenonProto.Path request = buildPath(path);
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
            .setLink("/var/run")
            .setTarget("/run")
            .build();

        client.createSymbolicLink(request);

        verify(filesystem).createSymbolicLink(new Path("/var/run"), new Path("/run"));
    }

    @Test
    public void createSymbolicLink_notConnected() throws XenonException {
        thrown.expectMessage("UNAVAILABLE: sftp adaptor: Not connected");

        XenonProto.CreateSymbolicLinkRequest request = XenonProto.CreateSymbolicLinkRequest.newBuilder()
            .setFilesystem(createFileSystem())
            .setLink("/var/run")
            .setTarget("/run")
            .build();
        doThrow(new NotConnectedException("sftp", "Not connected")).when(filesystem).createSymbolicLink(new Path("/var/run"), new Path("/run"));

        client.createSymbolicLink(request);
    }

    @Test
    public void setPosixFilePermissions() throws XenonException {
        XenonProto.Path path = buildPath("/etc/passwd");
        XenonProto.SetPosixFilePermissionsRequest request = XenonProto.SetPosixFilePermissionsRequest.newBuilder()
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
            .setSource(buildPath(source))
            .setTarget(buildPath(target))
            .build();
        when(filesystem.copy(new Path(source), filesystem, new Path(target), CopyMode.CREATE, false)).thenReturn("COPY-1");

        XenonProto.CopyOperation response = client.copy(request);

        XenonProto.CopyOperation expected = XenonProto.CopyOperation.newBuilder()
            .setFilesystem(createFileSystem())
            .setId("COPY-1")
            .build();
        assertEquals(expected, response);
    }

    @Test
    public void copy_notConnected() throws XenonException {
        thrown.expectMessage("UNAVAILABLE: sftp adaptor: Not connected");

        String source = "/etc/passwd";
        String target = "/etc/passwd.bak";
        XenonProto.CopyRequest request = XenonProto.CopyRequest.newBuilder()
            .setSource(buildPath(source))
            .setTarget(buildPath(target))
            .build();
        when(filesystem.copy(new Path(source), filesystem, new Path(target), CopyMode.CREATE, false)).thenThrow(new NotConnectedException("sftp", "Not connected"));

        client.copy(request);
    }

    @Test
    public void cancel() throws XenonException {
        XenonProto.CopyOperation request = XenonProto.CopyOperation.newBuilder()
            .setFilesystem(createFileSystem())
            .setId("COPY-1")
            .build();
        CopyStatus status = buildCopyStatus();
        when(filesystem.cancel("COPY-1")).thenReturn(status);

        XenonProto.CopyStatus response = client.cancel(request);

        assertEquals("ID", "COPY-1", response.getCopyOperation().getId());
    }

    @Test
    public void cancel_notConnected() throws XenonException {
        thrown.expectMessage("UNAVAILABLE: sftp adaptor: Not connected");

        XenonProto.CopyOperation request = XenonProto.CopyOperation.newBuilder()
            .setFilesystem(createFileSystem())
            .setId("COPY-1")
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
        XenonProto.CopyOperation request = XenonProto.CopyOperation.newBuilder()
            .setFilesystem(createFileSystem())
            .setId("COPY-1")
            .build();
        CopyStatus status = buildCopyStatus();
        when(filesystem.getStatus("COPY-1")).thenReturn(status);

        XenonProto.CopyStatus response = client.getStatus(request);

        assertEquals("ID", "COPY-1", response.getCopyOperation().getId());
    }


    @Test
    public void getStatus_notConnected() throws XenonException {
        thrown.expectMessage("UNAVAILABLE: sftp adaptor: Not connected");

        XenonProto.CopyOperation request = XenonProto.CopyOperation.newBuilder()
            .setFilesystem(createFileSystem())
            .setId("COPY-1")
            .build();
        when(filesystem.getStatus("COPY-1")).thenThrow(new NotConnectedException("sftp", "Not connected"));

        client.getStatus(request);
    }

    @Test
    public void waitUntilDone() throws XenonException {
        XenonProto.CopyOperationWithTimeout request = XenonProto.CopyOperationWithTimeout.newBuilder()
            .setFilesystem(createFileSystem())
            .setId("COPY-1")
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

        XenonProto.CopyOperationWithTimeout request = XenonProto.CopyOperationWithTimeout.newBuilder()
            .setFilesystem(createFileSystem())
            .setId("COPY-1")
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
                .setIsRegularFile(true)
                .build(),
            XenonProto.PathAttributes.newBuilder()
                .setPath(buildPath("/etc/group"))
                .setIsRegularFile(true)
                .build()
        );
        assertEquals(expected, response);
    }

    @Test
    public void getEntryPath() {
        XenonProto.FileSystem request = createFileSystem();
        when(filesystem.getEntryPath()).thenReturn(new Path("/home/someone"));

        XenonProto.Path response = client.getEntryPath(request);

        XenonProto.Path expected = buildPath("/home/someone");
        assertEquals(expected, response);
    }

    @Test
    public void getEntryPath_unknownFilesystem() {
        thrown.expectMessage("NOT_FOUND: File system with id: sftp://someone@localhost/");

        XenonProto.FileSystem request = createUnknownFileSystem();

        client.getEntryPath(request);
    }

    @Test
    public void localFileSystems() {
        XenonProto.FileSystems response = client.localFileSystems(empty());

        String username = System.getProperty("user.name");
        XenonProto.FileSystem.Builder fsb = XenonProto.FileSystem.newBuilder();
        XenonProto.FileSystems.Builder fssb = XenonProto.FileSystems.newBuilder();
        for (File root : File.listRoots()) {
            fsb.setId("file://" + username + "@" + root.getAbsolutePath());
            fssb.addFilesystems(fsb.build());
        }
        XenonProto.FileSystems expected = fssb.build();
        assertEquals(expected, response);
    }

    @Test
    public void create() {
        XenonProto.CreateFileSystemRequest request = XenonProto.CreateFileSystemRequest.newBuilder()
            .setAdaptor("file")
            .setDefaultCred(XenonProto.DefaultCredential.newBuilder().setUsername("user1"))
            .build();

        XenonProto.FileSystem response = client.create(request);

        String fsId = "file://user1@";
        XenonProto.FileSystem expected = XenonProto.FileSystem.newBuilder()
            .setId(fsId)
            .build();
        assertEquals(expected, response);
        Stream<XenonProto.FileSystem> registeredFss = client.listFileSystems(empty()).getFilesystemsList().stream();
        assertTrue("Registered file system", registeredFss.anyMatch(c -> c.getId().equals(fsId)));
    }

    @Test
    public void create_again_alreadyExistError() {
        thrown.expectMessage("ALREADY_EXISTS: File system with id: file://user1@");

        XenonProto.CreateFileSystemRequest request = XenonProto.CreateFileSystemRequest.newBuilder()
            .setAdaptor("file")
            .setDefaultCred(XenonProto.DefaultCredential.newBuilder().setUsername("user1"))
            .build();

        client.create(request);

        client.create(request);
    }
}