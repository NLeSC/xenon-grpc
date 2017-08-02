package nl.esciencecenter.xenon.grpc.filesystems;

import static java.util.UUID.randomUUID;
import static nl.esciencecenter.xenon.grpc.MapUtils.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.adaptors.filesystems.PathAttributesImplementation;
import nl.esciencecenter.xenon.filesystems.CopyMode;
import nl.esciencecenter.xenon.filesystems.FileSystem;
import nl.esciencecenter.xenon.filesystems.Path;
import nl.esciencecenter.xenon.filesystems.PosixFilePermission;
import nl.esciencecenter.xenon.grpc.XenonFileSystemsGrpc;
import nl.esciencecenter.xenon.grpc.XenonProto;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageOrBuilder;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FileSystemsServiceTest {

    private Server server;
    private ManagedChannel channel;
    private XenonFileSystemsGrpc.XenonFileSystemsBlockingStub client;
    private FileSystem filesystem;

    private XenonProto.CreateFileSystemRequest createFileSystemRequest() {
       return XenonProto.CreateFileSystemRequest.newBuilder()
               .setAdaptor("file")
               .setDefaultCred(XenonProto.DefaultCredential.newBuilder().setUsername("someone").build())
               .build();
    }

    private XenonProto.FileSystem createFileSystem() {
        return XenonProto.FileSystem.newBuilder()
                .setRequest(createFileSystemRequest())
                .setId("file://someone@/")
                .build();
    }

    @Before
    public void setUp() throws IOException {
        FileSystemsService service = new FileSystemsService();
        // register mocked filesystem to service
        filesystem = mock(FileSystem.class);
        when(filesystem.getAdaptorName()).thenReturn("file");
        when(filesystem.getLocation()).thenReturn("/");
        service.putFileSystem(createFileSystemRequest(), "someone", filesystem);
        // setup server
        String name = service.getClass().getName() + "-" + randomUUID().toString();
        server = InProcessServerBuilder.forName(name).directExecutor().addService(service).build();
        server.start();
        // setup client
        channel = InProcessChannelBuilder.forName(name).directExecutor().usePlaintext(true).build();
        client = XenonFileSystemsGrpc.newBlockingStub(channel);
    }

    @After
    public void tearDown() {
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
    public void closeAllFileSystems() throws XenonException {
        FileSystemsService service = new FileSystemsService();
        service.putFileSystem(createFileSystemRequest(), "someone", filesystem);

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

    @Test(expected = StatusRuntimeException.class)
    public void exists_throwsUp() throws XenonException {
        XenonProto.Path request = buildPath("/etc/passwd");
        when(filesystem.exists(new Path("/etc/passwd"))).thenThrow(new XenonException("file", "throw up"));

        client.exists(request);
    }

    @Test
    public void createDirectory() throws XenonException {
        XenonProto.Path request = buildPath("/somedir");

        client.createDirectory(request);

        verify(filesystem).createDirectory(new Path("/somedir"));
    }

    @Test
    public void createDirectories() throws XenonException {
        XenonProto.Path request = buildPath("/somedir");

        client.createDirectories(request);

        verify(filesystem).createDirectories(new Path("/somedir"));
    }

    @Test
    public void createFile() throws XenonException {
        XenonProto.Path request = buildPath("/somefile");

        client.createFile(request);

        verify(filesystem).createFile(new Path("/somefile"));
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
        XenonProto.Path request = buildPath("/etc/passwd");
        PathAttributesImplementation attribs = new PathAttributesImplementation();
        attribs.setPath(new Path("/etc/passwd"));
        attribs.setRegular(true);
        when(filesystem.getAttributes(new Path("/etc/passwd"))).thenReturn(attribs);

        XenonProto.PathAttributes response = client.getAttributes(request);

        XenonProto.PathAttributes expected = XenonProto.PathAttributes.newBuilder()
                .setPath(request)
                .setIsRegularFile(true)
                .build();
        assertEquals(expected, response);
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
    public void copy() throws XenonException {
        String source = "/etc/passwd";
        String target = "/etc/passwd.bak";
        XenonProto.CopyRequest request = XenonProto.CopyRequest.newBuilder()
            .setSource(buildPath(source))
            .setTarget(buildPath(target))
            .build();
        when(filesystem.copy(new Path(source), filesystem, new Path(target), CopyMode.CREATE, false)).thenReturn("COPY-1");

        XenonProto.CopyOperation response = client.copy(request);

        MessageOrBuilder expected = XenonProto.CopyOperation.newBuilder()
            .setFilesystem(createFileSystem())
            .setId("COPY-1")
            .build();
        assertEquals(expected, response);
    }
}