package nl.esciencecenter.xenon.grpc.filesystems;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.adaptors.filesystems.PathAttributesImplementation;
import nl.esciencecenter.xenon.filesystems.FileSystem;
import nl.esciencecenter.xenon.filesystems.Path;
import nl.esciencecenter.xenon.filesystems.PathAttributes;
import nl.esciencecenter.xenon.grpc.XenonFileSystemsGrpc;
import nl.esciencecenter.xenon.grpc.XenonProto;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static java.util.UUID.randomUUID;
import static nl.esciencecenter.xenon.grpc.MapUtils.empty;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    public void exists() throws XenonException {
        XenonProto.Path request = XenonProto.Path.newBuilder()
                .setFilesystem(createFileSystem())
                .setPath("/etc/passwd")
                .build();
        when(filesystem.exists(new Path("/etc/passwd"))).thenReturn(true);

        XenonProto.Is response = client.exists(request);

        assertTrue(response.getValue());
    }

    @Test
    public void createDirectory() throws XenonException {
        XenonProto.Path request = XenonProto.Path.newBuilder()
                .setFilesystem(createFileSystem())
                .setPath("/somedir")
                .build();

        client.createDirectory(request);

        verify(filesystem).createDirectory(new Path("/somedir"));
    }

    @Test
    public void createDirectories() throws XenonException {
        XenonProto.Path request = XenonProto.Path.newBuilder()
                .setFilesystem(createFileSystem())
                .setPath("/somedir")
                .build();

        client.createDirectories(request);

        verify(filesystem).createDirectories(new Path("/somedir"));
    }

    @Test
    public void createFile() throws XenonException {
        XenonProto.Path request = XenonProto.Path.newBuilder()
                .setFilesystem(createFileSystem())
                .setPath("/somefile")
                .build();

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
        XenonProto.Path request = XenonProto.Path.newBuilder()
                .setFilesystem(createFileSystem())
                .setPath("/etc/passwd")
                .build();
        PathAttributes attribs = new PathAttributesImplementation();
        // TODO fill it
        when(filesystem.getAttributes(new Path("/etc/passwd"))).thenReturn(attribs);

        XenonProto.PathAttributes response = client.getAttributes(request);

        XenonProto.PathAttributes expected = XenonProto.PathAttributes.newBuilder()
                // TODO fill it
                .build();
        assertEquals(expected, response);
    }

    @Test
    public void readSymbolicLink() throws XenonException {
        XenonProto.Path request = XenonProto.Path.newBuilder()
                .setFilesystem(createFileSystem())
                .setPath("/var/run")
                .build();
        when(filesystem.readSymbolicLink(new Path("/var/run"))).thenReturn(new Path("/run"));

        XenonProto.Path response = client.readSymbolicLink(request);

        XenonProto.Path expected = XenonProto.Path.newBuilder()
                .setFilesystem(createFileSystem())
                .setPath("/run")
                .build();
        assertEquals(expected, response);
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
}