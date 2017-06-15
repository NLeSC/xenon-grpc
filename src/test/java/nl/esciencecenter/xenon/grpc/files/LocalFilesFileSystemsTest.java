package nl.esciencecenter.xenon.grpc.files;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import nl.esciencecenter.xenon.grpc.XenonProto;

import io.grpc.StatusRuntimeException;
import org.junit.Test;

public class LocalFilesFileSystemsTest extends LocalFilesTestBase {
    @Test
    public void localFileSystems() {
        XenonProto.Empty empty = XenonProto.Empty.getDefaultInstance();

        XenonProto.FileSystems response = client.localFileSystems(empty);

        assertTrue("Has some filesystems", response.getFilesystemsCount() > 0);
    }

    @Test
    public void isOpen() {
        XenonProto.FileSystem fs = getFs();

        XenonProto.Is response = client.isOpen(fs);

        assertTrue(response.getValue());
    }

    @Test
    public void listFileSystems() {
        XenonProto.FileSystem fs = getFs();
        XenonProto.Empty empty = XenonProto.Empty.getDefaultInstance();

        XenonProto.FileSystems response = client.listFileSystems(empty);

        // TODO only OK on single filesystem machines
        XenonProto.FileSystems expected = XenonProto.FileSystems.newBuilder().addFilesystems(fs).build();
        assertEquals(expected, response);
    }


    @Test
    public void listFileSystems_empty() {
        XenonProto.Empty empty = XenonProto.Empty.getDefaultInstance();

        XenonProto.FileSystems response = client.listFileSystems(empty);

        assertTrue(response.getFilesystemsList().isEmpty());
    }

    @Test
    public void newFileSystem() {
        XenonProto.NewFileSystemRequest request = XenonProto.NewFileSystemRequest.newBuilder()
            .setAdaptor("file")
            .build();

        XenonProto.FileSystem response = client.newFileSystem(request);

        assertEquals(request, response.getRequest());
        assertFalse("Has id", response.getId().isEmpty());
    }

    @Test
    public void close_nonexistingfs_notfound() {
        String someId = "some-id-that-does-not-exist";
        XenonProto.FileSystem request = XenonProto.FileSystem.newBuilder()
            .setId(someId)
            .build();
        thrown.expect(StatusRuntimeException.class);
        thrown.expectMessage("NOT_FOUND: " + someId);

        client.close(request);
    }

    @Test
    public void close() {
        XenonProto.NewFileSystemRequest fsrequest = XenonProto.NewFileSystemRequest.newBuilder()
            .setAdaptor("file")
            .build();
        XenonProto.FileSystem fs = client.newFileSystem(fsrequest);

        client.close(fs);

        XenonProto.FileSystems fsList = client.listFileSystems(XenonProto.Empty.getDefaultInstance());
        assertTrue(fsList.getFilesystemsList().isEmpty());
    }
}
