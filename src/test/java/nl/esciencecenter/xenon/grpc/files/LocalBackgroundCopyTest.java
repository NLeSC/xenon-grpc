package nl.esciencecenter.xenon.grpc.files;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.IOException;

import nl.esciencecenter.xenon.grpc.XenonProto;

import io.grpc.StatusRuntimeException;
import org.junit.Before;
import org.junit.Test;

public class LocalBackgroundCopyTest extends LocalFilesTestBase {
    private File sourceFile;
    private File targetFile;
    private XenonProto.Path source;
    private XenonProto.Path target;

    @Test
    public void listBackgroundCopyStatuses_empty() {
        XenonProto.CopyStatuses response = client.listBackgroundCopyStatuses(empty());

        assertEquals(0, response.getStatusesCount());
    }

    @Test
    public void deleteBackgroundCopy_notfound() {
        client.deleteBackgroundCopy(getNotFoundCopy());
    }

    @Test
    public void getBackgroundCopyStatus_notfound() {
        client.getBackgroundCopyStatus(getNotFoundCopy());
    }

    @Test
    public void cancelBackgroundCopy_notfound() {
        client.cancelBackgroundCopy(getNotFoundCopy());
    }

    private XenonProto.Copy getNotFoundCopy() {
        String someId = "some-id-that-does-not-exist";
        XenonProto.Copy request = XenonProto.Copy.newBuilder()
            .setId(someId)
            .build();
        thrown.expect(StatusRuntimeException.class);
        thrown.expectMessage("NOT_FOUND: " + someId);
        return request;
    }

    @Before
    public void setUp() throws IOException {
        super.setUp();
        sourceFile = myfolder.newFile("source.txt");
        targetFile = new File(myfolder.getRoot(), "target.txt");
        source = getLocalPath(sourceFile.getAbsolutePath());
        target = getLocalPath(targetFile.getAbsolutePath());
    }

    @Test
    public void backgroundCopy() {
        XenonProto.Copy response = submitCopy();

        assertFalse("Has id", response.getId().isEmpty());
    }

    @Test
    public void listBackgroundCopyStatuses() {
        XenonProto.Copy copy = submitCopy();

        XenonProto.CopyStatuses response = client.listBackgroundCopyStatuses(empty());

        assertEquals("has 1 background copy", 1, response.getStatusesCount());
        XenonProto.CopyStatus status = response.getStatuses(0);
        assertEquals("has the submitted copy", copy, status.getCopy());
    }

    @Test
    public void deleteBackgroundCopy() {
        XenonProto.Copy copy = submitCopy();

        client.deleteBackgroundCopy(copy);

        XenonProto.CopyStatuses response = client.listBackgroundCopyStatuses(empty());
        assertEquals("Deleted copy is gone", 0, response.getStatusesCount());
    }

    private XenonProto.Copy submitCopy() {
        XenonProto.CopyRequest request = XenonProto.CopyRequest.newBuilder()
            .setSource(source)
            .setTarget(target)
            .build();

        return client.backgroundCopy(request);
    }


}
