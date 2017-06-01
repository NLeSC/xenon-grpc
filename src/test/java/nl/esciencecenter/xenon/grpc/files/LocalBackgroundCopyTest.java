package nl.esciencecenter.xenon.grpc.files;

import static org.junit.Assert.assertEquals;

import nl.esciencecenter.xenon.grpc.XenonProto;

import io.grpc.StatusRuntimeException;
import org.junit.Test;

public class LocalBackgroundCopyTest extends LocalFilesTestBase {
    @Test
    public void listBackgroundCopyStatuses_empty() {
        XenonProto.Empty empty = XenonProto.Empty.getDefaultInstance();

        XenonProto.CopyStatuses response = client.listBackgroundCopyStatuses(empty);

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
}
