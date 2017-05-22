package nl.esciencecenter.xenon.grpc.files;

import nl.esciencecenter.xenon.files.Copy;
import nl.esciencecenter.xenon.grpc.XenonProto;

public class CopyBackgroundTask {
    private final XenonProto.CopyRequest request;
    private final Copy copy;

    CopyBackgroundTask(XenonProto.CopyRequest request, Copy copy) {
        this.request = request;
        this.copy = copy;
    }

    public Copy getCopy() {
        return copy;
    }

    public XenonProto.CopyRequest getRequest() {
        return request;
    }
}
