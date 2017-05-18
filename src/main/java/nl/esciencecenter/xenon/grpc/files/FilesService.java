package nl.esciencecenter.xenon.grpc.files;

import nl.esciencecenter.xenon.grpc.XenonFilesGrpc;
import nl.esciencecenter.xenon.grpc.XenonSingleton;

public class FilesService extends XenonFilesGrpc.XenonFilesImplBase {
    private final XenonSingleton singleton;

    public FilesService(XenonSingleton singleton) {
        super();
        this.singleton = singleton;
    }
}
