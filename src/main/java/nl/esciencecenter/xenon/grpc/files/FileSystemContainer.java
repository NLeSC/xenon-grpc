package nl.esciencecenter.xenon.grpc.files;

import nl.esciencecenter.xenon.files.FileSystem;
import nl.esciencecenter.xenon.grpc.XenonProto;

public class FileSystemContainer {
    private final XenonProto.NewFileSystemRequest request;
    private final FileSystem fileSystem;

    public FileSystemContainer(XenonProto.NewFileSystemRequest request, FileSystem fileSystem) {
        this.request = request;
        this.fileSystem = fileSystem;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public XenonProto.NewFileSystemRequest getRequest() {
        return request;
    }
}
