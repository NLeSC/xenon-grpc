package nl.esciencecenter.xenon.grpc.filesystems;

import nl.esciencecenter.xenon.filesystems.FileSystem;
import nl.esciencecenter.xenon.grpc.XenonProto;

public class FileSystemContainer {
    private final XenonProto.CreateFileSystemRequest request;
    private final FileSystem fileSystem;

    FileSystemContainer(XenonProto.CreateFileSystemRequest request, FileSystem fileSystem) {
        this.request = request;
        this.fileSystem = fileSystem;
    }

    FileSystem getFileSystem() {
        return fileSystem;
    }

    public XenonProto.CreateFileSystemRequest getRequest() {
        return request;
    }
}
