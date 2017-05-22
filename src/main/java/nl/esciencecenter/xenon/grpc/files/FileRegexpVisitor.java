package nl.esciencecenter.xenon.grpc.files;

import io.grpc.stub.StreamObserver;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.files.FileAttributes;
import nl.esciencecenter.xenon.files.Files;
import nl.esciencecenter.xenon.files.Path;
import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.util.FileVisitResult;
import nl.esciencecenter.xenon.util.FileVisitor;

import static nl.esciencecenter.xenon.grpc.files.Writers.writeFileAttributes;

public class FileRegexpVisitor implements FileVisitor {
    private final StreamObserver<XenonProto.PathWithAttributes> observer;
    private final String filenameRegexp;
    private final boolean returnAttributes;
    private final XenonProto.PathWithAttributes.Builder builder;
    private final XenonProto.Path.Builder pathBuilder;

    FileRegexpVisitor(XenonProto.FileSystem fileSystem, StreamObserver<XenonProto.PathWithAttributes> responseObserver, boolean attributes, String filenameRegexp) {
        this.observer = responseObserver;
        this.returnAttributes = attributes;
        this.filenameRegexp = filenameRegexp;
        this.builder = XenonProto.PathWithAttributes.newBuilder();
        this.pathBuilder = XenonProto.Path.newBuilder().setFilesystem(fileSystem);
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, XenonException exception, Files files) throws XenonException {
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dir, FileAttributes attributes, Files files) throws XenonException {
        builder.setPath(pathBuilder.setPath(dir.getRelativePath().toString()));
        if (returnAttributes) {
            builder.setAttributes(writeFileAttributes(attributes));
        }
        observer.onNext(builder.build());
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(Path file, FileAttributes attributes, Files files) throws XenonException {
        String path = file.getRelativePath().toString();
        if ("".equals(filenameRegexp) || path.matches(filenameRegexp)) {
            builder.setPath(pathBuilder.setPath(path));
            if (returnAttributes) {
                builder.setAttributes(writeFileAttributes(attributes));
            }
        }
        observer.onNext(builder.build());
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, XenonException exception, Files files) throws XenonException {
        throw exception;
    }
}
