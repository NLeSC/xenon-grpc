package nl.esciencecenter.xenon.grpc.files;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.credentials.Credential;
import nl.esciencecenter.xenon.files.FileSystem;
import nl.esciencecenter.xenon.files.Files;
import nl.esciencecenter.xenon.files.Path;
import nl.esciencecenter.xenon.files.PathAlreadyExistsException;
import nl.esciencecenter.xenon.files.RelativePath;
import nl.esciencecenter.xenon.grpc.Parsers;
import nl.esciencecenter.xenon.grpc.XenonFilesGrpc;
import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.grpc.XenonSingleton;
import nl.esciencecenter.xenon.util.Utils;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

public class FilesService extends XenonFilesGrpc.XenonFilesImplBase {
    private final XenonSingleton singleton;
    private Map<String, FileSystemContainer> fileSystems = new ConcurrentHashMap<>();

    public FilesService(XenonSingleton singleton) {
        super();
        this.singleton = singleton;
    }

    @Override
    public void newFileSystem(XenonProto.NewFileSystemRequest request, StreamObserver<XenonProto.FileSystem> responseObserver) {
        Files files = singleton.getInstance().files();

        try {
            Credential credential = Parsers.parseCredential(singleton.getInstance(), request.getPassword(), request.getCertificate());
            FileSystem fileSystem = files.newFileSystem(
                    request.getAdaptor(),
                    request.getLocation(),
                    credential,
                    request.getPropertiesMap()
            );

            // TODO use more unique id, maybe use new label/alias field from request or a uuid
            String id = fileSystem.getAdaptorName() + ":" + fileSystem.getLocation();
            fileSystems.put(id, new FileSystemContainer(request, fileSystem));

            XenonProto.FileSystem value = XenonProto.FileSystem.newBuilder()
                    .setId(id)
                    .setRequest(request)
                    .build();
            responseObserver.onNext(value);
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void listFileSystems(XenonProto.Empty request, StreamObserver<XenonProto.FileSystems> responseObserver) {
        XenonProto.FileSystems.Builder setBuilder = XenonProto.FileSystems.newBuilder();
        XenonProto.FileSystem.Builder builder = XenonProto.FileSystem.newBuilder();
        for (Map.Entry<String, FileSystemContainer> entry : fileSystems.entrySet()) {
            XenonProto.NewFileSystemRequest fileSystemRequest = entry.getValue().getRequest();
            setBuilder.addFilesystems(builder
                    .setId(entry.getKey())
                    .setRequest(fileSystemRequest)
            );
        }
        responseObserver.onNext(setBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void close(XenonProto.FileSystem request, StreamObserver<XenonProto.Empty> responseObserver) {
        try {
            FileSystem filesystem = getFileSystem(request);
            singleton.getInstance().files().close(filesystem);
            fileSystems.remove(request.getId());
        } catch (XenonException e) {
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
        responseObserver.onNext(XenonProto.Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void exists(XenonProto.Path request, StreamObserver<XenonProto.Is> responseObserver) {
        Files files = singleton.getInstance().files();
        try {
            Path path = getPath(request);
            boolean value = files.exists(path);
            responseObserver.onNext(XenonProto.Is.newBuilder().setIs(value).build());
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    private Path getPath(XenonProto.Path request) throws XenonException, StatusException {
        Files files = singleton.getInstance().files();
        XenonProto.FileSystem fileSystemRequest = request.getFilesystem();
        FileSystem filesystem = getFileSystem(fileSystemRequest);
        return files.newPath(filesystem, new RelativePath(request.getPath()));
    }

    private FileSystem getFileSystem(XenonProto.FileSystem fileSystemRequest) throws StatusException {
        String id = fileSystemRequest.getId();
        if (!fileSystems.containsKey(id)) {
            throw Status.NOT_FOUND.augmentDescription(id).asException();
        }
        return fileSystems.get(id).getFileSystem();
    }

    @Override
    public void createDirectory(XenonProto.Path request, StreamObserver<XenonProto.Empty> responseObserver) {
        Files files = singleton.getInstance().files();
        try {
            Path path = getPath(request);
            files.createDirectory(path);
            responseObserver.onNext(XenonProto.Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (PathAlreadyExistsException e) {
            responseObserver.onError(Status.ALREADY_EXISTS.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        }
    }

    @Override
    public void createDirectories(XenonProto.Path request, StreamObserver<XenonProto.Empty> responseObserver) {
        Files files = singleton.getInstance().files();
        try {
            Path path = getPath(request);
            files.createDirectories(path);
            responseObserver.onNext(XenonProto.Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (PathAlreadyExistsException e) {
            responseObserver.onError(Status.ALREADY_EXISTS.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        }
    }

    @Override
    public void createFile(XenonProto.Path request, StreamObserver<XenonProto.Empty> responseObserver) {
        Files files = singleton.getInstance().files();
        try {
            Path path = getPath(request);
            files.createFile(path);
            responseObserver.onNext(XenonProto.Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (PathAlreadyExistsException e) {
            responseObserver.onError(Status.ALREADY_EXISTS.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        }
    }

    @Override
    public void delete(XenonProto.Path request, StreamObserver<XenonProto.Empty> responseObserver) {
        Files files = singleton.getInstance().files();
        try {
            Path path = getPath(request);
            Utils.recursiveDelete(files, path);
            responseObserver.onNext(XenonProto.Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (StatusException e) {
            responseObserver.onError(e);
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        }
    }

}
