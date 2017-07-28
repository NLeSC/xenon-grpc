package nl.esciencecenter.xenon.grpc.files;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import nl.esciencecenter.xenon.UnknownAdaptorException;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.credentials.Credential;
import nl.esciencecenter.xenon.filesystems.*;
import nl.esciencecenter.xenon.grpc.Parsers;
import nl.esciencecenter.xenon.grpc.XenonFileSystemsGrpc;
import nl.esciencecenter.xenon.grpc.XenonProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.UUID.randomUUID;
import static nl.esciencecenter.xenon.grpc.MapUtils.empty;
import static nl.esciencecenter.xenon.grpc.files.Parsers.*;
import static nl.esciencecenter.xenon.grpc.files.Writers.*;
import static nl.esciencecenter.xenon.grpc.files.Writers.writeFileAttributes;

public class FileSystemsService extends XenonFileSystemsGrpc.XenonFileSystemsImplBase {
    private static final int BUFFER_SIZE = 8192;
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemsService.class);
    private Map<String, FileSystemContainer> fileSystems = new ConcurrentHashMap<>();
    private Map<String, CopyBackgroundTask> copyBackgroundTasks = new ConcurrentHashMap<>();

    @Override
    public void createFileSystem(XenonProto.CreateFileSystemRequest request, StreamObserver<XenonProto.FileSystem> responseObserver) {
        try {
            Credential credential = Parsers.parseCredential(request.getDefaultCred(), request.getPassword(), request.getCertificate());
            FileSystem fileSystem = FileSystem.create(
                    request.getAdaptor(),
                    request.getLocation(),
                    credential,
                    request.getPropertiesMap()
            );

            String fileSystemId = getFileSystemId(fileSystem, username);
            fileSystems.put(fileSystemId, new FileSystemContainer(request, fileSystem));

            XenonProto.FileSystem value = XenonProto.FileSystem.newBuilder()
                    .setId(fileSystemId)
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
            XenonProto.CreateFileSystemRequest fileSystemRequest = entry.getValue().getRequest();
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
            // TODO cancel+delete any background copy tasks running on this filesystem
            filesystem.close();
            fileSystems.remove(request.getId());
            responseObserver.onNext(empty());
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void exists(XenonProto.Path request, StreamObserver<XenonProto.Is> responseObserver) {
        try {
            FileSystem filesystem = getFileSystem(request.getFilesystem());
            Path path = getPath(request);
            boolean value = filesystem.exists(path);
            responseObserver.onNext(XenonProto.Is.newBuilder().setValue(value).build());
            responseObserver.onCompleted();
        } catch (NoSuchPathException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asException());
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    private Path getPath(XenonProto.Path request) throws XenonException, StatusException {
        return new Path(request.getPath());
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
        try {
            FileSystem filesystem = getFileSystem(request.getFilesystem());
            Path path = getPath(request);
            filesystem.createDirectory(path);
            responseObserver.onNext(empty());
            responseObserver.onCompleted();
        } catch (PathAlreadyExistsException e) {
            responseObserver.onError(Status.ALREADY_EXISTS.withDescription(e.getMessage()).withCause(e).asException());
        } catch (NoSuchPathException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        }
    }

    @Override
    public void createDirectories(XenonProto.Path request, StreamObserver<XenonProto.Empty> responseObserver) {
        try {
            FileSystem filesystem = getFileSystem(request.getFilesystem());
            Path path = getPath(request);
            filesystem.createDirectories(path);
            responseObserver.onNext(empty());
            responseObserver.onCompleted();
        } catch (PathAlreadyExistsException e) {
            responseObserver.onError(Status.ALREADY_EXISTS.withDescription(e.getMessage()).withCause(e).asException());
        } catch (NoSuchPathException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        }
    }

    @Override
    public void createFile(XenonProto.Path request, StreamObserver<XenonProto.Empty> responseObserver) {
        try {
            FileSystem filesystem = getFileSystem(request.getFilesystem());
            Path path = getPath(request);
            filesystem.createFile(path);
            responseObserver.onNext(empty());
            responseObserver.onCompleted();
        } catch (PathAlreadyExistsException e) {
            responseObserver.onError(Status.ALREADY_EXISTS.withDescription(e.getMessage()).withCause(e).asException());
        } catch (NoSuchPathException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        }
    }

    @Override
    public void delete(XenonProto.DeleteRequest request, StreamObserver<XenonProto.Empty> responseObserver) {
        try {
            FileSystem filesystem = getFileSystem(request.getPath().getFilesystem());
            Path path = getPath(request.getPath());
            filesystem.delete(path, request.getRecursive());
            responseObserver.onNext(empty());
            responseObserver.onCompleted();
        } catch (StatusException e) {
            responseObserver.onError(e);
        } catch (NoSuchPathException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asException());
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        }
    }

    @Override
    public void readFromFile(XenonProto.Path request, StreamObserver<XenonProto.ReadFromFileResponse> responseObserver) {
        XenonProto.ReadFromFileResponse.Builder builder = XenonProto.ReadFromFileResponse.newBuilder();
        InputStream pipe = null;
        try {
            FileSystem filesystem = getFileSystem(request.getFilesystem());
            Path path = getPath(request);
            pipe = filesystem.readFromFile(path);
            // Read file in chunks and pass on as stream of byte arrays
            ByteString buffer;
            do {
                buffer = ByteString.readFrom(pipe, BUFFER_SIZE);
                responseObserver.onNext(builder.setBuffer(buffer).build());
            } while (!buffer.isEmpty());
            responseObserver.onCompleted();
        } catch (StatusException e) {
            responseObserver.onError(e);
        } catch (NoSuchPathException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asException());
        } catch (XenonException | IOException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } finally {
            try {
                if (pipe != null) {
                    pipe.close();
                }
            } catch (IOException e) {
                responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
            }
        }
    }

    @Override
    public StreamObserver<XenonProto.WriteToFileRequest> writeToFile(StreamObserver<XenonProto.Empty> responseObserver) {
        return new StreamObserver<XenonProto.WriteToFileRequest>() {
            private OutputStream pipe;

            @Override
            public void onNext(XenonProto.WriteToFileRequest value) {
                try {
                    // open pip to write to on first incoming chunk
                    if (pipe == null) {
                        FileSystem filesystem = getFileSystem(value.getPath().getFilesystem());
                        Path path = getPath(value.getPath());
                        if (XenonProto.WriteToFileRequest.getDefaultInstance().getSize() == value.getSize()) {
                            pipe = filesystem.writeToFile(path);
                        } else {
                            pipe = filesystem.writeToFile(path, value.getSize());
                        }
                    }
                    pipe.write(value.getBuffer().toByteArray());
                } catch (NoSuchPathException e) {
                    responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asException());
                } catch (XenonException | IOException e) {
                    responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
                } catch (StatusException e) {
                    responseObserver.onError(e);
                }
            }

            @Override
            public void onError(Throwable t) {
                if (pipe != null) {
                    try {
                        LOGGER.warn("Error from client", t);
                        pipe.close();
                    } catch (IOException e) {
                        LOGGER.warn("Error from server", e);
                    }
                }
            }

            @Override
            public void onCompleted() {
                if (pipe != null) {
                    try {
                        pipe.close();
                    } catch (IOException e) {
                        LOGGER.warn("Error from server", e);
                    }
                }
                responseObserver.onNext(empty());
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public void getAttributes(XenonProto.Path request, StreamObserver<XenonProto.PathAttributes> responseObserver) {
        try {
            FileSystem filesystem = getFileSystem(request.getFilesystem());
            Path path = getPath(request);
            PathAttributes attributes = filesystem.getAttributes(path);
            responseObserver.onNext(writeFileAttributes(attributes));
            responseObserver.onCompleted();
        } catch (NoSuchPathException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asException());
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void setPosixFilePermissions(XenonProto.SetPosixFilePermissionsRequest request, StreamObserver<XenonProto.Empty> responseObserver) {
        try {
            FileSystem filesystem = getFileSystem(request.getPath().getFilesystem());
            Path path = getPath(request.getPath());
            Set<PosixFilePermission> permissions = parsePermissions(request.getPermissionsList());
            filesystem.setPosixFilePermissions(path, permissions);
            responseObserver.onNext(empty());
            responseObserver.onCompleted();
        } catch (NoSuchPathException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asException());
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void readSymbolicLink(XenonProto.Path request, StreamObserver<XenonProto.Path> responseObserver) {
        try {
            FileSystem filesystem = getFileSystem(request.getFilesystem());
            Path source = getPath(request);
            Path target = filesystem.readSymbolicLink(source);
            responseObserver.onNext(writePath(target, request.getFilesystem()));
            responseObserver.onCompleted();
        } catch (NoSuchPathException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asException());
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void isOpen(XenonProto.FileSystem request, StreamObserver<XenonProto.Is> responseObserver) {
        try {
            FileSystem filesystem = getFileSystem(request);
            boolean open = filesystem.isOpen();
            responseObserver.onNext(XenonProto.Is.newBuilder().setValue(open).build());
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void localFileSystems(XenonProto.Empty request, StreamObserver<XenonProto.FileSystems> responseObserver) {
        Files files = singleton.getInstance().files();
        try {
            FileSystem[] xenonFilesystems = Utils.getLocalFileSystems(files);
            XenonProto.NewFileSystemRequest.Builder builder = XenonProto.NewFileSystemRequest.newBuilder();

            // Store file systems for later use
            for (FileSystem xenonFilesystem : xenonFilesystems) {
                String fileSystemId = getFileSystemId(xenonFilesystem, username);
                XenonProto.NewFileSystemRequest fsRequest = builder
                    .setAdaptor(xenonFilesystem.getAdaptorName())
                    .setLocation(xenonFilesystem.getLocation())
                    .putAllProperties(xenonFilesystem.getProperties())
                    .build();
                fileSystems.put(fileSystemId, new FileSystemContainer(fsRequest, xenonFilesystem));
            }

            XenonProto.FileSystems filesystems = writeFileSystems(xenonFilesystems);
            responseObserver.onNext(filesystems);
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        }
    }

    @Override
    public void rename(XenonProto.SourceTarget request, StreamObserver<XenonProto.Empty> responseObserver) {
        try {
            FileSystem filesystem = getFileSystem(request.getSource().getFilesystem());
            Path source = getPath(request.getSource());
            Path target = getPath(request.getTarget());
            filesystem.rename(source, target);
            responseObserver.onNext(empty());
            responseObserver.onCompleted();
        } catch (NoSuchPathException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asException());
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void copy(XenonProto.CopyRequest request, StreamObserver<XenonProto.Empty> responseObserver) {
        Files files = singleton.getInstance().files();
        try {
            Path source = getPath(request.getSource());
            Path target = getPath(request.getTarget());
            CopyOption[] options = parseCopyOptions(request.getOptionsList());

            Utils.recursiveCopy(files, source, target, options);

            responseObserver.onNext(empty());
            responseObserver.onCompleted();
        } catch (NoSuchPathException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asException());
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void backgroundCopy(XenonProto.CopyRequest request, StreamObserver<XenonProto.Copy> responseObserver) {
        Files files = singleton.getInstance().files();
        try {
            Path source = getPath(request.getSource());
            Path target = getPath(request.getTarget());
            CopyOption[] options = parseCopyOptions(request.getOptionsList());
            // Mark ASYNCHRONOUS
            List<CopyOption> asyncOptions = new ArrayList<>();
            asyncOptions.addAll(Arrays.asList(options));
            asyncOptions.add(CopyOption.ASYNCHRONOUS);

            Copy copy = files.copy(source, target, asyncOptions.toArray(new CopyOption[0]));
            String copyId = getCopyId(copy);

            copyBackgroundTasks.put(copyId, new CopyBackgroundTask(request, copy));

            XenonProto.Copy response = XenonProto.Copy.newBuilder().setId(copyId).setRequest(request).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    private String getCopyId(Copy copy) {
        return randomUUID().toString();
    }

    @Override
    public void cancel(XenonProto.CopyResponse request, StreamObserver<XenonProto.CopyStatus> responseObserver) {
        Files files = singleton.getInstance().files();
        try {
            Copy copy = getBackgroundCopyTask(request);

            CopyStatus status = files.cancelCopy(copy);

            copyBackgroundTasks.remove(request.getId());

            responseObserver.onNext(writeCopyStatus(status, request));
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void getStatus(XenonProto.CopyResponse request, StreamObserver<XenonProto.CopyStatus> responseObserver) {
        Files files = singleton.getInstance().files();
        try {
            Copy copy = getBackgroundCopyTask(request);

            CopyStatus status = files.getCopyStatus(copy);

            responseObserver.onNext(writeCopyStatus(status, request));
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    private Copy getBackgroundCopyTask(XenonProto.Copy request) throws StatusException {
        String id = request.getId();
        if (!copyBackgroundTasks.containsKey(id)) {
            throw Status.NOT_FOUND.augmentDescription(id).asException();
        }
        return copyBackgroundTasks.get(id).getCopy();
    }

    @Override
    public void listBackgroundCopyStatuses(XenonProto.Empty request, StreamObserver<XenonProto.CopyStatuses> responseObserver) {
        Files files = singleton.getInstance().files();
        XenonProto.CopyStatuses.Builder builder = XenonProto.CopyStatuses.newBuilder();
        try {
            for (Map.Entry<String, CopyBackgroundTask> entry : copyBackgroundTasks.entrySet()) {
                CopyStatus status = files.getCopyStatus(entry.getValue().getCopy());
                XenonProto.Copy copy = XenonProto.Copy.newBuilder()
                    .setId(entry.getKey())
                    .setRequest(entry.getValue().getRequest())
                    .build();
                builder.addStatuses(writeCopyStatus(status, copy));
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        }
    }

    @Override
    public void getAdaptorDescription(XenonProto.AdaptorName request, StreamObserver<XenonProto.FileSystemAdaptorDescription> responseObserver) {
        try {
            FileSystemAdaptorDescription descIn = FileSystem.getAdaptorDescription(request.getName());
            XenonProto.FileSystemAdaptorDescription description = mapFileAdaptorDescription(descIn);
            responseObserver.onNext(description);
            responseObserver.onCompleted();
        } catch (UnknownAdaptorException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asException());
        }
    }

    @Override
    public void getAdaptorDescriptions(XenonProto.Empty request, StreamObserver<XenonProto.FileSystemAdaptorDescriptions> responseObserver) {
        FileSystemAdaptorDescription[] descIns = FileSystem.getAdaptorDescriptions();

        XenonProto.FileSystemAdaptorDescriptions.Builder setBuilder = XenonProto.FileSystemAdaptorDescriptions.newBuilder();
        for (FileSystemAdaptorDescription descIn : descIns) {
                XenonProto.FileSystemAdaptorDescription description = mapFileAdaptorDescription(descIn);
                setBuilder.addDescriptions(description);
        }
        responseObserver.onNext(setBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteBackgroundCopy(XenonProto.Copy request, StreamObserver<XenonProto.Empty> responseObserver) {
        Files files = singleton.getInstance().files();
        try {
            Copy copy = getBackgroundCopyTask(request);
            CopyStatus status = files.getCopyStatus(copy);
            if (!status.isDone()) {
                files.cancelCopy(copy);
            }
            copyBackgroundTasks.remove(request.getId());
            responseObserver.onNext(empty());
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void list(XenonProto.ListRequest request, StreamObserver<XenonProto.PathAttributes> responseObserver) {
        try {
            FileSystem filesystem = getFileSystem(request.getDir().getFilesystem());
            Path dir = getPath(request.getDir());

            Iterable<PathAttributes> items = filesystem.list(dir, request.getRecursive());
            for (PathAttributes item : items) {
                responseObserver.onNext(writeFileAttributes(item));
            }
            responseObserver.onCompleted();
        } catch (NoSuchPathException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asException());
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }
}
