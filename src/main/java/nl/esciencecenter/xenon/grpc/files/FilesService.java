package nl.esciencecenter.xenon.grpc.files;

import static java.util.UUID.randomUUID;
import static nl.esciencecenter.xenon.grpc.files.Parsers.parseCopyOptions;
import static nl.esciencecenter.xenon.grpc.files.Parsers.parseOpenOptions;
import static nl.esciencecenter.xenon.grpc.files.Parsers.parsePermissions;
import static nl.esciencecenter.xenon.grpc.files.Writers.getFileSystemId;
import static nl.esciencecenter.xenon.grpc.files.Writers.mapFileAdaptorDescription;
import static nl.esciencecenter.xenon.grpc.files.Writers.writeCopyStatus;
import static nl.esciencecenter.xenon.grpc.files.Writers.writeFileAttributes;
import static nl.esciencecenter.xenon.grpc.files.Writers.writeFileSystems;
import static nl.esciencecenter.xenon.grpc.files.Writers.writePath;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import nl.esciencecenter.xenon.AdaptorStatus;
import nl.esciencecenter.xenon.Xenon;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.credentials.Credential;
import nl.esciencecenter.xenon.files.Copy;
import nl.esciencecenter.xenon.files.CopyOption;
import nl.esciencecenter.xenon.files.CopyStatus;
import nl.esciencecenter.xenon.files.FileAttributes;
import nl.esciencecenter.xenon.files.FileSystem;
import nl.esciencecenter.xenon.files.Files;
import nl.esciencecenter.xenon.files.Path;
import nl.esciencecenter.xenon.files.PathAlreadyExistsException;
import nl.esciencecenter.xenon.files.PosixFilePermission;
import nl.esciencecenter.xenon.files.RelativePath;
import nl.esciencecenter.xenon.grpc.Parsers;
import nl.esciencecenter.xenon.grpc.XenonFilesGrpc;
import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.grpc.XenonSingleton;
import nl.esciencecenter.xenon.util.FileVisitor;
import nl.esciencecenter.xenon.util.Utils;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilesService extends XenonFilesGrpc.XenonFilesImplBase {
    private static final int BUFFER_SIZE = 8192;
    private static final Logger LOGGER = LoggerFactory.getLogger(FilesService.class);
    private final XenonSingleton singleton;
    private Map<String, FileSystemContainer> fileSystems = new ConcurrentHashMap<>();
    private Map<String, CopyBackgroundTask> copyBackgroundTasks = new ConcurrentHashMap<>();

    public FilesService(XenonSingleton singleton) {
        super();
        this.singleton = singleton;
    }

    @Override
    public void newFileSystem(XenonProto.NewFileSystemRequest request, StreamObserver<XenonProto.FileSystem> responseObserver) {
        Files files = singleton.getInstance().files();

        try {
            Credential credential = Parsers.parseCredential(singleton.getInstance(), request.getAdaptor(), request.getPassword(), request.getCertificate());
            FileSystem fileSystem = files.newFileSystem(
                    request.getAdaptor(),
                    request.getLocation(),
                    credential,
                    request.getPropertiesMap()
            );

            String fileSystemId = getFileSystemId(fileSystem);
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
            // TODO cancel+delete any background copy tasks running on this filesystem
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

    @Override
    public void read(XenonProto.Path request, StreamObserver<XenonProto.FileStream> responseObserver) {
        Files files = singleton.getInstance().files();
        XenonProto.FileStream.Builder builder = XenonProto.FileStream.newBuilder();
        InputStream pipe = null;
        try {
            Path path = getPath(request);
            pipe = files.newInputStream(path);
            // Read file in chunks and pass on as stream of byte arrays
            byte[] buffer = new byte[BUFFER_SIZE];
            while (pipe.read(buffer) != -1) {
                responseObserver.onNext(builder.setBuffer(ByteString.copyFrom(buffer)).build());
            }
            responseObserver.onCompleted();
        } catch (StatusException e) {
            responseObserver.onError(e);
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
    public StreamObserver<XenonProto.WriteRequest> write(StreamObserver<XenonProto.Empty> responseObserver) {
        return new StreamObserver<XenonProto.WriteRequest>() {
            private OutputStream pipe;

            @Override
            public void onNext(XenonProto.WriteRequest value) {
                try {
                    // open pip to write to on first incoming chunk
                    if (pipe == null) {
                        Files files = singleton.getInstance().files();
                        Path path = getPath(value.getPath());
                        pipe = files.newOutputStream(path, parseOpenOptions(value.getOptionsList()));
                    }
                    pipe.write(value.getBuffer().toByteArray());
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
                responseObserver.onNext(XenonProto.Empty.getDefaultInstance());
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public void getAttributes(XenonProto.Path request, StreamObserver<XenonProto.FileAttributes> responseObserver) {
        Files files = singleton.getInstance().files();
        try {
            Path path = getPath(request);
            FileAttributes attributes = files.getAttributes(path);
            responseObserver.onNext(writeFileAttributes(attributes));
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void setPosixFilePermissions(XenonProto.PosixFilePermissionsRequest request, StreamObserver<XenonProto.Empty> responseObserver) {
        Files files = singleton.getInstance().files();
        try {
            Path path = getPath(request.getPath());
            Set<PosixFilePermission> permissions = parsePermissions(request.getPermissionsList());
            files.setPosixFilePermissions(path, permissions);
            responseObserver.onNext(XenonProto.Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void readSymbolicLink(XenonProto.Path request, StreamObserver<XenonProto.Path> responseObserver) {
        Files files = singleton.getInstance().files();
        try {
            Path source = getPath(request);
            Path target = files.readSymbolicLink(source);
            responseObserver.onNext(writePath(target, request.getFilesystem()));
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void isOpen(XenonProto.FileSystem request, StreamObserver<XenonProto.Is> responseObserver) {
        Files files = singleton.getInstance().files();
        try {
            FileSystem filesystem = getFileSystem(request);
            boolean open = files.isOpen(filesystem);
            responseObserver.onNext(XenonProto.Is.newBuilder().setIs(open).build());
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
                String fileSystemId = getFileSystemId(xenonFilesystem);
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
    public void move(XenonProto.SourceTarget request, StreamObserver<XenonProto.Empty> responseObserver) {
        Files files = singleton.getInstance().files();
        try {
            Path source = getPath(request.getSource());
            Path target = getPath(request.getTarget());
            files.move(source, target);
            responseObserver.onNext(XenonProto.Empty.getDefaultInstance());
            responseObserver.onCompleted();
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

            responseObserver.onNext(XenonProto.Empty.getDefaultInstance());
            responseObserver.onCompleted();
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
    public void cancelBackgroundCopy(XenonProto.Copy request, StreamObserver<XenonProto.CopyStatus> responseObserver) {
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
    public void getBackgroundCopyStatus(XenonProto.Copy request, StreamObserver<XenonProto.CopyStatus> responseObserver) {
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
    public void getAdaptorDescription(XenonProto.AdaptorName request, StreamObserver<XenonProto.FileAdaptorDescription> responseObserver) {
        Xenon xenon = singleton.getInstance();
        try {
            AdaptorStatus status = xenon.getAdaptorStatus(request.getName());
            XenonProto.FileAdaptorDescription description = mapFileAdaptorDescription(status);
            responseObserver.onNext(description);
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        }
    }

    @Override
    public void getAdaptorDescriptions(XenonProto.Empty request, StreamObserver<XenonProto.FileAdaptorDescriptions> responseObserver) {
        Xenon xenon = singleton.getInstance();
        // TODO use xenon.files().getAdaptorDescriptions(), when https://github.com/NLeSC/Xenon/issues/430 is completed
        // TODO Filter getAdaptorStatuses on file capable adaptors
        AdaptorStatus[] statuses = xenon.getAdaptorStatuses();

        XenonProto.FileAdaptorDescriptions.Builder setBuilder = XenonProto.FileAdaptorDescriptions.newBuilder();
        HashSet<String> fileBasedAdaptors = new HashSet<>(Arrays.asList("local", "ssh", "webdav", "ftp"));
        for (AdaptorStatus status : statuses) {
            if (fileBasedAdaptors.contains(status.getName())) {
                XenonProto.FileAdaptorDescription description = mapFileAdaptorDescription(status);
                setBuilder.addDescriptions(description);
            }
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
            responseObserver.onNext(XenonProto.Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void walkFileTree(XenonProto.WalkFileTreeRequest request, StreamObserver<XenonProto.PathWithAttributes> responseObserver) {
        Files files = singleton.getInstance().files();
        try {
            Path start = getPath(request.getStart());
            FileVisitor visitor = new FileRegexpVisitor(request.getStart().getFilesystem(), responseObserver, !request.getWithoutAttributes(), request.getFilenameRegexp());
            Integer depth = request.getMaxDepth();
            if (depth == 0) {
                depth = Integer.MAX_VALUE;
            }
            Utils.walkFileTree(files, start, request.getFollowLinks(), depth, visitor);

            responseObserver.onCompleted();
        } catch (XenonException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asException());
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }
}
