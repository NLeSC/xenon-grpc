package nl.esciencecenter.xenon.grpc.filesystems;

import static nl.esciencecenter.xenon.grpc.MapUtils.empty;
import static nl.esciencecenter.xenon.grpc.MapUtils.mapCredential;
import static nl.esciencecenter.xenon.grpc.MapUtils.mapException;
import static nl.esciencecenter.xenon.grpc.filesystems.MapUtils.getFileSystemId;
import static nl.esciencecenter.xenon.grpc.filesystems.MapUtils.mapCopyMode;
import static nl.esciencecenter.xenon.grpc.filesystems.MapUtils.mapCopyStatus;
import static nl.esciencecenter.xenon.grpc.filesystems.MapUtils.mapFileAdaptorDescription;
import static nl.esciencecenter.xenon.grpc.filesystems.MapUtils.parsePermissions;
import static nl.esciencecenter.xenon.grpc.filesystems.MapUtils.writeFileAttributes;
import static nl.esciencecenter.xenon.grpc.filesystems.MapUtils.writeFileSystems;
import static nl.esciencecenter.xenon.grpc.filesystems.MapUtils.writePath;
import static nl.esciencecenter.xenon.utils.LocalFileSystemUtils.getLocalFileSystems;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.credentials.Credential;
import nl.esciencecenter.xenon.credentials.DefaultCredential;
import nl.esciencecenter.xenon.filesystems.CopyMode;
import nl.esciencecenter.xenon.filesystems.CopyStatus;
import nl.esciencecenter.xenon.filesystems.FileSystem;
import nl.esciencecenter.xenon.filesystems.FileSystemAdaptorDescription;
import nl.esciencecenter.xenon.filesystems.Path;
import nl.esciencecenter.xenon.filesystems.PathAttributes;
import nl.esciencecenter.xenon.filesystems.PosixFilePermission;
import nl.esciencecenter.xenon.grpc.XenonFileSystemsGrpc;
import nl.esciencecenter.xenon.grpc.XenonProto;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemsService extends XenonFileSystemsGrpc.XenonFileSystemsImplBase {
    private static final int BUFFER_SIZE = 8192;
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemsService.class);
    private Map<String, FileSystemContainer> fileSystems = new ConcurrentHashMap<>();

    @Override
    public void createFileSystem(XenonProto.CreateFileSystemRequest request, StreamObserver<XenonProto.FileSystem> responseObserver) {
        try {
            Credential credential = mapCredential(request);
            FileSystem fileSystem = FileSystem.create(
                    request.getAdaptor(),
                    request.getLocation(),
                    credential,
                    request.getPropertiesMap()
            );

            String fileSystemId = putFileSystem(request, credential.getUsername(), fileSystem);

            XenonProto.FileSystem value = XenonProto.FileSystem.newBuilder()
                    .setId(fileSystemId)
                    .setRequest(request)
                    .build();
            responseObserver.onNext(value);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }

    String putFileSystem(XenonProto.CreateFileSystemRequest request, String username, FileSystem fileSystem) {
        String fileSystemId = getFileSystemId(fileSystem, username);
        fileSystems.put(fileSystemId, new FileSystemContainer(request, fileSystem));
        return fileSystemId;
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
            filesystem.close();
            fileSystems.remove(request.getId());
            responseObserver.onNext(empty());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }

    public void closeAllFileSystems() throws XenonException {
        for (Map.Entry<String, FileSystemContainer> entry : fileSystems.entrySet()) {
            entry.getValue().getFileSystem().close();
            fileSystems.remove(entry.getKey());
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
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
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
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
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
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
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
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
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
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
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
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        } finally {
            try {
                if (pipe != null) {
                    pipe.close();
                }
            } catch (Exception e) {
                responseObserver.onError(mapException(e));
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
                } catch (Exception e) {
                    responseObserver.onError(mapException(e));
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
    public StreamObserver<XenonProto.AppendToFileRequest> appendToFile(StreamObserver<XenonProto.Empty> responseObserver) {
        return new StreamObserver<XenonProto.AppendToFileRequest>() {
            private OutputStream pipe;

            @Override
            public void onNext(XenonProto.AppendToFileRequest value) {
                try {
                    // open pip to write to on first incoming chunk
                    if (pipe == null) {
                        FileSystem filesystem = getFileSystem(value.getPath().getFilesystem());
                        Path path = getPath(value.getPath());
                        pipe = filesystem.appendToFile(path);
                    }
                    pipe.write(value.getBuffer().toByteArray());
                } catch (Exception e) {
                    responseObserver.onError(mapException(e));
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
            responseObserver.onNext(writeFileAttributes(request.getFilesystem(), attributes));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
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
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
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
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }

    @Override
    public void isOpen(XenonProto.FileSystem request, StreamObserver<XenonProto.Is> responseObserver) {
        try {
            FileSystem filesystem = getFileSystem(request);
            boolean open = filesystem.isOpen();
            responseObserver.onNext(XenonProto.Is.newBuilder().setValue(open).build());
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
        responseObserver.onCompleted();
    }

    @Override
    public void localFileSystems(XenonProto.Empty request, StreamObserver<XenonProto.FileSystems> responseObserver) {
        try {
            FileSystem[] xenonFilesystems = getLocalFileSystems();
            XenonProto.CreateFileSystemRequest.Builder builder = XenonProto.CreateFileSystemRequest.newBuilder();

            DefaultCredential cred = new DefaultCredential();
            // Store file systems for later use
            for (FileSystem xenonFilesystem : xenonFilesystems) {
                String fileSystemId = getFileSystemId(xenonFilesystem, cred.getUsername());
                XenonProto.CreateFileSystemRequest fsRequest = builder
                    .setAdaptor(xenonFilesystem.getAdaptorName())
                    .setLocation(xenonFilesystem.getLocation())
                    .setDefaultCred(XenonProto.DefaultCredential.getDefaultInstance())
                    .putAllProperties(xenonFilesystem.getProperties())
                    .build();
                fileSystems.put(fileSystemId, new FileSystemContainer(fsRequest, xenonFilesystem));
            }

            XenonProto.FileSystems filesystems = writeFileSystems(xenonFilesystems);
            responseObserver.onNext(filesystems);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }

    @Override
    public void rename(XenonProto.RenameRequest request, StreamObserver<XenonProto.Empty> responseObserver) {
        try {
            FileSystem filesystem = getFileSystem(request.getFilesystem());
            Path source = new Path(request.getSource());
            Path target = new Path(request.getTarget());

            filesystem.rename(source, target);

            responseObserver.onNext(empty());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }

    @Override
    public void createSymbolicLink(XenonProto.CreateSymbolicLinkRequest request, StreamObserver<XenonProto.Empty> responseObserver) {
        try {
            FileSystem filesystem = getFileSystem(request.getFilesystem());
            Path link = new Path(request.getLink());
            Path target = new Path(request.getTarget());

            filesystem.createSymbolicLink(link, target);

            responseObserver.onNext(empty());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }

    @Override
    public void copy(XenonProto.CopyRequest request, StreamObserver<XenonProto.CopyOperation> responseObserver) {
        try {
            FileSystem sourceFS = getFileSystem(request.getSource().getFilesystem());
            Path sourcePath = getPath(request.getSource());
            FileSystem targetFS = getFileSystem(request.getTarget().getFilesystem());
            Path targetPath = getPath(request.getTarget());
            CopyMode mode = mapCopyMode(request.getMode());

            String copyId = sourceFS.copy(sourcePath, targetFS, targetPath, mode, request.getRecursive());

            XenonProto.CopyOperation response = XenonProto.CopyOperation.newBuilder()
                    .setId(copyId)
                    .setFilesystem(request.getSource().getFilesystem())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }

    @Override
    public void cancel(XenonProto.CopyOperation request, StreamObserver<XenonProto.CopyStatus> responseObserver) {
        try {
            FileSystem filesystem = getFileSystem(request.getFilesystem());

            CopyStatus status = filesystem.cancel(request.getId());

            responseObserver.onNext(mapCopyStatus(status, request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }

    @Override
    public void getStatus(XenonProto.CopyOperation request, StreamObserver<XenonProto.CopyStatus> responseObserver) {
        try {
            FileSystem filesystem = getFileSystem(request.getFilesystem());

            CopyStatus status = filesystem.getStatus(request.getId());

            responseObserver.onNext(mapCopyStatus(status, request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }

    @Override
    public void getAdaptorDescription(XenonProto.AdaptorName request, StreamObserver<XenonProto.FileSystemAdaptorDescription> responseObserver) {
        try {
            FileSystemAdaptorDescription descIn = FileSystem.getAdaptorDescription(request.getName());
            XenonProto.FileSystemAdaptorDescription description = mapFileAdaptorDescription(descIn);
            responseObserver.onNext(description);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
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
    public void list(XenonProto.ListRequest request, StreamObserver<XenonProto.PathAttributes> responseObserver) {
        try {
            FileSystem filesystem = getFileSystem(request.getDir().getFilesystem());
            Path dir = getPath(request.getDir());

            Iterable<PathAttributes> items = filesystem.list(dir, request.getRecursive());
            for (PathAttributes item : items) {
                responseObserver.onNext(writeFileAttributes(request.getDir().getFilesystem(), item));
            }
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }

    @Override
    public void getEntryPath(XenonProto.FileSystem request, StreamObserver<XenonProto.Path> responseObserver) {
        try {
            FileSystem filesystem = getFileSystem(request);

            Path path = filesystem.getEntryPath();

            XenonProto.Path pathResponse = XenonProto.Path.newBuilder().setPath(path.getAbsolutePath()).setFilesystem(request).build();
            responseObserver.onNext(pathResponse);
            responseObserver.onCompleted();
        } catch (StatusException e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void waitUntilDone(XenonProto.CopyOperationWithTimeout request, StreamObserver<XenonProto.CopyStatus> responseObserver) {
        try {
            FileSystem filesystem = getFileSystem(request.getFilesystem());

            CopyStatus status = filesystem.waitUntilDone(request.getId(), request.getTimeout());

            XenonProto.CopyOperation operation = XenonProto.CopyOperation.newBuilder().setFilesystem(request.getFilesystem()).setId(request.getId()).build();
            responseObserver.onNext(mapCopyStatus(status, operation));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }
}
