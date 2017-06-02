package nl.esciencecenter.xenon.grpc.files;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import nl.esciencecenter.xenon.AdaptorStatus;
import nl.esciencecenter.xenon.XenonPropertyDescription;
import nl.esciencecenter.xenon.files.AttributeNotSupportedException;
import nl.esciencecenter.xenon.files.CopyStatus;
import nl.esciencecenter.xenon.files.FileAttributes;
import nl.esciencecenter.xenon.files.FileSystem;
import nl.esciencecenter.xenon.files.Path;
import nl.esciencecenter.xenon.files.PosixFilePermission;
import nl.esciencecenter.xenon.grpc.XenonProto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static nl.esciencecenter.xenon.grpc.MapUtils.mapPropertyDescriptions;

/*
    Writers to convert Xenon objects to gRPC response fields
 */
class Writers {
    private static final Logger LOGGER = LoggerFactory.getLogger(Writers.class);

    private Writers() {
    }

    static XenonProto.FileAttributes writeFileAttributes(FileAttributes a) {
        XenonProto.FileAttributes.Builder builder = XenonProto.FileAttributes.newBuilder()
            .setCreationTime(a.creationTime())
            .setIsDirectory(a.isDirectory())
            .setIsExecutable(a.isExecutable())
            .setIsHidden(a.isHidden())
            .setIsOther(a.isOther())
            .setIsReadable(a.isReadable())
            .setIsRegularFile(a.isRegularFile())
            .setIsSymbolicLink(a.isSymbolicLink())
            .setIsWritable(a.isWritable())
            .setLastAccessTime(a.lastAccessTime())
            .setLastModifiedTime(a.lastModifiedTime())
            .addAllPermissions(writePermissions(a))
            .setSize(a.size());
        try {
            builder.setOwner(a.owner());
        } catch (AttributeNotSupportedException e) {
            LOGGER.warn("Skipping owner, not supported for this path", e);
        }
        try {
            builder.setGroup(a.group());
        } catch (AttributeNotSupportedException e) {
            LOGGER.warn("Skipping group, not supported for this path", e);
        }
        return builder.build();
    }

    private static Set<XenonProto.PosixFilePermission> writePermissions(FileAttributes attribs) {
        Set<XenonProto.PosixFilePermission> permissions = new HashSet<>();
        try {
            for (PosixFilePermission permission : attribs.permissions()) {
                switch (permission) {
                    case GROUP_EXECUTE:
                        permissions.add(XenonProto.PosixFilePermission.GROUP_EXECUTE);
                        break;
                    case GROUP_READ:
                        permissions.add(XenonProto.PosixFilePermission.GROUP_READ);
                        break;
                    case GROUP_WRITE:
                        permissions.add(XenonProto.PosixFilePermission.GROUP_WRITE);
                        break;
                    case OTHERS_EXECUTE:
                        permissions.add(XenonProto.PosixFilePermission.OTHERS_EXECUTE);
                        break;
                    case OTHERS_READ:
                        permissions.add(XenonProto.PosixFilePermission.OTHERS_READ);
                        break;
                    case OTHERS_WRITE:
                        permissions.add(XenonProto.PosixFilePermission.OTHERS_WRITE);
                        break;
                    case OWNER_EXECUTE:
                        permissions.add(XenonProto.PosixFilePermission.OWNER_EXECUTE);
                        break;
                    case OWNER_READ:
                        permissions.add(XenonProto.PosixFilePermission.OWNER_READ);
                        break;
                    case OWNER_WRITE:
                        permissions.add(XenonProto.PosixFilePermission.OWNER_WRITE);
                        break;
                }
            }
        } catch (AttributeNotSupportedException e) {
            LOGGER.warn("Skipping posix file permissions, not supported for this path", e);
        }
        return permissions;
    }

    static XenonProto.Path writePath(Path path, XenonProto.FileSystem fs) {
        return XenonProto.Path.newBuilder().setFilesystem(fs).setPath(path.getRelativePath().getAbsolutePath()).build();
    }

    static String getFileSystemId(FileSystem fileSystem) {
        // TODO use more unique id, maybe use new label/alias field from request or a uuid
        return fileSystem.getAdaptorName() + ":" + fileSystem.getLocation();
    }

    private static XenonProto.FileSystem writeFileSystem(FileSystem fs) {
        XenonProto.NewFileSystemRequest request = XenonProto.NewFileSystemRequest.newBuilder()
            .setAdaptor(fs.getAdaptorName())
            .setLocation(fs.getLocation())
            .putAllProperties(fs.getProperties())
            .build();
        return XenonProto.FileSystem.newBuilder()
            .setId(getFileSystemId(fs))
            .setRequest(request)
            .build();
    }

    static XenonProto.FileSystems writeFileSystems(FileSystem[] xenonfilesystems) {
        XenonProto.FileSystems.Builder builder = XenonProto.FileSystems.newBuilder();
        for (FileSystem fs : xenonfilesystems) {
            builder.addFilesystems(writeFileSystem(fs));
        }
        return builder.build();
    }

    static XenonProto.CopyStatus writeCopyStatus(CopyStatus status, XenonProto.Copy copy) {
        XenonProto.CopyStatus.Builder builder = XenonProto.CopyStatus.newBuilder()
            .setBytesCopied(status.bytesCopied())
            .setBytesToCopy(status.bytesToCopy())
            .setCopy(copy)
            .setState(status.getState())
            .setDone(status.isDone())
            .setRunning(status.isRunning());
        if (status.hasException()) {
            builder.setError(status.getException().getMessage());
        }
        return builder.build();
    }

    static XenonProto.FileAdaptorDescription mapFileAdaptorDescription(AdaptorStatus status) {
        List<XenonProto.PropertyDescription> supportedProperties = mapPropertyDescriptions(status, XenonPropertyDescription.Component.FILESYSTEM);
        return XenonProto.FileAdaptorDescription.newBuilder()
            .setName(status.getName())
            .setDescription(status.getDescription())
            .addAllSupportedLocations(Arrays.asList(status.getSupportedLocations()))
            .addAllSupportedProperties(supportedProperties)
            .build();
    }
}
