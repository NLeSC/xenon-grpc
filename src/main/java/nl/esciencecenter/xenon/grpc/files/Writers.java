package nl.esciencecenter.xenon.grpc.files;

import nl.esciencecenter.xenon.filesystems.*;
import nl.esciencecenter.xenon.grpc.XenonProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static nl.esciencecenter.xenon.grpc.MapUtils.mapPropertyDescriptions;

/*
    Writers to convert Xenon objects to gRPC response fields
 */
class Writers {
    private static final Logger LOGGER = LoggerFactory.getLogger(Writers.class);

    private Writers() {
    }

    static XenonProto.PathAttributes writeFileAttributes(PathAttributes a) {
        XenonProto.PathAttributes.Builder builder = XenonProto.PathAttributes.newBuilder()
            .setCreationTime(a.getCreationTime())
            .setIsDirectory(a.isDirectory())
            .setIsExecutable(a.isExecutable())
            .setIsHidden(a.isHidden())
            .setIsOther(a.isOther())
            .setIsReadable(a.isReadable())
            .setIsRegularFile(a.isRegular())
            .setIsSymbolicLink(a.isSymbolicLink())
            .setIsWritable(a.isWritable())
            .setLastAccessTime(a.getLastAccessTime())
            .setLastModifiedTime(a.getLastModifiedTime())
            .addAllPermissions(writePermissions(a))
            .setSize(a.getSize());
        try {
            builder.setOwner(a.getOwner());
        } catch (AttributeNotSupportedException e) {
            LOGGER.warn("Skipping owner, not supported for this path", e);
        }
        try {
            builder.setGroup(a.getGroup());
        } catch (AttributeNotSupportedException e) {
            LOGGER.warn("Skipping group, not supported for this path", e);
        }
        return builder.build();
    }

    private static Set<XenonProto.PosixFilePermission> writePermissions(PathAttributes attribs) {
        Set<XenonProto.PosixFilePermission> permissions = new HashSet<>();
        try {
            for (PosixFilePermission permission : attribs.getPermissions()) {
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

    static String getFileSystemId(FileSystem fileSystem, String username) {
        return fileSystem.getAdaptorName() + "://" + username + "@" + fileSystem.getLocation();
    }

    private static XenonProto.FileSystem writeFileSystem(FileSystem fs) {
        XenonProto.CreateFileSystemRequest request = XenonProto.CreateFileSystemRequest.newBuilder()
            .setAdaptor(fs.getAdaptorName())
            .setLocation(fs.getLocation())
            .putAllProperties(fs.getProperties())
            .build();
        return XenonProto.FileSystem.newBuilder()
            .setId(getFileSystemId(fs, username))
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

    static XenonProto.CopyStatus writeCopyStatus(CopyStatus status, XenonProto.CopyResponse copy) {
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

    static XenonProto.FileSystemAdaptorDescription mapFileAdaptorDescription(FileSystemAdaptorDescription status) {
        List<XenonProto.PropertyDescription> supportedProperties = mapPropertyDescriptions(status.getSupportedProperties());
        return XenonProto.FileSystemAdaptorDescription.newBuilder()
            .setName(status.getName())
            .setDescription(status.getDescription())
            .addAllSupportedLocations(Arrays.asList(status.getSupportedLocations()))
            .addAllSupportedProperties(supportedProperties)
            .build();
    }
}
