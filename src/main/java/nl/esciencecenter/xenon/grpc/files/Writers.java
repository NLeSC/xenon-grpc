package nl.esciencecenter.xenon.grpc.files;

import java.util.HashSet;
import java.util.Set;

import nl.esciencecenter.xenon.files.AttributeNotSupportedException;
import nl.esciencecenter.xenon.files.FileAttributes;
import nl.esciencecenter.xenon.files.PosixFilePermission;
import nl.esciencecenter.xenon.grpc.XenonProto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
    Writers to convert Xenon objects to gRPC response fields
 */
class Writers {
    private static final Logger LOGGER = LoggerFactory.getLogger(Writers.class);

    private Writers() {
    }

    static XenonProto.FileAttributes writeWritePermissions(FileAttributes a) {
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

}
