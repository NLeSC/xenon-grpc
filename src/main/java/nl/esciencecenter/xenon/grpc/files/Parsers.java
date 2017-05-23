package nl.esciencecenter.xenon.grpc.files;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import nl.esciencecenter.xenon.files.CopyOption;
import nl.esciencecenter.xenon.files.OpenOption;
import nl.esciencecenter.xenon.files.PosixFilePermission;
import nl.esciencecenter.xenon.grpc.XenonProto;

import io.grpc.Status;
import io.grpc.StatusException;

class Parsers {
    private Parsers() {
    }

    static OpenOption[] parseOpenOptions(List<XenonProto.WriteRequest.OpenOption> optionsList) throws StatusException {
        List<OpenOption> options = new ArrayList<>();
        // OpenOption is only used to write a file, so always add WRITE option.
        options.add(OpenOption.WRITE);
        for (XenonProto.WriteRequest.OpenOption option: optionsList) {
            switch (option) {
                case CREATE:
                    options.add(OpenOption.CREATE);
                    break;
                case OPEN:
                    options.add(OpenOption.OPEN);
                    break;
                case OPEN_OR_CREATE:
                    options.add(OpenOption.OPEN_OR_CREATE);
                    break;
                case APPEND:
                    options.add(OpenOption.APPEND);
                    break;
                case TRUNCATE:
                    options.add(OpenOption.TRUNCATE);
                    break;
                case UNRECOGNIZED:
                    throw Status.INVALID_ARGUMENT.withDescription("Unrecognized open option").asException();
            }
        }
        return options.toArray(new OpenOption[0]);
    }

    static Set<PosixFilePermission> parsePermissions(List<XenonProto.PosixFilePermission> permissionsValueList) throws StatusException {
        Set<PosixFilePermission> permissions = new HashSet<>();
        for (XenonProto.PosixFilePermission permission:  permissionsValueList) {
            switch (permission) {
                case OWNER_READ:
                    permissions.add(PosixFilePermission.OWNER_READ);
                    break;
                case OWNER_WRITE:
                    permissions.add(PosixFilePermission.OWNER_WRITE);
                    break;
                case OWNER_EXECUTE:
                    permissions.add(PosixFilePermission.OWNER_EXECUTE);
                    break;
                case GROUP_READ:
                    permissions.add(PosixFilePermission.GROUP_READ);
                    break;
                case GROUP_WRITE:
                    permissions.add(PosixFilePermission.GROUP_WRITE);
                    break;
                case GROUP_EXECUTE:
                    permissions.add(PosixFilePermission.GROUP_EXECUTE);
                    break;
                case OTHERS_READ:
                    permissions.add(PosixFilePermission.OTHERS_READ);
                    break;
                case OTHERS_WRITE:
                    permissions.add(PosixFilePermission.OTHERS_WRITE);
                    break;
                case OTHERS_EXECUTE:
                    permissions.add(PosixFilePermission.OTHERS_EXECUTE);
                    break;
                case UNRECOGNIZED:
                    throw Status.INVALID_ARGUMENT.withDescription("Unrecognized posix file permission").asException();
            }
        }
        return permissions;
    }

    static CopyOption[] parseCopyOptions(List<XenonProto.CopyRequest.CopyOption> optionList) throws StatusException {
        List<CopyOption> options = new ArrayList<>();
        for (XenonProto.CopyRequest.CopyOption option : optionList) {
            switch (option) {
                case CREATE:
                    options.add(CopyOption.CREATE);
                    break;
                case REPLACE:
                    options.add(CopyOption.REPLACE);
                    break;
                case IGNORE:
                    options.add(CopyOption.IGNORE);
                    break;
                case APPEND:
                    options.add(CopyOption.APPEND);
                    break;
                case RESUME:
                    options.add(CopyOption.RESUME);
                    break;
                case VERIFY:
                    options.add(CopyOption.VERIFY);
                    break;
                case UNRECOGNIZED:
                    throw Status.INVALID_ARGUMENT.withDescription("Unrecognized copy option").asException();
            }
        }
        return options.toArray(new CopyOption[0]);
    }

}
