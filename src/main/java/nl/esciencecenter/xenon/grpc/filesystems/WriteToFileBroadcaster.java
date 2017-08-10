package nl.esciencecenter.xenon.grpc.filesystems;

import static nl.esciencecenter.xenon.grpc.MapUtils.empty;
import static nl.esciencecenter.xenon.grpc.MapUtils.mapException;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.esciencecenter.xenon.filesystems.FileSystem;
import nl.esciencecenter.xenon.filesystems.Path;
import nl.esciencecenter.xenon.grpc.XenonProto;

public class WriteToFileBroadcaster implements StreamObserver<XenonProto.WriteToFileRequest> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WriteToFileBroadcaster.class);
    private OutputStream pipe;
    private final Map<String, FileSystem> fileSystems;
    private final StreamObserver<XenonProto.Empty> responseObserver;

    WriteToFileBroadcaster(Map<String, FileSystem> fileSystems, StreamObserver<XenonProto.Empty> responseObserver) {
        this.fileSystems = fileSystems;
        this.responseObserver = responseObserver;
    }

    @Override
    public void onNext(XenonProto.WriteToFileRequest value) {
        try {
            // open pip to write to on first incoming chunk
            if (pipe == null) {
                String id = value.getPath().getFilesystem().getId();
                if (!fileSystems.containsKey(id)) {
                    throw Status.NOT_FOUND.withDescription("File system with id: " + id).asException();
                }
                FileSystem filesystem = fileSystems.get(id);
                Path path = new Path(value.getPath().getPath());
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
                pipe.close();
            } catch (IOException e) {
                responseObserver.onError(mapException(e));
            }
        }
        responseObserver.onError(mapException(t));
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
}
