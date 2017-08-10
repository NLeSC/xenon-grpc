package nl.esciencecenter.xenon.grpc.filesystems;

import static nl.esciencecenter.xenon.grpc.MapUtils.empty;
import static nl.esciencecenter.xenon.grpc.MapUtils.mapException;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.esciencecenter.xenon.filesystems.FileSystem;
import nl.esciencecenter.xenon.grpc.XenonProto;

public class Broadcaster {
    private static final Logger LOGGER = LoggerFactory.getLogger(Broadcaster.class);
    OutputStream pipe;
    final Map<String, FileSystem> fileSystems;
    final StreamObserver<XenonProto.Empty> responseObserver;

    Broadcaster(Map<String, FileSystem> fileSystems, StreamObserver<XenonProto.Empty> responseObserver) {
        this.fileSystems = fileSystems;
        this.responseObserver = responseObserver;
    }

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
