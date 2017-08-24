package nl.esciencecenter.xenon.grpc.filesystems;

import static nl.esciencecenter.xenon.grpc.MapUtils.mapException;

import java.util.Map;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import nl.esciencecenter.xenon.filesystems.FileSystem;
import nl.esciencecenter.xenon.filesystems.Path;
import nl.esciencecenter.xenon.grpc.XenonProto;

public class AppendToFileBroadcaster extends Broadcaster implements StreamObserver<XenonProto.AppendToFileRequest>{

    AppendToFileBroadcaster(Map<String, FileSystem> fileSystems, StreamObserver<XenonProto.Empty> responseObserver) {
        super(fileSystems, responseObserver);
    }

    @Override
    public void onNext(XenonProto.AppendToFileRequest value) {
        try {
            // open pip to write to on first incoming chunk
            if (pipe == null) {
                String id = value.getFilesystem().getId();
                if (!fileSystems.containsKey(id)) {
                    throw Status.NOT_FOUND.withDescription("File system with id: " + id).asException();
                }
                FileSystem filesystem = fileSystems.get(id);
                Path path = new Path(value.getPath().getPath());
                pipe = filesystem.appendToFile(path);
            }
            pipe.write(value.getBuffer().toByteArray());
        } catch (Exception e) {
            responseObserver.onError(mapException(e));
        }
    }
}
