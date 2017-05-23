package nl.esciencecenter.xenon.grpc.jobs;

import java.io.InputStream;

import nl.esciencecenter.xenon.grpc.XenonProto;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;

class JobOutputStreamsForwarder {
    private final XenonProto.JobOutputStreams.Builder builder;
    private final StreamObserver<XenonProto.JobOutputStreams> observer;

    JobOutputStreamsForwarder(StreamObserver<XenonProto.JobOutputStreams> responseObserver, InputStream stderr, InputStream stdout) {
        this.observer = responseObserver;
        builder = XenonProto.JobOutputStreams.newBuilder();
        // TODO setup forwarder to pipe stdout and stderr of streams to responseObserver
    }

    private void writeStdOut(byte[] value) {
        XenonProto.JobOutputStreams response = builder.clearStderr().setStdout(ByteString.copyFrom(value)).build();
        observer.onNext(response);
    }

    private void writeStdErr(byte[] value) {
        XenonProto.JobOutputStreams response = builder.clearStdout().setStderr(ByteString.copyFrom(value)).build();
        observer.onNext(response);
    }

    void close() {
        observer.onCompleted();
    }
}
