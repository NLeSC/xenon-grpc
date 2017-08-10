package nl.esciencecenter.xenon.grpc.filesystems;

import static nl.esciencecenter.xenon.grpc.MapUtils.empty;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.OutputStream;

import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.filesystems.FileSystem;
import nl.esciencecenter.xenon.filesystems.Path;
import nl.esciencecenter.xenon.grpc.XenonProto;

import com.google.protobuf.ByteString;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import org.junit.Before;
import org.junit.Test;

public class FileSystemServiceStreamTest {
    private FileSystem filesystem;
    private FileSystemsService service;

    private XenonProto.FileSystem createFileSystem() {
        return XenonProto.FileSystem.newBuilder()
                .setId("file://someone@/")
                .build();
    }

    private XenonProto.Path buildPath(String path) {
        return XenonProto.Path.newBuilder()
                .setFilesystem(createFileSystem())
                .setPath(path)
                .build();
    }

    @Before
    public void setUp() throws IOException, StatusException {
        service = new FileSystemsService();
        // register mocked filesystem to service
        filesystem = mock(FileSystem.class);
        when(filesystem.getAdaptorName()).thenReturn("file");
        when(filesystem.getLocation()).thenReturn("/");
        service.putFileSystem(filesystem, "someone");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void writeToFile_withoutSize() throws XenonException, IOException {
        StreamObserver<XenonProto.Empty> responseObserver = (StreamObserver<XenonProto.Empty>) mock(StreamObserver.class);
        String path = "/somefile";
        OutputStream pipe = mock(OutputStream.class);
        when(filesystem.writeToFile(new Path(path))).thenReturn(pipe);

        StreamObserver<XenonProto.WriteToFileRequest> requestBroadcaster = service.writeToFile(responseObserver);

        // send request
        ByteString content = ByteString.copyFrom("Some content".getBytes());
        XenonProto.WriteToFileRequest request = XenonProto.WriteToFileRequest.newBuilder()
                .setPath(buildPath(path))
                .setBuffer(content)
                .build();
        requestBroadcaster.onNext(request);
        requestBroadcaster.onCompleted();

        // verify received content
        verify(pipe).write(content.toByteArray());
        verify(pipe).close();

        // verify response
        verify(responseObserver).onNext(empty());
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void writeToFile_withSize() throws XenonException, IOException {
        StreamObserver<XenonProto.Empty> responseObserver = (StreamObserver<XenonProto.Empty>) mock(StreamObserver.class);
        String path = "/somefile";
        OutputStream pipe = mock(OutputStream.class);
        when(filesystem.writeToFile(new Path(path), 12L)).thenReturn(pipe);

        StreamObserver<XenonProto.WriteToFileRequest> requestBroadcaster = service.writeToFile(responseObserver);

        // send request
        ByteString content = ByteString.copyFrom("Some content".getBytes());
        XenonProto.WriteToFileRequest request = XenonProto.WriteToFileRequest.newBuilder()
                .setPath(buildPath(path))
                .setBuffer(content)
                .setSize(12L)
                .build();
        requestBroadcaster.onNext(request);
        requestBroadcaster.onCompleted();

        // verify received content
        verify(pipe).write(content.toByteArray());
        verify(pipe).close();

        // verify response
        verify(responseObserver).onNext(empty());
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void appendToFile() throws XenonException, IOException {
        StreamObserver<XenonProto.Empty> responseObserver = (StreamObserver<XenonProto.Empty>) mock(StreamObserver.class);
        String path = "/somefile";
        OutputStream pipe = mock(OutputStream.class);
        when(filesystem.appendToFile(new Path(path))).thenReturn(pipe);

        StreamObserver<XenonProto.AppendToFileRequest> requestBroadcaster = service.appendToFile(responseObserver);

        // send request
        ByteString content = ByteString.copyFrom("Some content".getBytes());
        XenonProto.AppendToFileRequest request = XenonProto.AppendToFileRequest.newBuilder()
                .setPath(buildPath(path))
                .setBuffer(content)
                .build();
        requestBroadcaster.onNext(request);
        requestBroadcaster.onCompleted();

        // verify received content
        verify(pipe).write(content.toByteArray());
        verify(pipe).close();

        // verify response
        verify(responseObserver).onNext(empty());
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }
}
