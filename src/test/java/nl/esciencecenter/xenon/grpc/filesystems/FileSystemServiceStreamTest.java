package nl.esciencecenter.xenon.grpc.filesystems;

import static nl.esciencecenter.xenon.grpc.MapUtils.empty;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.OutputStream;

import com.google.protobuf.ByteString;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.filesystems.FileSystem;
import nl.esciencecenter.xenon.filesystems.Path;
import nl.esciencecenter.xenon.grpc.XenonProto;

public class FileSystemServiceStreamTest {
    private FileSystem filesystem;
    private FileSystemService service;
    private String filesystemId;

    private XenonProto.FileSystem createFileSystem() {
        return XenonProto.FileSystem.newBuilder()
                .setId(filesystemId)
                .build();
    }

    private XenonProto.Path buildPath(String path) {
        return XenonProto.Path.newBuilder()
            .setPath(path)
            .build();
    }

    @Before
    public void setUp() throws IOException, StatusException, XenonException {
        service = new FileSystemService();
        // register mocked filesystem to service
        filesystem = mock(FileSystem.class);
        when(filesystem.getAdaptorName()).thenReturn("file");
        when(filesystem.getLocation()).thenReturn("/");
        filesystemId = service.putFileSystem(filesystem, "someone");
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
                .setFilesystem(createFileSystem())
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
                .setFilesystem(createFileSystem())
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
    public void writeToFile_badFsID() throws XenonException {
        ArgumentCaptor<StatusException> captor = ArgumentCaptor.forClass(StatusException.class);
        StreamObserver<XenonProto.Empty> responseObserver = (StreamObserver<XenonProto.Empty>) mock(StreamObserver.class);
        String path = "/somefile";
        OutputStream pipe = mock(OutputStream.class);
        when(filesystem.writeToFile(new Path(path))).thenReturn(pipe);

        StreamObserver<XenonProto.WriteToFileRequest> requestBroadcaster = service.writeToFile(responseObserver);

        // send request
        ByteString content = ByteString.copyFrom("".getBytes());
        String badFSID = "bad filesystem id";
        XenonProto.WriteToFileRequest request = XenonProto.WriteToFileRequest.newBuilder()
            .setFilesystem(XenonProto.FileSystem.newBuilder().setId(badFSID))
            .setPath(buildPath(path))
            .setBuffer(content)
            .build();
        requestBroadcaster.onNext(request);
        requestBroadcaster.onCompleted();

        verify(responseObserver).onError(captor.capture());
        StatusException actual = captor.getValue();
        assertThat(actual.getMessage(), containsString(badFSID));
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
                .setFilesystem(createFileSystem())
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
    public void appendToFile_badFsID() throws XenonException {
        ArgumentCaptor<StatusException> captor = ArgumentCaptor.forClass(StatusException.class);
        StreamObserver<XenonProto.Empty> responseObserver = (StreamObserver<XenonProto.Empty>) mock(StreamObserver.class);
        String path = "/somefile";
        OutputStream pipe = mock(OutputStream.class);
        when(filesystem.appendToFile(new Path(path))).thenReturn(pipe);

        StreamObserver<XenonProto.AppendToFileRequest> requestBroadcaster = service.appendToFile(responseObserver);

        // send request
        ByteString content = ByteString.copyFrom("Some content".getBytes());
        String badFSID = "bad filesystem id";
        XenonProto.AppendToFileRequest request = XenonProto.AppendToFileRequest.newBuilder()
            .setFilesystem(XenonProto.FileSystem.newBuilder().setId(badFSID))
            .setPath(buildPath(path))
            .setBuffer(content)
            .build();
        requestBroadcaster.onNext(request);
        requestBroadcaster.onCompleted();

        verify(responseObserver).onError(captor.capture());
        StatusException actual = captor.getValue();
        assertThat(actual.getMessage(), containsString(badFSID));
    }
}
