package nl.esciencecenter.xenon.grpc.files;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import nl.esciencecenter.xenon.grpc.MapUtils;
import nl.esciencecenter.xenon.grpc.XenonFilesGrpc;
import nl.esciencecenter.xenon.grpc.XenonProto;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class LocalFilesWriteTest extends LocalFilesTestBase {
    private XenonFilesGrpc.XenonFilesStub aclient;

    @Before
    @Override
    public void setUp() throws IOException {
        super.setUp();
        // The default client does not have XenonFiles.write method
        // because it only works in blocking synchronous mode, but the XenonFiles.write must be called asynchronously
        // so use an asynchronous client
        aclient = XenonFilesGrpc.newStub(channel);
    }

    @Test
    public void write_singlechunk() throws IOException {
        String content = "Some text to put into the file";
        File targetFile = new File(myfolder.getRoot(), "target.txt");
        ByteString buffer = ByteString.copyFrom(content, Charset.defaultCharset());
        XenonProto.WriteRequest request = XenonProto.WriteRequest.newBuilder()
                .setPath(getLocalPath(targetFile.getAbsolutePath()))
                .setBuffer(buffer)
                .addOptions(XenonProto.WriteRequest.OpenOption.CREATE)
                .addOptions(XenonProto.WriteRequest.OpenOption.APPEND)
                .build();
        @SuppressWarnings("unchecked")
        StreamObserver<XenonProto.Empty> responseObserver = mock(StreamObserver.class);

        StreamObserver<XenonProto.WriteRequest> requestObserver = aclient.write(responseObserver);
        requestObserver.onNext(request);
        verify(responseObserver, never()).onNext(any(XenonProto.Empty.class));
        requestObserver.onCompleted();

        verify(responseObserver, timeout(100)).onNext(MapUtils.empty());
        verify(responseObserver, timeout(100)).onCompleted();
        verify(responseObserver, never()).onError(any(Throwable.class));
        assertEquals(Collections.singletonList(content), Files.readAllLines(targetFile.toPath(), Charset.defaultCharset()));
    }

    @Test
    public void write_multichunk() throws IOException {
        String content1 = "Some text to put into the file";
        String content2 = "Some other text to put into the file as well";
        File targetFile = new File(myfolder.getRoot(), "target.txt");
        ByteString buffer1 = ByteString.copyFrom(content1, Charset.defaultCharset());
        XenonProto.WriteRequest.Builder builder = XenonProto.WriteRequest.newBuilder()
                .setPath(getLocalPath(targetFile.getAbsolutePath()))
                .addOptions(XenonProto.WriteRequest.OpenOption.CREATE)
                .addOptions(XenonProto.WriteRequest.OpenOption.APPEND);
        XenonProto.WriteRequest request1 = builder.setBuffer(buffer1).build();
        ByteString buffer2 = ByteString.copyFrom(content2, Charset.defaultCharset());
        XenonProto.WriteRequest request2 = builder.setBuffer(buffer2).build();

        @SuppressWarnings("unchecked")
        StreamObserver<XenonProto.Empty> responseObserver = mock(StreamObserver.class);

        StreamObserver<XenonProto.WriteRequest> requestObserver = aclient.write(responseObserver);
        requestObserver.onNext(request1);
        requestObserver.onNext(request2);
        requestObserver.onCompleted();

        verify(responseObserver, timeout(100)).onNext(MapUtils.empty());
        verify(responseObserver, timeout(100)).onCompleted();
        List<String> result = Files.readAllLines(targetFile.toPath(), Charset.defaultCharset());
        List<String> expected = Collections.singletonList(content1 + content2);
        assertEquals(expected, result);
    }

}
