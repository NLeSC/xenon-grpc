package nl.esciencecenter.xenon.grpc.files;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import nl.esciencecenter.xenon.files.Files;
import nl.esciencecenter.xenon.files.Path;
import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.util.FileVisitResult;

import io.grpc.stub.StreamObserver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FileRegexpVisitorTest {
    private FileRegexpVisitor visitor;
    private XenonProto.FileSystem fileSystem;
    @Mock
    private StreamObserver<XenonProto.PathWithAttributes> observer;
    @Mock
    private Files files;

    @Before
    public void setUp() {
        fileSystem = XenonProto.FileSystem.getDefaultInstance();
        visitor = new FileRegexpVisitor(fileSystem, observer, false, "");
    }

    @Test
    public void postVisitDirectory() throws Exception {
        FileVisitResult result = visitor.postVisitDirectory(mock(Path.class), null, files);
        assertEquals(result, FileVisitResult.CONTINUE);
        verify(observer, times(0)).onCompleted();
    }
}