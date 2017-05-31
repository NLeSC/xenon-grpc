package nl.esciencecenter.xenon.grpc.files;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.XenonFactory;
import nl.esciencecenter.xenon.grpc.XenonFilesGrpc;
import nl.esciencecenter.xenon.grpc.XenonProto;
import nl.esciencecenter.xenon.grpc.XenonSingleton;

import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.internal.ServerImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

/**
 * Tests files service using local adaptor.
 * The service is wrapped in a InProcessServer so it can be called using a client stub instead of using observers directly
 */
public class LocalFilesServiceTest {
    @Rule
    public TemporaryFolder myfolder = new TemporaryFolder();
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private XenonSingleton singleton;
    private ServerImpl server;
    private ManagedChannel channel;
    private XenonFilesGrpc.XenonFilesBlockingStub client;

    @Before
    public void SetUp() throws IOException {
        singleton = new XenonSingleton();
        FilesService service = new FilesService(singleton);
        server = InProcessServerBuilder.forName("test").addService(service).build();
        server.start();
        channel = InProcessChannelBuilder.forName("test").directExecutor().usePlaintext(true).build();
        client = XenonFilesGrpc.newBlockingStub(channel);
    }

    @After
    public void tearDown() throws XenonException {
        XenonFactory.endXenon(singleton.getInstance());
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    public void localFileSystems() {
        XenonProto.Empty empty = XenonProto.Empty.getDefaultInstance();

        XenonProto.FileSystems response = client.localFileSystems(empty);

        assertTrue("Has some filesystems", response.getFilesystemsCount() > 0);
    }

    @Test
    public void isOpen() {
        XenonProto.FileSystem fs = getFs();

        XenonProto.Is response = client.isOpen(fs);

        assertTrue(response.getIs());
    }

    @Test
    public void listFileSystems() {
        XenonProto.FileSystem fs = getFs();
        XenonProto.Empty empty = XenonProto.Empty.getDefaultInstance();

        XenonProto.FileSystems response = client.listFileSystems(empty);

        // TODO only OK on single filesystem machines
        XenonProto.FileSystems expected = XenonProto.FileSystems.newBuilder().addFilesystems(fs).build();
        assertEquals(expected, response);
    }

    @Test
    public void exists_itdoes() {
        String path = myfolder.getRoot().getAbsolutePath();
        XenonProto.Empty empty = XenonProto.Empty.getDefaultInstance();
        XenonProto.FileSystem fs = client.localFileSystems(empty).getFilesystems(0);
        XenonProto.Path request = XenonProto.Path.newBuilder().setFilesystem(fs).setPath(path).build();

        XenonProto.Is response = client.exists(request);

        assertTrue(response.getIs());
    }

    /**
     * @return first local fs
     */
    private XenonProto.FileSystem getFs() {
        XenonProto.Empty empty = XenonProto.Empty.getDefaultInstance();
        return client.localFileSystems(empty).getFilesystems(0);
    }

    private XenonProto.Path getLocalPath(String path) {
        XenonProto.FileSystem fs =getFs();
        return XenonProto.Path.newBuilder().setFilesystem(fs).setPath(path).build();
    }

    @Test
    public void exists_itdoesnot() throws IOException {
        File somefile = myfolder.newFile("nowyoutseeit");
        somefile.delete();
        String path = somefile.getAbsolutePath();
        XenonProto.Path request = getLocalPath(path);

        XenonProto.Is response = client.exists(request);

        assertFalse(response.getIs());
    }

    @Test
    public void createDirectory() {
        String path = myfolder.getRoot().getAbsolutePath() + File.separator + "somedir";
        XenonProto.Path request = getLocalPath(path);

        client.createDirectory(request);

        File newdir = new File(path);
        assertTrue(newdir.exists());
    }

    @Test
    public void createDirectory_alreadyexists() {
        String path = myfolder.getRoot().getAbsolutePath();
        XenonProto.Path request = getLocalPath(path);
        thrown.expect(StatusRuntimeException.class);
        thrown.expectMessage("ALREADY_EXISTS: local adaptor: Directory file:///" + path + " already exists!");

        client.createDirectory(request);
    }

    @Test
    public void createDirectories() {
        String path = myfolder.getRoot().getAbsolutePath() + File.separator + "somedir1" + File.separator + "somedir2";
        XenonProto.Path request = getLocalPath(path);

        client.createDirectories(request);

        File newdir = new File(path);
        assertTrue(newdir.exists());
    }

    @Test
    public void createDirectories_alreadyexists() {
        String path = myfolder.getRoot().getAbsolutePath();
        XenonProto.Path request = getLocalPath(path);
        thrown.expect(StatusRuntimeException.class);
        thrown.expectMessage("ALREADY_EXISTS: local adaptor: Directory file:///" + path + " already exists!");

        client.createDirectories(request);
    }

    @Test
    public void createFile() throws IOException {
        File somefile = myfolder.newFile("nowyoutseeit");
        somefile.delete();
        XenonProto.Path request = getLocalPath(somefile.getAbsolutePath());

        client.createFile(request);

        assertTrue(somefile.exists());
    }

    @Test
    public void delete() throws IOException {
        File somefile = myfolder.newFile("nowyoutseeit");
        String path = somefile.getAbsolutePath();
        XenonProto.Path request = getLocalPath(path);

        client.delete(request);

        assertFalse(somefile.exists());
    }

    @Ignore("service hangs")
    @Test
    public void getAttributes() throws IOException {
        String path = myfolder.getRoot().getAbsolutePath();
        XenonProto.Path request = getLocalPath(path);

        XenonProto.FileAttributes attribs = client.getAttributes(request);

        assertTrue("isDirectory", attribs.getIsDirectory());
        Path epath = myfolder.getRoot().toPath();
        assertEquals("owner", Files.getOwner(epath), attribs.getOwner());
    }

    @Test
    public void move() throws IOException {
        String sourceFile = myfolder.newFile("sourcefile").getAbsolutePath();
        XenonProto.Path source = getLocalPath(sourceFile);
        String targetFile = myfolder.getRoot().getAbsolutePath() + File.separator + "targetfile";
        XenonProto.Path target = getLocalPath(targetFile);
        XenonProto.SourceTarget request = XenonProto.SourceTarget.newBuilder().setSource(source).setTarget(target).build();

        client.move(request);

        assertTrue("Target is present", new File(targetFile).exists());
        assertFalse("Source is gone",new File(sourceFile).exists());
    }

    @Test
    public void walkFileTree_onemptydir() {
        String path = myfolder.getRoot().getAbsolutePath();
        XenonProto.Path start = getLocalPath(path);
        XenonProto.WalkFileTreeRequest request = XenonProto.WalkFileTreeRequest.newBuilder().setStart(start).build();
        List<XenonProto.PathWithAttributes> objects = new ArrayList<>();

        Iterator<XenonProto.PathWithAttributes> response = client.walkFileTree(request);

        response.forEachRemaining(objects::add);
        assertEquals("Only start dir", 1, objects.size());
        XenonProto.PathWithAttributes startObject = objects.get(0);
        assertEquals("Start dir path",path, startObject.getPath().getPath());
    }

    @Test
    public void walkFileTree_noattributes() {
        String path = myfolder.getRoot().getAbsolutePath();
        XenonProto.Path start = getLocalPath(path);
        XenonProto.WalkFileTreeRequest request = XenonProto.WalkFileTreeRequest.newBuilder().setStart(start).setWithoutAttributes(true).build();
        List<XenonProto.PathWithAttributes> objects = new ArrayList<>();

        Iterator<XenonProto.PathWithAttributes> response = client.walkFileTree(request);

        response.forEachRemaining(objects::add);
        assertEquals("Only start dir", 1, objects.size());
        XenonProto.PathWithAttributes startObject = objects.get(0);
        assertEquals(startObject.getAttributes(), XenonProto.FileAttributes.getDefaultInstance());
    }

    @Test
    public void walkFileTree_subdirwithfiles_depthMax() throws IOException {
        String path = myfolder.getRoot().getAbsolutePath();
        XenonProto.Path start = getLocalPath(path);
        XenonProto.WalkFileTreeRequest request = XenonProto.WalkFileTreeRequest.newBuilder().setStart(start).build();
        Set<String> expectedPaths = new HashSet<>(Arrays.asList(
            ".",
            "." + File.separator + "level1dir",
            "." + File.separator + "level1file.txt",
            "." + File.separator + "level1dir" + File.separator + "level2file1.txt",
            "." + File.separator + "level1dir" + File.separator + "level2file2.md"
        ));

        walker(request, expectedPaths);
    }

    @Test
    public void walkFileTree_subdirwithfiles_depth1() throws IOException {
        String path = myfolder.getRoot().getAbsolutePath();
        XenonProto.Path start = getLocalPath(path);
        XenonProto.WalkFileTreeRequest request = XenonProto.WalkFileTreeRequest.newBuilder().setStart(start).setMaxDepth(1).build();
        Set<String> expectedPaths = new HashSet<>(Arrays.asList(
            ".",
            "." + File.separator + "level1dir",
            "." + File.separator + "level1file.txt"
        ));

        walker(request, expectedPaths);
    }

    @Test
    public void walkFileTree_filenameRegexp() throws IOException {
        String path = myfolder.getRoot().getAbsolutePath();
        XenonProto.Path start = getLocalPath(path);
        XenonProto.WalkFileTreeRequest request = XenonProto.WalkFileTreeRequest.newBuilder().setStart(start).setFilenameRegexp(".*\\.md").build();
        Set<String> expectedPaths = new HashSet<>(Arrays.asList(
            ".",
            "." + File.separator + "level1dir",
            "." + File.separator + "level1dir" + File.separator + "level2file2.md"
        ));

        walker(request, expectedPaths);
    }


    private void walker(XenonProto.WalkFileTreeRequest request, Set<String> expectedPaths) throws IOException {
        File level1dir = myfolder.newFolder("level1dir");
        myfolder.newFile("level1file.txt");
        File level2file1 = new File(level1dir, "level2file1.txt");
        level2file1.createNewFile();
        File level2file2 = new File(level1dir, "level2file2.md");
        level2file2.createNewFile();
        List<XenonProto.PathWithAttributes> objects = new ArrayList<>();

        Iterator<XenonProto.PathWithAttributes> response = client.walkFileTree(request);

        response.forEachRemaining(objects::add);
        String path = myfolder.getRoot().getAbsolutePath();
        Set<String> paths = objects.stream().map((XenonProto.PathWithAttributes p) -> p.getPath().getPath().replace(path, ".")).collect(Collectors.toSet());
        assertEquals(expectedPaths, paths);
    }

    @Test
    public void copy_singlefile() throws IOException {
        File sourceFile = myfolder.newFile("source.txt");
        File targetFile = new File(myfolder.getRoot(), "target.txt");
        XenonProto.Path source = getLocalPath(sourceFile.getAbsolutePath());
        XenonProto.Path target = getLocalPath(targetFile.getAbsolutePath());
        XenonProto.CopyRequest.Builder builder = XenonProto.CopyRequest.newBuilder();
        XenonProto.CopyRequest request = builder
            .setSource(source)
            .setTarget(target)
            .build();

        client.copy(request);

        assertTrue("Target exists", targetFile.exists());
    }

    @Test
    public void read() throws IOException {
        File somefile = myfolder.newFile("nowyoutseeit");
        List<String> content = Arrays.asList("line1", "line2");
        Files.write(somefile.toPath(), content, Charset.defaultCharset());
        XenonProto.Path request = getLocalPath(somefile.getAbsolutePath());

        Iterator<XenonProto.FileStream> response = client.read(request);
        List<XenonProto.FileStream> chunks = new ArrayList<>();
        response.forEachRemaining(chunks::add);
        String mergedContent = "";
        for (XenonProto.FileStream chunk: chunks) {
            mergedContent += chunk.getBuffer().toString(Charset.defaultCharset());
        }
        assertTrue(mergedContent.startsWith("line1"));
        // TODO compare whole content, but there are a bunch of ? at the end of the expected content
    }

    @Test
    public void listBackgroundCopyStatuses() {
        XenonProto.Empty empty = XenonProto.Empty.getDefaultInstance();
        XenonProto.CopyStatuses response = client.listBackgroundCopyStatuses(empty);

        assertEquals(0, response.getStatusesCount());
    }
}