package nl.esciencecenter.xenon.grpc.files;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import nl.esciencecenter.xenon.adaptors.local.LocalAdaptor;
import nl.esciencecenter.xenon.grpc.XenonProto;

import io.grpc.StatusRuntimeException;
import org.junit.Test;

/**
 * Tests files service using local adaptor.
 * The service is wrapped in a InProcessServer so it can be called using a client stub instead of using observers directly
 */
public class LocalFilesServiceTest extends LocalFilesTestBase {
    @Test
    public void getAdaptorDescription() {
        XenonProto.AdaptorName request = XenonProto.AdaptorName.newBuilder().setName("local").build();

        XenonProto.FileAdaptorDescription response = client.getAdaptorDescription(request);
        XenonProto.FileAdaptorDescription expected = localAdaptorDescription();

        assertEquals(expected, response);
    }

    private XenonProto.FileAdaptorDescription localAdaptorDescription() {
        return XenonProto.FileAdaptorDescription.newBuilder()
                .setName("local")
                .setDescription(LocalAdaptor.ADAPTOR_DESCRIPTION)
                .addAllSupportedLocations(Arrays.asList("(null)", "(empty string)", "/"))
                .build();
    }

    @Test
    public void getAdaptorDescriptions() {
        XenonProto.FileAdaptorDescriptions response = client.getAdaptorDescriptions(XenonProto.Empty.getDefaultInstance());

        assertEquals(4, response.getDescriptionsCount());
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

    @Test
    public void getAttributes() throws IOException {
        String path = myfolder.getRoot().getAbsolutePath();
        XenonProto.Path request = getLocalPath(path);

        XenonProto.FileAttributes attribs = client.getAttributes(request);

        assertTrue("isDirectory", attribs.getIsDirectory());
        Path epath = myfolder.getRoot().toPath();
        assertEquals("owner", Files.getOwner(epath).getName(), attribs.getOwner());
        // TODO check other attribs
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
    public void setPosixFilePermissions() throws IOException {
        XenonProto.Path path = getLocalPath(myfolder.getRoot().getAbsolutePath());
        XenonProto.PosixFilePermissionsRequest request = XenonProto.PosixFilePermissionsRequest.newBuilder()
            .setPath(path)
            .addPermissions(XenonProto.PosixFilePermission.OWNER_READ)
            .addPermissions(XenonProto.PosixFilePermission.OWNER_WRITE)
            .addPermissions(XenonProto.PosixFilePermission.OWNER_EXECUTE)
            .build();

        client.setPosixFilePermissions(request);

        Set<PosixFilePermission> attribs = Files.getPosixFilePermissions(myfolder.getRoot().toPath());
        Set<PosixFilePermission> expected = new HashSet<>(Arrays.asList(
            PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.OWNER_EXECUTE
        ));
        assertEquals(expected, attribs);
    }
}