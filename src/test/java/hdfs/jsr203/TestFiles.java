/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hdfs.jsr203;

import static java.nio.file.FileVisitResult.CONTINUE;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.UserPrincipal;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFiles extends TestHadoop {

    private static MiniDFSCluster cluster;
    private static URI clusterUri;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        cluster = startMini(TestFileSystem.class.getName());
        clusterUri = formalizeClusterURI(cluster.getFileSystem().getUri());
    }

    @AfterClass
    public static void teardownClass() throws Exception {
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    private static MiniDFSCluster startMini(String testName) throws IOException {
        File baseDir = new File("./target/hdfs/" + testName).getAbsoluteFile();
        FileUtil.fullyDelete(baseDir);
        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        MiniDFSCluster hdfsCluster = builder.clusterId(testName).build();
        hdfsCluster.waitActive();
        return hdfsCluster;
    }

    /**
     * Test for
     * {@link Files#createDirectories(Path, java.nio.file.attribute.FileAttribute...)
     * Files.createDirectories()}.
     * 
     * @throws IOException
     */
    @Test(expected = DirectoryNotEmptyException.class)
    public void testCreateDirectories() throws IOException {
        Path rootPath = Paths.get(clusterUri);

        Path dir = rootPath.resolve(rootPath.resolve("tmp/1/2/3/4/5"));

        Path dir2 = Files.createDirectories(dir);
        assertTrue(Files.exists(dir2));

        Files.delete(rootPath.resolve("tmp/1/2/3/4/5"));
        Files.delete(rootPath.resolve("tmp/1/2/3/4"));
        Files.delete(rootPath.resolve("tmp/1/2/3"));
        Files.delete(rootPath.resolve("tmp/1/2"));
        // Throws
        Files.delete(rootPath.resolve("tmp"));
    }

    /**
     * Test for {@link Files#isReadable(Path) Files.isReadable()}.
     * 
     * @throws IOException
     */
    @Test
    public void testIsReadable() throws IOException {
        Path rootPath = Paths.get(clusterUri);

        Path temp = Files.createTempFile(rootPath, "isReadable", "");
        assertTrue(Files.exists(temp));

        assertTrue(Files.isReadable(temp));
    }

    @Test
    public void testGetLastModifiedTime() throws IOException {
        Path rootPath = Paths.get(clusterUri);

        FileTime ft = Files.getLastModifiedTime(rootPath, LinkOption.NOFOLLOW_LINKS);

        assertNotNull(ft);

    }

    /**
     * Test 'basic' file view support.
     * 
     * @throws IOException
     */
    @Test
    public void testGetBasicFileAttributeView() throws IOException {
        Path rootPath = Paths.get(clusterUri);

        assertTrue(rootPath.getFileSystem().supportedFileAttributeViews().contains("basic"));

        // Get root view
        BasicFileAttributeView view = Files.getFileAttributeView(rootPath, BasicFileAttributeView.class,
                LinkOption.NOFOLLOW_LINKS);

        assertNotNull(view);
        assertNotNull(view.readAttributes());
        assertNotNull(view.readAttributes().lastModifiedTime());
    }

    /**
     * Test {@code posix} file view support.
     * 
     * @throws IOException
     */
    @Test
    public void testGetPosixFileAttributeView() throws IOException {
        Path rootPath = Paths.get(clusterUri);

        assertTrue(rootPath.getFileSystem().supportedFileAttributeViews().contains("posix"));

        // Get root view
        PosixFileAttributeView view = Files.getFileAttributeView(rootPath, PosixFileAttributeView.class,
                LinkOption.NOFOLLOW_LINKS);

        assertNotNull(view);
        assertNotNull(view.readAttributes());
        assertNotNull(view.readAttributes().lastModifiedTime());
    }

    /**
     * Test read attributes in {@code hadoop} file view.
     * 
     * <p>
     * Also test that {@code hadoop:blockSize}, {@code hadoop:len} and
     * {@code hadoop:replication} are supported.
     * 
     * @throws IOException
     */
    @Test
    public void testGetHadoopFileAttributeView() throws IOException {
        Path rootPath = Paths.get(clusterUri);

        assertTrue(rootPath.getFileSystem().supportedFileAttributeViews().contains("hadoop"));

        assertNotNull(Files.getAttribute(rootPath, "hadoop:blockSize", LinkOption.NOFOLLOW_LINKS));
        assertNotNull(Files.getAttribute(rootPath, "hadoop:len", LinkOption.NOFOLLOW_LINKS));
        assertNotNull(Files.getAttribute(rootPath, "hadoop:replication", LinkOption.NOFOLLOW_LINKS));
    }

    /**
     * Test {@link java.nio.file.attribute.BasicFileAttributeView
     * BasicFileAttributeView} support.
     * 
     * @throws IOException
     */
    @Test
    public void testGetAttributeBasic() throws IOException {
        Path rootPath = Paths.get(clusterUri);

        assertTrue(rootPath.getFileSystem().supportedFileAttributeViews().contains("basic"));

        assertNotNull(Files.getAttribute(rootPath, "basic:lastModifiedTime", LinkOption.NOFOLLOW_LINKS));
        assertNotNull(Files.getAttribute(rootPath, "basic:lastAccessTime", LinkOption.NOFOLLOW_LINKS));
        assertNotNull(Files.getAttribute(rootPath, "basic:creationTime", LinkOption.NOFOLLOW_LINKS));
        assertNotNull(Files.getAttribute(rootPath, "basic:size", LinkOption.NOFOLLOW_LINKS));
        assertNotNull(Files.getAttribute(rootPath, "basic:isRegularFile", LinkOption.NOFOLLOW_LINKS));
        assertNotNull(Files.getAttribute(rootPath, "basic:isDirectory", LinkOption.NOFOLLOW_LINKS));
        assertNotNull(Files.getAttribute(rootPath, "basic:isSymbolicLink", LinkOption.NOFOLLOW_LINKS));
        assertNotNull(Files.getAttribute(rootPath, "basic:isOther", LinkOption.NOFOLLOW_LINKS));
        assertNotNull(Files.getAttribute(rootPath, "basic:fileKey", LinkOption.NOFOLLOW_LINKS));
    }

    /**
     * Test owner in posix file view support.
     * 
     * @throws IOException
     */
    @Test
    public void testGetPosixView() throws IOException {
        Path rootPath = Paths.get(clusterUri);

        assertTrue(rootPath.getFileSystem().supportedFileAttributeViews().contains("posix"));
        PosixFileAttributeView view = Files.getFileAttributeView(rootPath, PosixFileAttributeView.class,
                LinkOption.NOFOLLOW_LINKS);
        assertNotNull(view);
        UserPrincipal user = view.getOwner();
        assertNotNull(user);
        assertNotNull(user.getName());
    }

    /**
     * Test create existing file with StandardOpenOption.CREATE_NEW throws a
     * <code>FileAlreadyExistsException</code>
     * 
     * @throws IOException
     */
    @Test(expected = FileAlreadyExistsException.class)
    public void testCreateFileThrowsFileAlreadyExistsException() throws IOException {
        Path rootPath = Paths.get(clusterUri);

        Path pathToTest = rootPath.resolve("tmp/out6.txt");

        Files.createFile(pathToTest);
        Files.createFile(pathToTest);
        Files.createFile(pathToTest);
        Files.createFile(pathToTest);
    }
    

    /**
     * A {@code FileVisitor} that finds
     * all files that match the
     * specified pattern.
     */
    public static class Finder
        extends SimpleFileVisitor<Path> {

        private final PathMatcher matcher;
        private int numMatches = 0;

        Finder(String pattern) {
            matcher = FileSystems.getDefault()
                    .getPathMatcher("glob:" + pattern);
        }

        // Compares the glob pattern against
        // the file or directory name.
        void find(Path file) {
            Path name = file.getFileName();
            if (name != null && matcher.matches(name)) {
                numMatches++;
                System.out.println(file);
            }
        }

        // Prints the total number of
        // matches to standard out.
        void done() {
            System.out.println("Matched: "
                + numMatches);
        }

        // Invoke the pattern matching
        // method on each file.
        @Override
        public FileVisitResult visitFile(Path file,
                BasicFileAttributes attrs) {
            find(file);
            return CONTINUE;
        }

        // Invoke the pattern matching
        // method on each directory.
        @Override
        public FileVisitResult preVisitDirectory(Path dir,
                BasicFileAttributes attrs) {
            find(dir);
            return CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file,
                IOException exc) {
            System.err.println(exc);
            return CONTINUE;
        }
    }
    
    /**
     * Simple test to visit directories.
     *
     * @throws IOException
     */
    @Test
    public void walkFileTree() throws IOException {
        Path pathToTest = Paths.get(clusterUri);
        EnumSet<FileVisitOption> opts = EnumSet.of(FileVisitOption.FOLLOW_LINKS);

        String pattern = "*";
        Finder finder = new Finder(pattern);
        Files.walkFileTree(pathToTest, opts, Integer.MAX_VALUE, finder);
    }

    @Test
    public void isHidden() throws IOException {
        Path rootPath = Paths.get(clusterUri);
        Path path = Files.createTempFile(rootPath, "test", "tmp");
        Assert.assertFalse(Files.isHidden(path));
        Path path2 = Files.createTempFile(rootPath, ".", "tmp");
        Assert.assertTrue(Files.isHidden(path2));
    }

    @Test
    public void isSameFile() throws IOException {
        Path rootPath = Paths.get(clusterUri);
        Path path = Files.createTempFile(rootPath, "test", "tmp");
        Assert.assertTrue(Files.isSameFile(path, path));
    }

    @Test
    public void setAttribute() throws IOException {
        Path rootPath = Paths.get(clusterUri);
        Path path = Files.createTempFile(rootPath, "test", "tmp");
        Object att = Files.getAttribute(path, "hadoop:replication");
        Assert.assertNotNull(att);
        Files.setAttribute(path, "hadoop:replication", att);
    }

    @Test
    public void getHadoopViewAttributes() throws IOException {
        Path rootPath = Paths.get(clusterUri);
        Path path = Files.createTempFile(rootPath, "test", "tmp");
        Assert.assertNotNull(Files.getAttribute(path, "hadoop:blockSize"));
        Assert.assertNotNull(Files.getAttribute(path, "hadoop:len"));
        Assert.assertNotNull(Files.getAttribute(path, "hadoop:replication"));
        Assert.assertNotNull(Files.getAttribute(path, "hadoop:isEncrypted"));
    }
}
