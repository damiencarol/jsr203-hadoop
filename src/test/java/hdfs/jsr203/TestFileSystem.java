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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.ProviderMismatchException;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.nio.file.spi.FileSystemProvider;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFileSystem extends TestHadoop {

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
   * Check that a FileSystemProvider handle <code>hdfs</code> scheme.
   */
  @Test
  public void testAutoRegister() {

    boolean found = false;
    for (FileSystemProvider fp : FileSystemProvider.installedProviders())
      if (fp.getScheme().equals(HadoopFileSystemProvider.SCHEME))
        found = true;
    // Check auto register of the provider
    assertTrue(found);
  }

  @Test
  public void testProvider() throws URISyntaxException {
    URI uri = clusterUri.resolve("/tmp/testProvider");
    Path path = Paths.get(uri);
    assertNotNull(path.getFileSystem());
    assertNotNull(path.getFileSystem().provider());
  }

  @Test(expected = NoSuchFileException.class)
  public void testNoSuchFileExceptionOnDelete()
      throws URISyntaxException, IOException {
    // start the demo cluster
    // MiniDFSCluster cluster =
    // startMini("testNoSuchFileExceptionOnDelete");
    // URI uri = new URI("hdfs://" + host + ":" + cluster.getNameNodePort()
    // + "/tmp/test_file");

    URI uri = clusterUri.resolve("/tmp/testNoSuchFileExceptionOnDelete");
    Path path = Paths.get(uri);

    Assume.assumeTrue(!Files.exists(path));

    Files.createFile(path);
    assertTrue(Files.exists(path));
    Files.delete(path);
    assertFalse(Files.exists(path));

    try {
      Files.delete(path); // this one generate the exception
    } finally {
      // cluster.shutdown();
    }
  }

  @Test(expected = DirectoryNotEmptyException.class)
  public void testDirectoryNotEmptyExceptionOnDelete()
      throws URISyntaxException, IOException {
    // Create the directory
    URI uriDir = clusterUri
        .resolve("/tmp/testDirectoryNotEmptyExceptionOnDelete");
    Path pathDir = Paths.get(uriDir);
    // Check that directory doesn't exists
    if (Files.exists(pathDir)) {
      Files.delete(pathDir);
    }

    Files.createDirectory(pathDir);
    assertTrue(Files.exists(pathDir));
    // Create the file
    Path path = pathDir.resolve("test_file");
    Files.createFile(path);
    assertTrue(Files.exists(path));

    Files.delete(pathDir); // this one generate the exception
    assertFalse(Files.exists(path));

  }

  @Test
  public void testSetLastModifiedTime() throws URISyntaxException, IOException {
    URI uri = clusterUri.resolve("/tmp/testSetLastModifiedTime");
    Path file = Paths.get(uri);
    Files.createFile(file);
    assertTrue(Files.exists(file));
    BasicFileAttributes attr = Files.readAttributes(file,
        BasicFileAttributes.class);
    assertNotNull(attr);
    long currentTime = System.currentTimeMillis();
    FileTime ft = FileTime.fromMillis(currentTime);
    Files.setLastModifiedTime(file, ft);

    Files.delete(file);
  }

  @Test
  public void testOutputInput() throws URISyntaxException, IOException {
    URI uri = clusterUri.resolve("/tmp/testOutputInput");
    Path path = Paths.get(uri);

    String string_test = "Test !";
    OutputStream out = Files.newOutputStream(path);
    byte[] buf = string_test.getBytes();
    out.write(buf);
    out.flush();
    out.close();

    InputStream in = Files.newInputStream(path);
    byte[] buf2 = new byte[50];
    final int size = in.read(buf2, 0, buf.length);

    assertEquals("Content read from file is not equal to content written.",
        string_test, new String(buf2, 0, size));
  }

  @Test
  public void testTempFile() throws URISyntaxException, IOException {
    URI uri = clusterUri.resolve("/tmp/testTempFile");
    Path path = Paths.get(uri);
    Path tempFile = Files.createTempFile(path, null, ".myapp");
    Files.delete(tempFile);
  }

  @Test
  public void testDefaults() throws URISyntaxException, IOException {
    URI uri = clusterUri.resolve("/tmp/testDefaults");
    Path path = Paths.get(uri);
    Files.deleteIfExists(path);
  }

  @Test
  public void testLastModifiedTime() throws URISyntaxException, IOException {
    URI uri = clusterUri.resolve("/tmp/testLastModifiedTime");
    Path path = Paths.get(uri);
    Files.createDirectory(path);
    assertTrue(Files.exists(path));
    Files.getLastModifiedTime(path);
  }

  @Test
  public void testCheckRead() throws URISyntaxException, IOException {
    URI uri = clusterUri.resolve("/tmp/testCheckRead");
    Path path = Paths.get(uri);
    if (Files.exists(path))
      Files.delete(path);
    assertFalse(Files.exists(path));
    Files.createFile(path);
    assertTrue(Files.exists(path));
  }

  @Test
  public void testFileStore() throws URISyntaxException, IOException {
    URI uri = clusterUri.resolve("/tmp/testFileStore");
    Path path = Paths.get(uri);
    if (Files.exists(path))
      Files.delete(path);
    assertFalse(Files.exists(path));
    Files.createFile(path);
    assertTrue(Files.exists(path));
    FileStore st = Files.getFileStore(path);
    assertNotNull(st);
  }

  @Test(expected = NullPointerException.class)
  public void testNullPath() {
    HadoopFileSystemProvider.toHadoopPath(null);
  }

  @Test(expected = ProviderMismatchException.class)
  public void testMismatchedPath() {
    HadoopFileSystemProvider.toHadoopPath(Paths.get("my_file"));
  }

  /**
   * Simple test to check {@link PathMatcher} support.
   *
   * @throws IOException
   */
  @Test
  public void getPathMatcher() throws IOException {
    Path pathToTest = Paths.get(clusterUri);

    PathMatcher matcher = pathToTest.getFileSystem()
        .getPathMatcher("glob:*.{java,class}");

    assertTrue(
        matcher.matches(pathToTest.getFileSystem().getPath("test.java")));
  }

  /**
   * Simple test to check {@link PathMatcher} support.
   *
   * @throws IOException
   */
  @Test(expected = PatternSyntaxException.class)
  public void getPathMatcherMissingHandelbar() throws IOException {
    Path pathToTest = Paths.get(clusterUri);

    PathMatcher matcher = pathToTest.getFileSystem()
        .getPathMatcher("glob:*.{java,class");

    assertTrue(
        matcher.matches(pathToTest.getFileSystem().getPath("test.java")));
  }

  /**
   * Simple test to check {@link PathMatcher} support.
   *
   * @throws IOException
   */
  @Test
  public void getPathMatcherRegex() throws IOException {
    Path pathToTest = Paths.get(clusterUri);

    PathMatcher matcher = pathToTest.getFileSystem()
        .getPathMatcher("regex:.*\\.java");

    assertTrue(
        matcher.matches(pathToTest.getFileSystem().getPath("test.java")));
  }

  /**
   * Simple test to check {@link PathMatcher} support.
   *
   * @throws IOException
   */
  @Test(expected = IllegalArgumentException.class)
  public void getPathMatcherInvalid() throws IOException {
    Path pathToTest = Paths.get(clusterUri);

    pathToTest.getFileSystem().getPathMatcher("gloubiboulga");
  }

  /**
   * Simple test to check {@link PathMatcher} support.
   *
   * @throws IOException
   */
  @Test(expected = UnsupportedOperationException.class)
  public void getPathMatcherUnsupported() throws IOException {
    Path pathToTest = Paths.get(clusterUri);

    pathToTest.getFileSystem().getPathMatcher("gloubiboulga:.*\\.java");
  }

  /**
   * Simple test to check {@link PathMatcher} support.
   *
   * @throws IOException
   */
  @Test
  public void getPathMatcherMask() throws IOException {
    Path pathToTest = Paths.get(clusterUri);

    PathMatcher matcher = pathToTest.getFileSystem()
        .getPathMatcher("glob:????.{java,class}");

    assertTrue(
        matcher.matches(pathToTest.getFileSystem().getPath("test.java")));
  }

  /**
   * Simple test to check {@link FileStore} support.
   *
   * @throws IOException
   */
  @Test
  public void getFileStores() throws IOException {
    Path pathToTest = Paths.get(clusterUri);

    Iterable<FileStore> fileStores = pathToTest.getFileSystem().getFileStores();
    for (FileStore store : fileStores) {
      store.getUsableSpace();
      assertNotNull(store.toString());
    }
  }

  @Test
  public void getSeparator() throws IOException {
    Path pathToTest = Paths.get(clusterUri);

    assertNotNull(pathToTest.getFileSystem().getSeparator());
  }

  @Test
  public void getRootDirectories() throws IOException {
    Path pathToTest = Paths.get(clusterUri);

    assertNotNull(pathToTest.getFileSystem().getRootDirectories());
  }

  @Test
  public void testCopyAndMoveFiles() throws IOException {
    URI uriSrc = clusterUri.resolve("/tmp/testSrcFile");
    Path pathSrc = Paths.get(uriSrc);

    URI uriDstCp = clusterUri.resolve("/tmp/testDstCopyFile");
    Path pathDstCp = Paths.get(uriDstCp);

    OutputStream os = Files.newOutputStream(pathSrc);
    os.write("write \n several \n things\n".getBytes());
    os.close();
    Files.copy(pathSrc, pathDstCp);
    assertTrue(Files.exists(pathDstCp));
    assertEquals(Files.size(pathSrc), Files.size(pathDstCp));

    URI uriDstMv = clusterUri.resolve("/tmp/testDstMoveFile");
    Path pathDstMv = Paths.get(uriDstMv);
    Files.move(pathDstCp, pathDstMv);
    assertFalse(Files.exists(pathDstCp));// move from
    assertTrue(Files.exists(pathDstMv));// move to
    assertEquals(Files.size(pathSrc), Files.size(pathDstMv));
    Files.deleteIfExists(pathSrc);
    Files.deleteIfExists(pathDstMv);
  }

  @Test
  public void newWatchService() throws IOException {
    Path pathToTest = Paths.get(clusterUri);
    WatchService ws = pathToTest.getFileSystem().newWatchService();
    WatchKey key = pathToTest.register(ws, StandardWatchEventKinds.ENTRY_CREATE,
        StandardWatchEventKinds.ENTRY_DELETE,
        StandardWatchEventKinds.ENTRY_MODIFY);
    Assert.assertTrue(key.isValid());
  }

  @Test
  public void isReadOnly() {
    Paths.get(clusterUri).getFileSystem().isReadOnly();
  }

  @Test
  public void isOpen() {
    Paths.get(clusterUri).getFileSystem().isOpen();
  }

  @Test(expected=NullPointerException.class)
  public void invalidWatchService() throws IOException {
    Path pathToTest = Paths.get(clusterUri);
    // Register with null should throw exception
    WatchService watcher = null;
    pathToTest.register(watcher, StandardWatchEventKinds.ENTRY_CREATE);
  }

}
