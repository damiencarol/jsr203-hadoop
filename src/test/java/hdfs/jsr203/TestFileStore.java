/*
 * Copyright 2016 Damien Carol <damien.carol@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package hdfs.jsr203;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.DosFileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.UserDefinedFileAttributeView;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFileStore extends TestHadoop {

  private static MiniDFSCluster cluster;
  private static URI clusterUri;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    cluster = startMini(TestFileStore.class.getName());
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
    Assert.assertNotNull(st.name());
    Assert.assertNotNull(st.type());

    Assert.assertFalse(st.isReadOnly());

    Assert.assertNotEquals(0, st.getTotalSpace());
    Assert.assertNotEquals(0, st.getUnallocatedSpace());
    Assert.assertNotEquals(0, st.getUsableSpace());

    Assert
        .assertTrue(st.supportsFileAttributeView(BasicFileAttributeView.class));
    Assert.assertTrue(st.supportsFileAttributeView("basic"));

    st.getAttribute("test");
  }

  /**
   * Test: File and FileStore attributes
   */
  @Test
  public void testFileStoreAttributes() throws URISyntaxException, IOException {
    URI uri = clusterUri.resolve("/tmp/testFileStore");
    Path path = Paths.get(uri);
    if (Files.exists(path))
      Files.delete(path);
    assertFalse(Files.exists(path));
    Files.createFile(path);
    assertTrue(Files.exists(path));
    FileStore store1 = Files.getFileStore(path);
    assertNotNull(store1);
    assertTrue(store1.supportsFileAttributeView("basic"));
    assertTrue(store1.supportsFileAttributeView(BasicFileAttributeView.class));
    assertTrue(store1.supportsFileAttributeView("posix") == store1
        .supportsFileAttributeView(PosixFileAttributeView.class));
    assertTrue(store1.supportsFileAttributeView("dos") == store1
        .supportsFileAttributeView(DosFileAttributeView.class));
    assertTrue(store1.supportsFileAttributeView("acl") == store1
        .supportsFileAttributeView(AclFileAttributeView.class));
    assertTrue(store1.supportsFileAttributeView("user") == store1
        .supportsFileAttributeView(UserDefinedFileAttributeView.class));
  }

  public class FakeFileStoreAttributeView implements FileStoreAttributeView {
    @Override
    public String name() {
      return "fake";
    }

  }

  @Test
  public void testHadoopFileStoreAttributeView() throws IOException {
    URI uri = clusterUri.resolve("/tmp/testFileStore");
    Path path = Paths.get(uri);
    if (Files.exists(path))
      Files.delete(path);
    assertFalse(Files.exists(path));
    Files.createFile(path);
    assertTrue(Files.exists(path));
    FileStore store1 = Files.getFileStore(path);
    assertNotNull(store1);
    HadoopFileStoreAttributeView view = store1
        .getFileStoreAttributeView(HadoopFileStoreAttributeView.class);
    Assert.assertNotNull(view);
    Assert.assertEquals("hadoop", view.name());

    Assert.assertNull(
        store1.getFileStoreAttributeView(FakeFileStoreAttributeView.class));
  }
}
