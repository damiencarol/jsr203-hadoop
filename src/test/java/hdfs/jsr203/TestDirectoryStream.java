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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDirectoryStream extends TestHadoop {

  private static MiniDFSCluster cluster;
  private static URI clusterUri;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    cluster = startMini(TestDirectoryStream.class.getName());
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
   * According to Java documentation of NIO API, if we try to get an iterator two times,
   * there should be an <code>IllegalStateException</code>.
   * 
   * @throws IOException if we can't get the DirectoryStream
   */
  @Test(expected=IllegalStateException.class)
  public void testGetIteratorTwoTimes() throws IOException {
    Path dir = Paths.get(clusterUri);
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
      Assert.assertNotNull(stream.iterator());
      // This one throws the exception
      Assert.assertNotNull(stream.iterator());
    }
  }

  /**
   * According to Java documentation of NIO API, if we try to call <code>remove</code> on iterator,
   * there should be an exception.
   * <p>
   * Currently we use {@see UnsupportedOperationException} like OpenJDK.
   * 
   * @throws IOException if we can't get the DirectoryStream
   */
  @Test(expected=UnsupportedOperationException.class)
  public void testRemoveOnIterator() throws IOException {
    Path dir = Paths.get(clusterUri);
    // create file
    Path file = dir.resolve("foo");
    if (Files.exists(file)) {
      Files.delete(file);
    }
    Files.createFile(file);
    
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
      Assert.assertNotNull(stream.iterator());
      // This one throws the exception
      Assert.assertNotNull(stream.iterator());
      Iterator<Path> it = stream.iterator();
      it.next();
      it.remove();
    }
  }
}
