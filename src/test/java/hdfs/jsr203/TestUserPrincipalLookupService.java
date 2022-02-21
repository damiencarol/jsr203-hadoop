/*
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

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestUserPrincipalLookupService extends TestHadoop {

  private static MiniDFSCluster cluster;
  private static URI clusterUri;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    cluster = startMini(TestUserPrincipalLookupService.class.getName());
    clusterUri = formalizeClusterURI(cluster.getURI());
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
   * Test UserPrincipalLookupService support.
   * 
   * @throws IOException
   */
  @Test
  public void testGetPosixViewSetOwner() throws IOException {
    Path rootPath = Paths.get(clusterUri);

    UserPrincipalLookupService lus = rootPath.getFileSystem()
        .getUserPrincipalLookupService();
    assertNotNull(lus);
  }

  /**
   * Try to get user and set the same user in the root.
   * 
   * @throws IOException
   */
  @Test
  public void testGetSetUGI() throws IOException {
    Path rootPath = Paths.get(clusterUri);

    UserPrincipal user = Files.getOwner(rootPath);
    assertNotNull(user);

    Files.setOwner(rootPath, user);
  }

  @Test
  public void testUsersEquals() throws IOException {
    Path rootPath = Paths.get(clusterUri);

    UserPrincipal user = Files.getOwner(rootPath);
    assertNotNull(user);

    // Get the same user
    UserPrincipal user2 = Files.getOwner(rootPath);
    assertNotNull(user2);

    Assert.assertTrue(user.equals(user));
    Assert.assertTrue(user.equals(user2) && user2.equals(user));

    Assert.assertFalse(user.equals(null));
    Assert.assertFalse(user.equals(new Double(-1)));

    UserPrincipal userTest = rootPath.getFileSystem()
        .getUserPrincipalLookupService().lookupPrincipalByName("test");
    Assert.assertFalse(user.equals(userTest));
  }
  
  @Test
  public void testHashcode() throws IOException {
    Path rootPath = Paths.get(clusterUri);

    UserPrincipal user = Files.getOwner(rootPath);
    assertNotNull(user);

    // Get the same user
    UserPrincipal user2 = Files.getOwner(rootPath);
    assertNotNull(user2);
    // Test hash code
    Assert.assertTrue(user.hashCode() == user2.hashCode());
  }

  @Test
  public void testClone() throws IOException, CloneNotSupportedException {
    Path rootPath = Paths.get(clusterUri);

    UserPrincipal user = Files.getOwner(rootPath);
    assertNotNull(user);

    UserPrincipal user3 = (UserPrincipal) ((HadoopUserPrincipal) user).clone();
    Assert.assertTrue(user.equals(user3));
  }
}
