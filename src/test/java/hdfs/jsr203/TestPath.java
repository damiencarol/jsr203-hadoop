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
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TestPath extends TestHadoop {

    private static MiniDFSCluster cluster;
    private static URI clusterUri;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        cluster = startMini(TestPath.class.getName());
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
    public void testNormalize() {
        Path rootPath = Paths.get(clusterUri);
        Path start = rootPath.resolve("/tmp/testNormalize/dir1/../file.txt");
        Path expected = rootPath.resolve("/tmp/testNormalize/file.txt");

        assertEquals("Normalized path is incorrect.", expected.toString(), start.normalize().toString());
    }

    /**
     * Assertion to check that :
     * <code>p.relativize(p.resolve(q)).equals(q)</code>
     */
    @Test
    public void testRelativizeResolveCombination() {
        Path p = Paths.get(clusterUri).resolve("/a/b");
        Path q = p.getFileSystem().getPath("c", "d");

        assertEquals(q, p.relativize(p.resolve(q)));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testToFile() {
        Path rootPath = Paths.get(clusterUri);
        Path file = rootPath.resolve("/tmp/testToFile/file.txt");
        file.toFile();
    }

    @Test
    public void testCompareTo() {
        Path rootPath = Paths.get(clusterUri);
        Path path1 = rootPath.resolve("file1.txt");
        Path path2 = rootPath.resolve("file2.txt");

        assertThat(path2, Matchers.greaterThan(path1));
    }

    /**
     * Assertion to check that :
     * <code>Paths.get(p.toUri()).equals(p.toAbsolutePath()) </code>
     * 
     * @throws IOException
     */
    @Test
    public void testToURItoAbsolutePathCombination() throws IOException {
        Path rootPath = Paths.get(clusterUri);

        Files.createDirectories(rootPath.resolve("tmp/testNormalize/dir1/"));
        Files.createFile(rootPath.resolve("tmp/testNormalize/file.txt"));

        Path p = rootPath.resolve("tmp/testNormalize/dir1/../file.txt");
        assertTrue(Paths.get(p.toUri()).equals(p.toAbsolutePath()));

        p = rootPath.resolve("tmp/testNormalize/");
        assertTrue(Paths.get(p.toUri()).equals(p.toAbsolutePath()));
        p = rootPath.resolve("tmp/testNormalize");
        assertTrue(Paths.get(p.toUri()).equals(p.toAbsolutePath()));
        p = rootPath.resolve("tmp/testNormalize/dir1");
        assertTrue(Paths.get(p.toUri()).equals(p.toAbsolutePath()));
        p = rootPath.resolve("tmp/testNormalize/dir1/");
        assertTrue(Paths.get(p.toUri()).equals(p.toAbsolutePath()));
    }

    @Test
    public void testSubpath() throws IOException, URISyntaxException {
        Path rootPath = Paths.get(clusterUri);

        Files.createDirectories(rootPath.resolve("tmp/testNormalize/dir1/"));

        Path p = rootPath.resolve("tmp/testNormalize/dir1/");

        assertEquals("tmp/testNormalize", p.subpath(0, p.getNameCount() - 1).toString());
    }

    @Test
    public void testGetNameCount() throws IOException {
        Path rootPath = Paths.get(clusterUri);

        Files.createDirectories(rootPath.resolve("tmp/testNormalize/dir1/"));

        Path p = rootPath.resolve("tmp/testNormalize/dir1/");

        assertEquals(0, rootPath.getNameCount());
        assertEquals(3, p.getNameCount());
    }

    @Test
    public void testResolveSibling() throws IOException {
        Path rootPath = Paths.get(clusterUri);

        Files.createDirectories(rootPath.resolve("tmp/testNormalize/dir1/"));

        Path p = rootPath.resolve("tmp/testNormalize/test");
        assertEquals(p, p.resolveSibling(p.getFileName()));
    }

    @Test
    public void getName() throws IOException {
        Path rootPath = Paths.get(clusterUri);

        Path p = rootPath.resolve("tmp/testNormalize/test");
        assertEquals("tmp", p.getName(0).toString());
        assertEquals("testNormalize", p.getName(1).toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void getNameInvalidIndex() throws IOException {
        Path rootPath = Paths.get(clusterUri);

        Path p = rootPath.resolve("tmp/testNormalize/test");
        p.getName(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getNameInvalidIndex2() throws IOException {
        Path rootPath = Paths.get(clusterUri);

        Path p = rootPath.resolve("tmp/testNormalize/test");
        p.getName(p.getNameCount());
    }

    @Test
    public void startsWith() throws IOException {
        Path rootPath = Paths.get(clusterUri);

        Path p = rootPath.resolve("tmp/testNormalize/test");
        assertTrue(p.startsWith("/tmp"));
    }

    @Test
    public void endsWith() throws IOException {
        Path rootPath = Paths.get(clusterUri);

        Path p = rootPath.resolve("tmp/testNormalize/test");
        assertTrue(p.endsWith("test"));
    }

    @Test
    public void getRoot() throws IOException {
        Path rootPath = Paths.get(clusterUri);

        Path p = rootPath.resolve("tmp/testNormalize/test");
        assertEquals(rootPath, p.getRoot());
    }

    @Test
    public void iterator() throws IOException {
        Path rootPath = Paths.get(clusterUri);

        Path p = rootPath.resolve("tmp/testNormalize/test");
        Iterator<Path> it = p.iterator();
        assertNotNull(it);
        assertEquals("tmp", it.next().toString());
        assertEquals("testNormalize", it.next().toString());
        assertEquals("test", it.next().toString());
    }
}
