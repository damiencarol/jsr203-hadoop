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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class TestPath {
    private static MiniDFSCluster cluster;
    private static URI clusterUri;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        cluster = startMini(TestFileSystem.class.getName());
        clusterUri = cluster.getFileSystem().getUri();
    }

    @AfterClass
    public static void teardownClass() throws Exception {
        if (cluster != null)
        {
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
        Path p = Paths.get(clusterUri);
        Path q = p.resolve("tmp/testNormalize/dir1/../file.txt");

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
}
