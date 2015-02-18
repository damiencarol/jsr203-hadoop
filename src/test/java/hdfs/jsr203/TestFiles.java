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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFileAttributeView;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFiles {
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
    public void testGetLastModifiedTime() throws IOException {
        Path rootPath = Paths.get(clusterUri);

        FileTime ft = Files.getLastModifiedTime(rootPath, LinkOption.NOFOLLOW_LINKS);
        
        assertNotNull(ft);
        
    }

    /**
     * Test 'basic' file view support.
     * @throws IOException
     */
    @Test
    public void testGetBasicFileAttributeView() throws IOException {
        Path rootPath = Paths.get(clusterUri);
        
        assertTrue(rootPath.getFileSystem().supportedFileAttributeViews().contains("basic"));

        // Get root view
    	BasicFileAttributeView view = Files.getFileAttributeView(rootPath, 
    			BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
    	
    	assertNotNull(view);
    	assertNotNull(view.readAttributes());
    	assertNotNull(view.readAttributes().lastModifiedTime());
    }

    /**
     * Test 'posix' file view support.
     * @throws IOException
     */
    @Test
    public void testGetPosixFileAttributeView() throws IOException {
        Path rootPath = Paths.get(clusterUri);
        
        assertTrue(rootPath.getFileSystem().supportedFileAttributeViews().contains("posix"));

        // Get root view
    	PosixFileAttributeView view = Files.getFileAttributeView(rootPath, 
    			PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
    	
    	assertNotNull(view);
    	assertNotNull(view.readAttributes());
    	assertNotNull(view.readAttributes().lastModifiedTime());
    }

    /**
     * Test 'hadoop' file view support.
     * 
     * Also test that 'blockSize', 'len' and 'replication' are supported.
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
}
