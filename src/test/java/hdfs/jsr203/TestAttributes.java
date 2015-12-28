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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAttributes extends TestHadoop {

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
	 * Simple test to get all attributes.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testWriteBuffered() throws IOException {
		Path pathToTest = Paths.get(clusterUri);

		Set<String> sup = pathToTest.getFileSystem()
				.supportedFileAttributeViews();

		for (String viewID : sup) {
			for (Entry<String, Object> item : Files.readAttributes(pathToTest,
					viewID + ":*", LinkOption.NOFOLLOW_LINKS).entrySet()) {
				assertNotNull(item.getKey());
			}
		}
	}

	/**
	 * Simple test to get attributes.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testReadAttributes() throws IOException {
		Path pathToTest = Paths.get(clusterUri);

		// Read all basic-file-attributes.
		assertNotNull(Files.readAttributes(pathToTest, "*",
				LinkOption.NOFOLLOW_LINKS));
		// Reads the file size, last modified time, and last access time
		// attributes.
		assertNotNull(Files.readAttributes(pathToTest,
				"size,lastModifiedTime,lastAccessTime",
				LinkOption.NOFOLLOW_LINKS));
		// Read all POSIX-file-attributes.
		assertNotNull(Files.readAttributes(pathToTest, "posix:*",
				LinkOption.NOFOLLOW_LINKS));
		// Reads the POSX file permissions, owner, and file size.
		assertNotNull(Files.readAttributes(pathToTest,
				"posix:permissions,owner,size", LinkOption.NOFOLLOW_LINKS));
	}

    /**
     * Simple test to get attributes by string.
     * 
     * @throws IOException
     */
    @Test
    public void testReadAttribute() throws IOException {
        Path pathToTest = Paths.get(clusterUri);

        // Read all basic-file-attributes.
        assertNotNull(Files.getAttribute(pathToTest, "basic:creationTime",
                LinkOption.NOFOLLOW_LINKS));
        assertNotNull(Files.getAttribute(pathToTest, "basic:fileKey",
                LinkOption.NOFOLLOW_LINKS));
        assertNotNull(Files.getAttribute(pathToTest, "basic:isDirectory",
                LinkOption.NOFOLLOW_LINKS));
        assertNotNull(Files.getAttribute(pathToTest, "basic:isRegularFile",
                LinkOption.NOFOLLOW_LINKS));
        assertNotNull(Files.getAttribute(pathToTest, "basic:isSymbolicLink",
                LinkOption.NOFOLLOW_LINKS));
        assertNotNull(Files.getAttribute(pathToTest, "basic:isOther",
                LinkOption.NOFOLLOW_LINKS));
        assertNotNull(Files.getAttribute(pathToTest, "basic:lastAccessTime",
                LinkOption.NOFOLLOW_LINKS));
        assertNotNull(Files.getAttribute(pathToTest, "basic:lastModifiedTime",
                LinkOption.NOFOLLOW_LINKS));
        assertNotNull(Files.getAttribute(pathToTest, "basic:size",
                LinkOption.NOFOLLOW_LINKS));
    }
}
