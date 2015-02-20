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

public class TestAttributes {

	private static MiniDFSCluster cluster;
	private static URI clusterUri;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		cluster = startMini(TestFileSystem.class.getName());
		clusterUri = cluster.getFileSystem().getUri();
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
}
