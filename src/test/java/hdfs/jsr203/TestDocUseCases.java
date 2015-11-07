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

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestDocUseCases extends TestHadoop {

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

	@Test
	public void testWriteBufferedAndRead() throws IOException {
		Path rootPath = Paths.get(clusterUri);

		Path file = rootPath.resolve(rootPath.resolve("temp_file.txt"));

		Charset charset = Charset.forName("US-ASCII");
		String s = "this is a test";
		BufferedWriter writer = Files.newBufferedWriter(file, charset);
		writer.write(s, 0, s.length());
		writer.close();

		List<String> lines = Files.readAllLines(file, charset);
		assertEquals(1, lines.size());
		assertEquals(s, lines.get(0));

		// test positioned reads
		int offset = 8;
		byte[] contents = new byte[s.length() - offset];
		ByteBuffer buffer = ByteBuffer.wrap(contents);
		SeekableByteChannel seekableByteChannel = Files.newByteChannel(file);
		seekableByteChannel.position(offset);
		int read = seekableByteChannel.read(buffer);
		assertEquals(s.length() - offset, read);
		assertEquals("a test", new String(contents, charset));
	}

}
