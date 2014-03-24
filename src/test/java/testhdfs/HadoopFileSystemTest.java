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
package testhdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import junit.framework.TestCase;

public class HadoopFileSystemTest extends TestCase {

	private static final int port = 8020;
	public static String host = "nc-h04";




	public void testDefaults() throws URISyntaxException, IOException {
		URI uri = new URI("hdfs://" + host + ":" + port + "/tmp/toto");
		Path path = Paths.get(uri);
		Files.deleteIfExists(path);
	}

	public void testLastModifiedTime() throws URISyntaxException, IOException {
		URI uri = new URI("hdfs://" + host + ":" + port + "/tmp/toto");
		Path path = Paths.get(uri);
		Files.createDirectory(path);
		assertTrue(Files.exists(path));
		Files.getLastModifiedTime(path);
	}
	
	public void testCheckRead() throws URISyntaxException, IOException {
		URI uri = new URI("hdfs://" + host + ":" + port + "/tmp/test_file");
		Path path = Paths.get(uri);
		if (Files.exists(path))
			Files.delete(path);
		assertFalse(Files.exists(path));
		Files.createFile(path);
		assertTrue(Files.exists(path));
	}
	
	public void testFileStore() throws URISyntaxException, IOException {
		URI uri = new URI("hdfs://" + host + ":" + port + "/tmp/test_file");
		Path path = Paths.get(uri);
		if (Files.exists(path))
			Files.delete(path);
		assertFalse(Files.exists(path));
		Files.createFile(path);
		assertTrue(Files.exists(path));
		FileStore st = Files.getFileStore(path);
		assertNotNull(st);
	}
	
	
	
}
