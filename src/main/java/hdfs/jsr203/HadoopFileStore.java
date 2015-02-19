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

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;

public class HadoopFileStore extends FileStore {

	private HadoopFileSystem system;

	public HadoopFileStore(HadoopPath path) {
		this.system = path.getFileSystem();
	}

	@Override
	public String name() {
		return this.system.getHDFS().getCanonicalServiceName();
	}

	@Override
	public String type() {
		return "HDFS";
	}

	@Override
	public boolean isReadOnly() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public long getTotalSpace() throws IOException {
		// TODO Auto-generated method stub
		return this.system.getHDFS().getStatus().getCapacity();
	}

	@Override
	public long getUsableSpace() throws IOException {
		// TODO Auto-generated method stub
		return this.system.getHDFS().getStatus().getCapacity();
	}

	@Override
	public long getUnallocatedSpace() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean supportsFileAttributeView(
			Class<? extends FileAttributeView> type) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsFileAttributeView(String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <V extends FileStoreAttributeView> V getFileStoreAttributeView(
			Class<V> type) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getAttribute(String attribute) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
