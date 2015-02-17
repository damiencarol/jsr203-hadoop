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
package hdfs.jsr203.attribute;

import hdfs.jsr203.HadoopPath;

import java.io.IOException;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;

import org.apache.hadoop.fs.FileStatus;

public class HadoopFileOwnerAttributeView implements FileOwnerAttributeView {
	
	private final HadoopPath path;

	public HadoopFileOwnerAttributeView(HadoopPath path) {
		this.path = path;
	}

	@Override
	public String name() {
		return "owner";
	}

	@Override
	public UserPrincipal getOwner() throws IOException {
		try {
			UserPrincipalLookupService ls = this.path.getFileSystem().getUserPrincipalLookupService();
			FileStatus fileStatus = path.getFileSystem().getHDFS().getFileStatus(path.getRawResolvedPath());
			return ls.lookupPrincipalByName(fileStatus.getOwner());
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public void setOwner(UserPrincipal owner) throws IOException {
		// TODO manage change of owner
		throw new IOException("Not implemented");
	}

}
