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
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;

public class HadoopPosixFileAttributeView implements PosixFileAttributeView {
	
	private final HadoopPath path;

	public HadoopPosixFileAttributeView(HadoopPath path) {
		this.path = path;
	}

	@Override
	public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime,
			FileTime createTime) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public UserPrincipal getOwner() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setOwner(UserPrincipal owner) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public String name() {
		return "posix";
	}

	@Override
	public PosixFileAttributes readAttributes() throws IOException {
		FileStatus fileStatus = path.getFileSystem().getHDFS().getFileStatus(path.getRawResolvedPath());
		return new HadoopPosixFileAttributes(this.path.getFileSystem(), fileStatus);
	}

	@Override
	public void setPermissions(Set<PosixFilePermission> perms)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setGroup(GroupPrincipal group) throws IOException {
		// TODO Auto-generated method stub

	}

}
