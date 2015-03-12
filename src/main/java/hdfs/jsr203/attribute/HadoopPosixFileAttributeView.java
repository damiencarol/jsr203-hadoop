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
import java.nio.file.LinkOption;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

public class HadoopPosixFileAttributeView implements PosixFileAttributeView, IAttributeReader, IAttributeWriter {
	
	private final HadoopPath path;
	/** posix or owner ? */
	private final boolean isPosixView;
	
	private static enum AttrID {
        owner,
        creationTime,
        lastAccessTime,
        lastModifiedTime,
        isDirectory,
        isRegularFile,
        isSymbolicLink,
        isOther,
        fileKey
    };

	public HadoopPosixFileAttributeView(HadoopPath path, boolean isPosixView) {
		this.path = path;
		this.isPosixView = isPosixView;
	}

	@Override
	public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime,
			FileTime createTime) throws IOException {
		// TODO Auto-generated method stub

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
		FileSystem fs = path.getFileSystem().getHDFS();
		fs.setOwner(path.getRawResolvedPath(), owner.getName(), null);
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
		FileSystem fs = path.getFileSystem().getHDFS();
		fs.setOwner(path.getRawResolvedPath(), null, group.getName());
	}

	@Override
    public Map<String, Object> readAttributes(String attributes, LinkOption[] options) throws IOException
    {
        PosixFileAttributes zfas = readAttributes();
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        if ("*".equals(attributes)) {
            for (AttrID id : AttrID.values()) {
                try {
                    map.put(id.name(), attribute(id, zfas));
                } catch (IllegalArgumentException x) {}
            }
        } else {
            String[] as = attributes.split(",");
            for (String a : as) {
                try {
                    map.put(a, attribute(AttrID.valueOf(a), zfas));
                } catch (IllegalArgumentException x) {}
            }
        }
        return map;
    }

	@Override
	public void setAttribute(String attr, Object value, LinkOption[] options)
			throws IOException {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}
	
	Object attribute(AttrID id, PosixFileAttributes hfas) {
        switch (id) {
        case owner:
            return hfas.owner().getName();
            
       /*case blockSize:
            if (isPosixView)
                return 0;//hfas.getFileStatus().getBlockSize();
            break;*/
        }
        return null;
    }
}
