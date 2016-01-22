/*
 * Copyright 2016 Damien Carol <damien.carol@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package hdfs.jsr203;

import java.io.IOException;
import java.nio.file.LinkOption;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Implementation of {@link PosixFileAttributeView}.
 */
public class HadoopPosixFileAttributeView extends HadoopFileOwnerAttributeView
    implements PosixFileAttributeView, IAttributeReader, IAttributeWriter {

  private final HadoopPath path;
  /** posix or owner ? */
  private final boolean isPosixView;

  private static enum AttrID {
    owner, creationTime, lastAccessTime, lastModifiedTime, 
    isDirectory, isRegularFile, isSymbolicLink, isOther, fileKey,
    group, permissions
  };

  public HadoopPosixFileAttributeView(HadoopPath path, boolean isPosixView) {
    super(path);
    this.path = path;
    this.isPosixView = isPosixView;
  }

  @Override
  public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime,
      FileTime createTime) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public String name() {
    if (!isPosixView) {
      return super.name();
    }
    return "posix";
  }

  @Override
  public PosixFileAttributes readAttributes() throws IOException {
    Path resolvedPath = path.getRawResolvedPath();
    FileStatus fileStatus = path.getFileSystem().getHDFS()
        .getFileStatus(resolvedPath);
    String fileKey = resolvedPath.toString();
    return new HadoopPosixFileAttributes(this.path.getFileSystem(), fileKey,
        fileStatus);
  }

  @Override
  public void setPermissions(Set<PosixFilePermission> perms)
      throws IOException {
    throw new IOException("Not implemented");
  }

  @Override
  public void setGroup(GroupPrincipal group) throws IOException {
    FileSystem fs = path.getFileSystem().getHDFS();
    fs.setOwner(path.getRawResolvedPath(), null, group.getName());
  }

  @Override
  public Map<String, Object> readAttributes(String attributes,
      LinkOption[] options) throws IOException {
    PosixFileAttributes zfas = readAttributes();
    LinkedHashMap<String, Object> map = new LinkedHashMap<>();
    if ("*".equals(attributes)) {
      for (AttrID id : AttrID.values()) {
        try {
          map.put(id.name(), attribute(id, zfas));
        } catch (IllegalArgumentException x) {
        }
      }
    } else {
      String[] as = attributes.split(",");
      for (String a : as) {
        try {
          map.put(a, attribute(AttrID.valueOf(a), zfas));
        } catch (IllegalArgumentException x) {
        }
      }
    }
    return map;
  }

  @Override
  public void setAttribute(String attr, Object value, LinkOption[] options)
      throws IOException {
    // FIXME Implement HadoopPosixFileAttributeView.setAttribute()
    throw new UnsupportedOperationException();
  }

  Object attribute(AttrID id, PosixFileAttributes hfas) {
    switch (id) {
    case owner:
      return hfas.owner().getName();
    case group:
      return hfas.owner().getName();
    case permissions:
      return hfas.owner().getName();
    default:
      return null;
    }
  }
}
