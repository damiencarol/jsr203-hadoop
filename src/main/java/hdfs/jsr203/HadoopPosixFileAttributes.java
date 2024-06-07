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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.util.Set;

/**
 * Implement {@link PosixFileAttributes}.
 */
public class HadoopPosixFileAttributes extends HadoopBasicFileAttributes
    implements PosixFileAttributes {

  private final UserPrincipal owner;
  private final GroupPrincipal group;
  private final Set<PosixFilePermission> permissions;

  public HadoopPosixFileAttributes(HadoopFileSystem hdfs, Object fileKey,
                                   FileStatus fileStatus) throws IOException {
    super(fileKey, fileStatus);
    this.owner = hdfs.getUserPrincipalLookupService()
        .lookupPrincipalByGroupName(fileStatus.getOwner());
    this.group = hdfs.getUserPrincipalLookupService()
        .lookupPrincipalByGroupName(fileStatus.getGroup());
    FsPermission fsPermission = getFileStatus().getPermission();
    String perms = fsPermission.getUserAction().SYMBOL
        + fsPermission.getGroupAction().SYMBOL
        + fsPermission.getOtherAction().SYMBOL;
    this.permissions = PosixFilePermissions.fromString(perms);
  }

  @Override
  public UserPrincipal owner() {
    return owner;
  }

  @Override
  public GroupPrincipal group() {
    return group;
  }

  @Override
  public Set<PosixFilePermission> permissions() {
    return permissions;
  }
}
