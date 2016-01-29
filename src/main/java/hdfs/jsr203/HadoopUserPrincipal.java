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

import java.nio.file.attribute.UserPrincipal;
import java.util.Arrays;

/**
 * Implement {@link UserPrincipal}.
 */
public class HadoopUserPrincipal implements UserPrincipal {

  private org.apache.hadoop.security.UserGroupInformation ugi;
  private final String name;

  public HadoopUserPrincipal(HadoopFileSystem hdfs, String name) {
    this.ugi = org.apache.hadoop.security.UserGroupInformation
        .createRemoteUser(name);
    this.name = name;
  }

  @Override
  public String getName() {
    return this.ugi.getUserName();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (obj == null || getClass() != obj.getClass()) {
      return false;
    } else if (!this.ugi.getUserName()
        .equals(((HadoopUserPrincipal) obj).ugi.getUserName())
        || !Arrays.equals(this.ugi.getGroupNames(),
            ((HadoopUserPrincipal) obj).ugi.getGroupNames())) {
      return false;
    } else {
      return true;
    }
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    return new HadoopUserPrincipal(null, name);
  }
}
