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
import java.nio.file.attribute.FileOwnerAttributeView;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

/**
 * Implementation of {@link FileOwnerAttributeView}
 */
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
        UserPrincipalLookupService ls = this.path.getFileSystem().getUserPrincipalLookupService();
        FileStatus fileStatus = path.getFileSystem().getHDFS().getFileStatus(path.getRawResolvedPath());
        return ls.lookupPrincipalByName(fileStatus.getOwner());
    }

    @Override
    public void setOwner(UserPrincipal owner) throws IOException {
        FileSystem fs = path.getFileSystem().getHDFS();
        fs.setOwner(path.getRawResolvedPath(), owner.getName(), null);
    }

}
