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
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileTime;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class HadoopFileAttributeView implements FileAttributeView, IAttributeReader, IAttributeWriter
{
    private static enum AttrID {
        accessTime,
        blockSize,
        group,
        len,
        modificationTime,
        owner,
        replication,
        isDirectory,
        //isEncrypted, TODO enable encryption
        isFile,
        isSymLink
    };

    private final HadoopPath path;

    public HadoopFileAttributeView(HadoopPath path) {
        this.path = path;
    }

    @Override
    public String name() {
        return "hadoop";
    }

    public HadoopFileAttributes readAttributes() throws IOException
    {
        Path resolvedPath = path.getRawResolvedPath();
        FileStatus fileStatus = path.getFileSystem().getHDFS().getFileStatus(resolvedPath);
        String fileKey = resolvedPath.toString();
		return new HadoopFileAttributes(fileKey, fileStatus);
    }

    public void setTimes(FileTime lastModifiedTime,
                         FileTime lastAccessTime,
                         FileTime createTime)
        throws IOException
    {
        path.setTimes(lastModifiedTime, lastAccessTime, createTime);
    }

    @Override
    public void setAttribute(String attribute, Object value, LinkOption[] options)
        throws IOException
    {
        try {
            if (AttrID.valueOf(attribute) == AttrID.modificationTime)
                setTimes ((FileTime)value, null, null);
            if (AttrID.valueOf(attribute) == AttrID.accessTime)
                setTimes (null, (FileTime)value, null);
            //if (AttrID.valueOf(attribute) == AttrID.creationTime)
            //    setTimes (null, null, (FileTime)value);
            return;
        } catch (IllegalArgumentException x) {}
        throw new UnsupportedOperationException("'" + attribute +
            "' is unknown or read-only attribute");
    }

    @Override
    public Map<String, Object> readAttributes(String attributes, LinkOption[] options) throws IOException
    {
    	FileStatus fileStatus = path.getFileSystem().getHDFS().getFileStatus(path.getRawResolvedPath());
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        if ("*".equals(attributes)) {
            for (AttrID id : AttrID.values()) {
                try {
                    map.put(id.name(), attribute(id, fileStatus));
                } catch (IllegalArgumentException x) {}
            }
        } else {
            String[] as = attributes.split(",");
            for (String a : as) {
                try {
                    map.put(a, attribute(AttrID.valueOf(a), fileStatus));
                } catch (IllegalArgumentException x) {}
            }
        }
        return map;
    }

    Object attribute(AttrID id, FileStatus hfas) {
        switch (id) {
        case accessTime:
            return hfas.getAccessTime();
        case blockSize:
            return hfas.getBlockSize();
        case group:
            return hfas.getGroup();
        case len:
            return hfas.getLen();
        case modificationTime:
            return hfas.getModificationTime();
        case owner:
            return hfas.getOwner();
        case replication:
            return hfas.getReplication();
        case isDirectory:
            return hfas.isDirectory();
        // TODO enable encryption
        //case isEncrypted:
        //    return hfas.isEncrypted();
        case isFile:
            return hfas.isFile();
        case isSymLink:
            return hfas.isSymlink();
        }
        return null;
    }
}
