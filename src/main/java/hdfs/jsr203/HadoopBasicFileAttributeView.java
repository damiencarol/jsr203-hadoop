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
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileTime;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class HadoopBasicFileAttributeView implements BasicFileAttributeView, IAttributeReader, IAttributeWriter
{
    private static enum AttrID {
        size,
        creationTime,
        lastAccessTime,
        lastModifiedTime,
        isDirectory,
        isRegularFile,
        isSymbolicLink,
        isOther,
        fileKey,

        accessTime,
        blockSize,
        group,
        len,
        modificationTime,
        owner,
        replication,
        //isDirectory,
        isEncrypted,
        isFile,
        isSymLink
    };

    private final HadoopPath path;
    private final boolean isHadoopView;

    public HadoopBasicFileAttributeView(HadoopPath path, boolean isHadoopView) {
        this.path = path;
        this.isHadoopView = isHadoopView;
    }

    static HadoopBasicFileAttributeView get(HadoopPath path, String type) {
        if (type == null)
            throw new NullPointerException();
        if (type.equals("basic"))
            return new HadoopBasicFileAttributeView(path, false);
        if (type.equals("hadoop"))
            return new HadoopBasicFileAttributeView(path, true);
        return null;
    }

    @Override
    public String name() {
        return isHadoopView ? "hadoop" : "basic";
    }

    public HadoopBasicFileAttributes readAttributes() throws IOException
    {
        Path resolvedPath = path.getRawResolvedPath();
        FileStatus fileStatus = path.getFileSystem().getHDFS().getFileStatus(resolvedPath);
        String fileKey = resolvedPath.toString();
		return new HadoopBasicFileAttributes(fileKey,fileStatus);
    }

    @Override
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
            if (AttrID.valueOf(attribute) == AttrID.lastModifiedTime)
                setTimes ((FileTime)value, null, null);
            if (AttrID.valueOf(attribute) == AttrID.lastAccessTime)
                setTimes (null, (FileTime)value, null);
            if (AttrID.valueOf(attribute) == AttrID.creationTime)
                setTimes (null, null, (FileTime)value);
            return;
        } catch (IllegalArgumentException x) {}
        throw new UnsupportedOperationException("'" + attribute +
            "' is unknown or read-only attribute");
    }

    @Override
    public Map<String, Object> readAttributes(String attributes, LinkOption[] options) throws IOException
    {
        HadoopBasicFileAttributes zfas = readAttributes();
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

    Object attribute(AttrID id, HadoopBasicFileAttributes hfas) {
        if (isHadoopView) {
            switch (id) {
            case accessTime:
                return hfas.getFileStatus().getAccessTime();
            case blockSize:
                return hfas.getFileStatus().getBlockSize();
            case group:
                return hfas.getFileStatus().getGroup();
            case len:
                return hfas.getFileStatus().getLen();
            case modificationTime:
                return hfas.getFileStatus().getModificationTime();
            case owner:
                return hfas.getFileStatus().getOwner();
            case replication:
                return hfas.getFileStatus().getReplication();
            case isDirectory:
                return hfas.getFileStatus().isDirectory();
            case isEncrypted:
                return hfas.getFileStatus().isEncrypted();
            case isFile:
                return hfas.getFileStatus().isFile();
            case isSymLink:
                return hfas.getFileStatus().isSymlink();
            default:
                return null;
            }
        } else {
            switch (id) {
            case size:
                return hfas.size();
            case creationTime:
                return hfas.creationTime();
            case lastAccessTime:
                return hfas.lastAccessTime();
            case lastModifiedTime:
                return hfas.lastModifiedTime();
            case isDirectory:
                return hfas.isDirectory();
            case isRegularFile:
                return hfas.isRegularFile();
            case isSymbolicLink:
                return hfas.isSymbolicLink();
            case isOther:
                return hfas.isOther();
            case fileKey:
                return hfas.fileKey();
            default:
                return null;
            }
        }
    }
}
