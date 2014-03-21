package hdfs.jsr203;

import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileTime;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;

public class HadoopFileAttributeView implements BasicFileAttributeView
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
        
        blockSize,
        replication
    };

    private final HadoopPath path;
    private final boolean isHadoopView;

    private HadoopFileAttributeView(HadoopPath path, boolean isHadoopView) {
        this.path = path;
        this.isHadoopView = isHadoopView;
    }

    static <V extends FileAttributeView> V get(HadoopPath path, Class<V> type) {
        if (type == null)
            throw new NullPointerException();
        if (type == BasicFileAttributeView.class)
            return (V)new HadoopFileAttributeView(path, false);
        if (type == HadoopFileAttributeView.class)
            return (V)new HadoopFileAttributeView(path, true);
        return null;
    }

    static HadoopFileAttributeView get(HadoopPath path, String type) {
        if (type == null)
            throw new NullPointerException();
        if (type.equals("basic"))
            return new HadoopFileAttributeView(path, false);
        if (type.equals("hadoop"))
            return new HadoopFileAttributeView(path, true);
        return null;
    }

    @Override
    public String name() {
        return isHadoopView ? "hadoop" : "basic";
    }

    public HadoopFileAttributes readAttributes() throws IOException
    {
        FileStatus fileStatus = path.getFileSystem().getHDFS().getFileStatus(path.getRawResolvedPath());
		return new HadoopFileAttributes(fileStatus );
    }

    @Override
    public void setTimes(FileTime lastModifiedTime,
                         FileTime lastAccessTime,
                         FileTime createTime)
        throws IOException
    {
        path.setTimes(lastModifiedTime, lastAccessTime, createTime);
    }

    void setAttribute(String attribute, Object value)
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

    Map<String, Object> readAttributes(String attributes)
        throws IOException
    {
        HadoopFileAttributes zfas = readAttributes();
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

    Object attribute(AttrID id, HadoopFileAttributes zfas) {
        switch (id) {
        case size:
            return zfas.size();
        case creationTime:
            return zfas.creationTime();
        case lastAccessTime:
            return zfas.lastAccessTime();
        case lastModifiedTime:
            return zfas.lastModifiedTime();
        case isDirectory:
            return zfas.isDirectory();
        case isRegularFile:
            return zfas.isRegularFile();
        case isSymbolicLink:
            return zfas.isSymbolicLink();
        case isOther:
            return zfas.isOther();
        case fileKey:
            return zfas.fileKey();
            
        case blockSize:
            if (isHadoopView)
                return zfas.getFileStatus().getBlockSize();
            break;
        case replication:
            if (isHadoopView)
                return zfas.getFileStatus().getReplication();
            break;
        }
        return null;
    }
}
