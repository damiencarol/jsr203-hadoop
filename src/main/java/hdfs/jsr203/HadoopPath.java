package hdfs.jsr203;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileStore;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.ReadOnlyFileSystemException;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;

public class HadoopPath implements Path {

	private byte[] path;
	/** Store offsets of '/' chars */
	private volatile int[] offsets;
    private String internalPath;
	private HadoopFileSystem hdfs;

	HadoopPath(HadoopFileSystem hdfs, byte[] path) {
        this(hdfs, path, false);
    }

	HadoopPath(HadoopFileSystem hdfs, byte[] path, boolean normalized)
    {
        this.hdfs = hdfs;
        if (normalized)
            this.path = path;
        else
            this.path = normalize(path);
    }

	HadoopFileAttributes getAttributes() throws IOException {
		// this.hdfs.fs.getFileAttributes(this);//getResolvedPath());

		HadoopFileAttributes hfas = new HadoopFileAttributes(this.hdfs
				.getHDFS().getFileStatus(this.getRawResolvedPath()));
		/*
		 * if (hfas == null) throw new NoSuchFileException(toString());
		 */
		return hfas;
	}
	
	private HadoopPath checkPath(Path path) {
        if (path == null)
            throw new NullPointerException();
        if (!(path instanceof HadoopPath))
            throw new ProviderMismatchException();
        return (HadoopPath) path;
    }

	@Override
	public int compareTo(Path other) {
		final HadoopPath o = checkPath(other);
        return this.internalPath.compareTo(o.internalPath);
	}

	@Override
	public boolean endsWith(Path other) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean endsWith(String other) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Path getFileName() {
		return this;
	}

	@Override
	public HadoopFileSystem getFileSystem() {
		return this.hdfs;
	}

	@Override
	public Path getName(int index) {
		initOffsets();
        if (index < 0 || index >= offsets.length)
            throw new IllegalArgumentException();
        int begin = offsets[index];
        int len;
        if (index == (offsets.length-1))
            len = path.length - begin;
        else
            len = offsets[index+1] - begin - 1;
        // construct result
        byte[] result = new byte[len];
        System.arraycopy(path, begin, result, 0, len);
        return new HadoopPath(this.hdfs, result);
	}

	@Override
	public int getNameCount() {
		initOffsets();
        return offsets.length;
	}

	@Override
	public Path getParent() {
		initOffsets();
        int count = offsets.length;
        if (count == 0)    // no elements so no parent
            return null;
        int len = offsets[count-1] - 1;
        if (len <= 0)      // parent is root only (may be null)
            return getRoot();
        byte[] result = new byte[len];
        System.arraycopy(path, 0, result, 0, len);
        return new HadoopPath(this.hdfs, result);
	}

	@Override
	public Path getRoot() {
		if (this.isAbsolute())
            return new HadoopPath(this.hdfs, new byte[]{path[0]});
        else
            return null;
	}

	@Override
	public boolean isAbsolute() {
		return (this.path.length > 0 && path[0] == '/');
	}

	@Override
	public Iterator<Path> iterator() {
		return new Iterator<Path>() {
            private int i = 0;

            @Override
            public boolean hasNext() {
                return (i < getNameCount());
            }

            @Override
            public Path next() {
                if (i < getNameCount()) {
                    Path result = getName(i);
                    i++;
                    return result;
                } else {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public void remove() {
                throw new ReadOnlyFileSystemException();
            }
        };
	}

	@Override
	public Path normalize() {
		byte[] resolved = getResolved();
        if (resolved == path)    // no change
            return this;
        return new HadoopPath(this.hdfs, resolved, true);
	}
	
	// removes redundant slashs, replace "\" to hadoop separator "/"
    // and check for invalid characters
    private byte[] normalize(byte[] path) {
        if (path.length == 0)
            return path;
        byte prevC = 0;
        for (int i = 0; i < path.length; i++) {
            byte c = path[i];
            if (c == '\\')
                return normalize(path, i);
            if (c == (byte)'/' && prevC == '/')
                return normalize(path, i - 1);
            if (c == '\u0000')
                throw new InvalidPathException(this.hdfs.getString(path),
                                               "Path: nul character not allowed");
            prevC = c;
        }
        return path;
    }

    private byte[] normalize(byte[] path, int off) {
        byte[] to = new byte[path.length];
        int n = 0;
        while (n < off) {
            to[n] = path[n];
            n++;
        }
        int m = n;
        byte prevC = 0;
        while (n < path.length) {
            byte c = path[n++];
            if (c == (byte)'\\')
                c = (byte)'/';
            if (c == (byte)'/' && prevC == (byte)'/')
                continue;
            if (c == '\u0000')
                throw new InvalidPathException(this.hdfs.getString(path),
                                               "Path: nul character not allowed");
            to[m++] = c;
            prevC = c;
        }
        if (m > 1 && to[m - 1] == '/')
            m--;
        return (m == to.length)? to : Arrays.copyOf(to, m);
    }

	@Override
	public WatchKey register(WatchService watcher, Kind<?>... events)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public WatchKey register(WatchService watcher, Kind<?>[] events,
			Modifier... modifiers) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Path relativize(Path other) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Path resolve(Path other) {
		final HadoopPath o = checkPath(other);
        if (o.isAbsolute())
            return o;
        byte[] resolved = null;
        if (this.path[path.length - 1] == '/') {
            resolved = new byte[path.length + o.path.length];
            System.arraycopy(path, 0, resolved, 0, path.length);
            System.arraycopy(o.path, 0, resolved, path.length, o.path.length);
        } else {
            resolved = new byte[path.length + 1 + o.path.length];
            System.arraycopy(path, 0, resolved, 0, path.length);
            resolved[path.length] = '/';
            System.arraycopy(o.path, 0, resolved, path.length + 1, o.path.length);
        }
        return new HadoopPath(hdfs, resolved);
	}

	@Override
	public Path resolve(String other) {
		return resolve(getFileSystem().getPath(other));
	}

	@Override
	public Path resolveSibling(Path other) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Path resolveSibling(String other) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean startsWith(Path other) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean startsWith(String other) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public HadoopPath subpath(int beginIndex, int endIndex) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public HadoopPath toAbsolutePath() {
		if (isAbsolute()) {
            return this;
        } else {
            //add / before the existing path
            byte[] defaultdir = "/".getBytes(); //this.hdfs.getDefaultDir().path;
            int defaultlen = defaultdir.length;
            boolean endsWith = (defaultdir[defaultlen - 1] == '/');
            byte[] t = null;
            if (endsWith)
                t = new byte[defaultlen + path.length];
            else
                t = new byte[defaultlen + 1 + path.length];
            System.arraycopy(defaultdir, 0, t, 0, defaultlen);
            if (!endsWith)
                t[defaultlen++] = '/';
            System.arraycopy(path, 0, t, defaultlen, path.length);
            return new HadoopPath(this.hdfs, t, true);  // normalized
        }
	}

	@Override
	public File toFile() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Path toRealPath(LinkOption... options) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public URI toUri() {
		/*try {
			return new URI(HadoopFileSystemProvider.SCHEME,
					this.hdfs.getHost(), this.internalPath);
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		return null;
	}

	void checkAccess(AccessMode... modes) throws IOException {
		boolean w = false;
		boolean x = false;
		for (AccessMode mode : modes) {
			switch (mode) {
			case READ:
				break;
			case WRITE:
				w = true;
				break;
			case EXECUTE:
				x = true;
				break;
			default:
				throw new UnsupportedOperationException();
			}
		}
		org.apache.hadoop.fs.Path hdfs_path = getRawResolvedPath();
		// ZipFileAttributes attrs = zfs.getFileAttributes(getResolvedPath());
		// if (attrs == null && (path.length != 1 || path[0] != '/'))
		if (!this.hdfs.getHDFS().exists(hdfs_path))
			throw new NoSuchFileException(toString());
		if (w) {
			if (this.hdfs.isReadOnly())
				throw new AccessDeniedException(toString());
		}
		if (x)
			throw new AccessDeniedException(toString());
	}

	void createDirectory(FileAttribute<?>... attrs) throws IOException {
		this.hdfs.createDirectory(getResolvedPath(), attrs);
	}

	@Override
	public String toString() {
		return new String(this.path);
	}

	DirectoryStream<Path> newDirectoryStream(Filter<? super Path> filter)
			throws IOException {
		return new HadoopDirectoryStream(this, filter);
	}

	/**
	 * Helper to get the raw interface of HDFS path.
	 * 
	 * @return
	 */
	public org.apache.hadoop.fs.Path getRawResolvedPath() {
		return new org.apache.hadoop.fs.Path("hdfs://"
				+ hdfs.getHost() + ":" + hdfs.getPort() + new String(getResolvedPath()));
	}

	void delete() throws IOException {
		this.hdfs.deleteFile(getRawResolvedPath(), true);
	}

	void deleteIfExists() throws IOException {
		this.hdfs.deleteFile(getRawResolvedPath(), false);
	}

	void move(HadoopPath target, CopyOption... options) throws IOException {
		/*
		 * if (Files.isSameFile(this.zfs.getZipFile(), target.zfs.getZipFile()))
		 * { zfs.copyFile(true, getResolvedPath(), target.getResolvedPath(),
		 * options); } else { copyToTarget(target, options); delete(); }
		 */
		// this.hdfs.getHDFS().rename(arg0, arg1);
		// TODO: Fix this
		throw new IOException();
	}

	SeekableByteChannel newByteChannel(Set<? extends OpenOption> options,
			FileAttribute<?>... attrs) throws IOException {
		return this.hdfs.newByteChannel(getRawResolvedPath(), options, attrs);
	}

	public Map<String, Object> readAttributes(String attributes,
			LinkOption[] options) throws IOException {
		String view = null;
        String attrs = null;
        int colonPos = attributes.indexOf(':');
        if (colonPos == -1) {
            view = "basic";
            attrs = attributes;
        } else {
            view = attributes.substring(0, colonPos++);
            attrs = attributes.substring(colonPos);
        }
        HadoopFileAttributeView hfv = HadoopFileAttributeView.get(this, view);
        if (hfv == null) {
            throw new UnsupportedOperationException("view not supported");
        }
        return hfv.readAttributes(attrs);
	}

	// the result path does not contain ./ and .. components
    private volatile byte[] resolved = null;
    byte[] getResolvedPath() {
        byte[] r = resolved;
        if (r == null) {
            if (isAbsolute())
                r = getResolved();
            else
                r = toAbsolutePath().getResolvedPath();
            //if (r[0] == '/')
            //    r = Arrays.copyOfRange(r, 1, r.length);
            resolved = r;
        }
        return resolved;
    }
	
 // Remove DotSlash(./) and resolve DotDot (..) components
    private byte[] getResolved() {
        if (path.length == 0)
            return path;
        for (int i = 0; i < path.length; i++) {
            byte c = path[i];
            if (c == (byte)'.')
                return resolve0();
        }
        return path;
    }

    // TBD: performance, avoid initOffsets
    private byte[] resolve0() {
        byte[] to = new byte[path.length];
        int nc = getNameCount();
        int[] lastM = new int[nc];
        int lastMOff = -1;
        int m = 0;
        for (int i = 0; i < nc; i++) {
            int n = offsets[i];
            int len = (i == offsets.length - 1)?
                      (path.length - n):(offsets[i + 1] - n - 1);
            if (len == 1 && path[n] == (byte)'.') {
                if (m == 0 && path[0] == '/')   // absolute path
                    to[m++] = '/';
                continue;
            }
            if (len == 2 && path[n] == '.' && path[n + 1] == '.') {
                if (lastMOff >= 0) {
                    m = lastM[lastMOff--];  // retreat
                    continue;
                }
                if (path[0] == '/') {  // "/../xyz" skip
                    if (m == 0)
                        to[m++] = '/';
                } else {               // "../xyz" -> "../xyz"
                    if (m != 0 && to[m-1] != '/')
                        to[m++] = '/';
                    while (len-- > 0)
                        to[m++] = path[n++];
                }
                continue;
            }
            if (m == 0 && path[0] == '/' ||   // absolute path
                m != 0 && to[m-1] != '/') {   // not the first name
                to[m++] = '/';
            }
            lastM[++lastMOff] = m;
            while (len-- > 0)
                to[m++] = path[n++];
        }
        if (m > 1 && to[m - 1] == '/')
            m--;
        return (m == to.length)? to : Arrays.copyOf(to, m);
    }

	void setTimes(FileTime mtime, FileTime atime, FileTime ctime)
	        throws IOException
	{
		// Get actual value
		if (mtime == null || atime == null)
		{
			FileStatus stat = this.hdfs.getHDFS().getFileStatus(getRawResolvedPath());
			atime = FileTime.fromMillis(stat.getAccessTime());
			mtime = FileTime.fromMillis(stat.getModificationTime());
		}
		this.hdfs.getHDFS().setTimes(getRawResolvedPath(), mtime.toMillis(), atime.toMillis());
	}
	
	FileStore getFileStore() throws IOException {
        // each HadoopFileSystem only has one root (for each cluster)
        if (exists())
            return hdfs.getFileStore(this);
        throw new NoSuchFileException(this.hdfs.getString(path));
    }
	
	boolean exists() {
		// Root case
        if ("/".equals(internalPath))
            return true;
        try {
            return hdfs.exists(getRawResolvedPath());
        } catch (IOException x) {}
        return false;
    }
	
	// create offset list if not already created
    private void initOffsets() {
        if (offsets == null) {
            int count, index;
            // count names
            count = 0;
            index = 0;
            while (index < path.length) {
                byte c = path[index++];
                if (c != '/') {
                    count++;
                    while (index < path.length && path[index] != '/')
                        index++;
                }
            }
            // populate offsets
            int[] result = new int[count];
            count = 0;
            index = 0;
            while (index < path.length) {
                byte c = path[index];
                if (c == '/') {
                    index++;
                } else {
                    result[count++] = index++;
                    while (index < path.length && path[index] != '/')
                        index++;
                }
            }
            synchronized (this) {
                if (offsets == null)
                    offsets = result;
            }
        }
    }
    
    void copy(HadoopPath target, CopyOption... options)
            throws IOException
        {
            if (this.hdfs.sameCluster(target.hdfs))
                this.hdfs.copyFile(false,
                             getResolvedPath(), target.getResolvedPath(),
                             options);
            else
                copyToTarget(target, options);
        }

        private void copyToTarget(HadoopPath target, CopyOption... options)
            throws IOException
        {
        	throw new IOException("Copy beetween cluster is not implemented");
        	/*
            boolean replaceExisting = false;
            boolean copyAttrs = false;
            for (CopyOption opt : options) {
                if (opt == REPLACE_EXISTING)
                    replaceExisting = true;
                else if (opt == COPY_ATTRIBUTES)
                    copyAttrs = true;
            }
            // attributes of source file
            HadoopFileAttributes zfas = getAttributes();
            // check if target exists
            boolean exists;
            if (replaceExisting) {
                try {
                    target.deleteIfExists();
                    exists = false;
                } catch (DirectoryNotEmptyException x) {
                    exists = true;
                }
            } else {
                exists = target.exists();
            }
            if (exists)
                throw new FileAlreadyExistsException(target.toString());

            if (zfas.isDirectory()) {
                // create directory or file
                target.createDirectory();
            } else {
                InputStream is = hdfs.newInputStream(getResolvedPath());
                try {
                    OutputStream os = target.newOutputStream();
                    try {
                        byte[] buf = new byte[8192];
                        int n = 0;
                        while ((n = is.read(buf)) != -1) {
                            os.write(buf, 0, n);
                        }
                    } finally {
                        os.close();
                    }
                } finally {
                    is.close();
                }
            }
            if (copyAttrs) {
                BasicFileAttributeView view =
                    ZipFileAttributeView.get(target, BasicFileAttributeView.class);
                try {
                    view.setTimes(zfas.lastModifiedTime(),
                                  zfas.lastAccessTime(),
                                  zfas.creationTime());
                } catch (IOException x) {
                    // rollback?
                    try {
                        target.delete();
                    } catch (IOException ignore) { }
                    throw x;
                }
            }*/
        }
}
