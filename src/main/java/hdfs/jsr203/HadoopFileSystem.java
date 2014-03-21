package hdfs.jsr203;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.ClosedFileSystemException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.ReadOnlyFileSystemException;
import java.nio.file.StandardOpenOption;
import java.nio.file.WatchService;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

public class HadoopFileSystem extends FileSystem {
	
	private org.apache.hadoop.fs.FileSystem fs;
	private FileSystemProvider provider;
	private boolean readOnly;
	private volatile boolean isOpen = true;

	public HadoopFileSystem(FileSystemProvider provider, String host, int port) throws IOException {
		
		this.provider = provider;
		
		// Create dynamic configuration
		Configuration conf = new org.apache.hadoop.conf.Configuration();
		conf.set("fs.default.name", "hdfs://" + host + ":" + port + "");
		//conf.set("fs.defaultFS", "hdfs://" + host + ":" + port + "/");
        //conf.set("hadoop.job.ugi", "hbase");

        this.fs = org.apache.hadoop.fs.FileSystem.get(conf);
	}

	 private final void beginWrite() {
	        //rwlock.writeLock().lock();
	    }

	    private final void endWrite() {
	        //rwlock.writeLock().unlock();
	    }

	    private final void beginRead() {
	       // rwlock.readLock().lock();
	    }

	    private final void endRead() {
	        //rwlock.readLock().unlock();
	    }
	
	@Override
	public void close() throws IOException {
		this.fs.close();
	}

	@Override
	public Iterable<FileStore> getFileStores() {
		ArrayList<FileStore> list = new ArrayList<>(1);
        list.add(new HadoopFileStore(new HadoopPath(this, "/")));
        return list;
	}

	@Override
	public Path getPath(String first, String... more) {
		String path;
        if (more.length == 0) {
            path = first;
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(first);
            for (String segment: more) {
                if (segment.length() > 0) {
                    if (sb.length() > 0)
                        sb.append('/');
                    sb.append(segment);
                }
            }
            path = sb.toString();
        }
		return new HadoopPath(this, path);
	}

	@Override
	public PathMatcher getPathMatcher(String syntaxAndPattern) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterable<Path> getRootDirectories() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSeparator() {
		return "/";
	}

	@Override
	public UserPrincipalLookupService getUserPrincipalLookupService() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isOpen() {
		return isOpen;
	}

	@Override
	public boolean isReadOnly() {
		return readOnly;
	}

	@Override
	public WatchService newWatchService() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FileSystemProvider provider() {
		return this.provider;
	}

	private static final Set<String> supportedFileAttributeViews =
            Collections.unmodifiableSet(
                new HashSet<String>(Arrays.asList("basic", "hadoop")));

    @Override
    public Set<String> supportedFileAttributeViews() {
        return supportedFileAttributeViews;
    }
	
	public String getHost()
	{
		return fs.getUri().getHost();
	}

	public int getPort()
	{
		return fs.getUri().getPort();
	}

	public org.apache.hadoop.fs.FileSystem getHDFS(){
		return this.fs;
	}
	
	void createDirectory(org.apache.hadoop.fs.Path dir, FileAttribute<?>... attrs) throws IOException
	{
		//FsPermission permission = new FsPermission(mode);
		if(!this.fs.mkdirs(dir))
			throw new IOException();
	}

	public Iterator<Path> iteratorOf(HadoopPath path,
			java.nio.file.DirectoryStream.Filter<? super Path> filter) throws IOException, URISyntaxException {
		List<Path> list = new ArrayList<>();
		// Get list of childs
        FileStatus[] status = this.fs.listStatus(path.getRawPath());
        for (int i=0;i<status.length;i++){
                /*BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                String line;
                line=br.readLine();
                while (line != null){
                        System.out.println(line);
                        line=br.readLine();
                }*/
       	 	//System.out.println(status[i].getPath() + " is dir ? " + status[i].isDir());
        	HadoopPath zp = new HadoopPath(this, status[i].getPath());
        	if (filter == null || filter.accept(zp))
                list.add(zp);
        }
		return list.iterator();
	}

	public void deleteFile(org.apache.hadoop.fs.Path hadoopPath, boolean failIfNotExists)
        throws IOException
    {
		checkWritable();
		
		// If no exist
		if (!this.fs.exists(hadoopPath))
		{
			if (failIfNotExists)
				throw new NoSuchFileException(hadoopPath.toString());
		}
		else
		{
			FileStatus stat = this.fs.getFileStatus(hadoopPath);
			if (stat.isDir()) {
				FileStatus[] stats = this.fs.listStatus(hadoopPath);
				if (stats.length > 0)
					throw new DirectoryNotEmptyException(hadoopPath.toString());
			}
			// Try to delete with no recursion
			this.fs.delete(hadoopPath, false);
		}
		
        /*IndexNode inode = getInode(hadoopPath);
        if (inode == null) {
            if (hadoopPath != null && hadoopPath.length == 0)
                throw new ZipException("root directory </> can't not be delete");
            if (failIfNotExists)
                throw new NoSuchFileException(getString(hadoopPath));
        } else {
            if (inode.isDir() && inode.child != null)
                throw new DirectoryNotEmptyException(getString(hadoopPath));
            updateDelete(inode);
        }*/
    }
	
	private void checkOptions(Set<? extends OpenOption> options) {
        // check for options of null type and option is an intance of StandardOpenOption
        for (OpenOption option : options) {
            if (option == null)
                throw new NullPointerException();
            if (!(option instanceof StandardOpenOption))
                throw new IllegalArgumentException();
        }
    }

	SeekableByteChannel newByteChannel(org.apache.hadoop.fs.Path path,
            Set<? extends OpenOption> options,
            FileAttribute<?>... attrs) throws IOException
    {
		// simple one : this.fs.create(hadoopPath);
		// TODO Auto-generated method stub
		//this.hdfs.
		//		throw new IOException("Not implemented");
		
		
		checkOptions(options);
        if (options.contains(StandardOpenOption.WRITE) ||
            options.contains(StandardOpenOption.APPEND)) {
            checkWritable();
            beginRead();
            try {
                final WritableByteChannel wbc = Channels.newChannel(
                    newOutputStream(path, options.toArray(new OpenOption[0])));
                long leftover = 0;
                if (options.contains(StandardOpenOption.APPEND)) {
                    /*Entry e = getEntry0(path);
                    if (e != null && e.size >= 0)
                        leftover = e.size;*/
                	throw new IOException("APPEND NOT IMPLEMENTED");
                }
                final long offset = leftover;
                return new SeekableByteChannel() {
                    long written = offset;
                    public boolean isOpen() {
                        return wbc.isOpen();
                    }

                    public long position() throws IOException {
                        return written;
                    }

                    public SeekableByteChannel position(long pos)
                        throws IOException
                    {
                        throw new UnsupportedOperationException();
                    }

                    public int read(ByteBuffer dst) throws IOException {
                        throw new UnsupportedOperationException();
                    }

                    public SeekableByteChannel truncate(long size)
                        throws IOException
                    {
                        throw new UnsupportedOperationException();
                    }

                    public int write(ByteBuffer src) throws IOException {
                        int n = wbc.write(src);
                        written += n;
                        return n;
                    }

                    public long size() throws IOException {
                        return written;
                    }

                    public void close() throws IOException {
                        wbc.close();
                    }
                };
            } finally {
                endRead();
            }
        } else {
            beginRead();
            try {
                ensureOpen();
                HadoopFileAttributes e = new HadoopPath(this, path).getAttributes();
                if (e == null || e.isDirectory())
                    throw new NoSuchFileException(path.toString());
                final ReadableByteChannel rbc =
                    Channels.newChannel(getInputStream(path));
                final long size = e.size();
                return new SeekableByteChannel() {
                    long read = 0;
                    public boolean isOpen() {
                        return rbc.isOpen();
                    }

                    public long position() throws IOException {
                        return read;
                    }

                    public SeekableByteChannel position(long pos)
                        throws IOException
                    {
                        throw new UnsupportedOperationException();
                    }

                    public int read(ByteBuffer dst) throws IOException {
                        int n = rbc.read(dst);
                        if (n > 0) {
                            read += n;
                        }
                        return n;
                    }

                    public SeekableByteChannel truncate(long size)
                    throws IOException
                    {
                        throw new NonWritableChannelException();
                    }

                    public int write (ByteBuffer src) throws IOException {
                        throw new NonWritableChannelException();
                    }

                    public long size() throws IOException {
                        return size;
                    }

                    public void close() throws IOException {
                        rbc.close();
                    }
                };
            } finally {
                endRead();
            }
        }
	}
	
	// Returns an output stream for writing the contents into the specified
    // entry.
    OutputStream newOutputStream(org.apache.hadoop.fs.Path path, OpenOption... options)
        throws IOException
    {
        //checkWritable();
        boolean hasCreateNew = false;
        boolean hasCreate = false;
        boolean hasAppend = false;
        for (OpenOption opt: options) {
            if (opt == READ)
                throw new IllegalArgumentException("READ not allowed");
            if (opt == CREATE_NEW)
                hasCreateNew = true;
            if (opt == CREATE)
                hasCreate = true;
            if (opt == APPEND)
                hasAppend = true;
        }
        //beginRead();                 // only need a readlock, the "update()" will
        /*try {                        // try to obtain a writelock when the os is
            ensureOpen();            // being closed.
            //Entry e = getEntry0(path);
            HadoopFileAttributes e = path.getAttributes();
            if (e != null) {
                if (e.isDirectory() || hasCreateNew)
                    throw new FileAlreadyExistsException(path.toString());
                if (hasAppend) {
                    InputStream is = getInputStream(e);
                    OutputStream os = getOutputStream(new Entry(e, Entry.NEW));
                    copyStream(is, os);
                    is.close();
                    return os;
                }
                return getOutputStream(new Entry(e, Entry.NEW));
            } else {
                if (!hasCreate && !hasCreateNew)
                    throw new NoSuchFileException(path.toString());
                checkParents(path);
                return getOutputStream(new Entry(path, Entry.NEW));
            }
        } finally {
            //endRead();
        }*/
        return this.fs.create(path);
    }
    
    private InputStream getInputStream(org.apache.hadoop.fs.Path path)
            throws IOException
        {
    	return this.fs.open(path);
           /* InputStream eis = null;

            if (path.type == Entry.NEW) {
                if (path.bytes != null)
                    eis = new ByteArrayInputStream(path.bytes);
                else if (path.file != null)
                    eis = Files.newInputStream(path.file);
                else
                    throw new ZipException("update entry data is missing");
            } else if (path.type == Entry.FILECH) {
                // FILECH result is un-compressed.
                eis = Files.newInputStream(path.file);
                // TBD: wrap to hook close()
                // streams.add(eis);
                return eis;
            } else {  // untouced  CEN or COPY
                eis = new EntryInputStream(path, ch);
            }
            if (path.method == METHOD_DEFLATED) {
                // MORE: Compute good size for inflater stream:
                long bufSize = path.size + 2; // Inflater likes a bit of slack
                if (bufSize > 65536)
                    bufSize = 8192;
                final long size = path.size;
                eis = new InflaterInputStream(eis, getInflater(), (int)bufSize) {

                    private boolean isClosed = false;
                    public void close() throws IOException {
                        if (!isClosed) {
                            releaseInflater(inf);
                            this.in.close();
                            isClosed = true;
                            streams.remove(this);
                        }
                    }
                    // Override fill() method to provide an extra "dummy" byte
                    // at the end of the input stream. This is required when
                    // using the "nowrap" Inflater option. (it appears the new
                    // zlib in 7 does not need it, but keep it for now)
                    protected void fill() throws IOException {
                        if (eof) {
                            throw new EOFException(
                                "Unexpected end of ZLIB input stream");
                        }
                        len = this.in.read(buf, 0, buf.length);
                        if (len == -1) {
                            buf[0] = 0;
                            len = 1;
                            eof = true;
                        }
                        inf.setInput(buf, 0, len);
                    }
                    private boolean eof;

                    public int available() throws IOException {
                        if (isClosed)
                            return 0;
                        long avail = size - inf.getBytesWritten();
                        return avail > (long) Integer.MAX_VALUE ?
                            Integer.MAX_VALUE : (int) avail;
                    }
                };
            } else if (path.method == METHOD_STORED) {
                // TBD: wrap/ it does not seem necessary
            } else {
                throw new ZipException("invalid compression method");
            }
            streams.add(eis);
            return eis;*/
        }
    
    private void checkWritable() throws IOException {
        if (readOnly)
            throw new ReadOnlyFileSystemException();
    }
    
    private void ensureOpen() throws IOException {
        if (!isOpen)
            throw new ClosedFileSystemException();
    }
    
    boolean exists(org.apache.hadoop.fs.Path path)
            throws IOException
        {
            beginRead();
            try {
                ensureOpen();
                return this.fs.exists(path);
            } finally {
                endRead();
            }
        }
    
    FileStore getFileStore(HadoopPath path) {
        return new HadoopFileStore(path);
    }
}
