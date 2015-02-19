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
package hdfs.jsr203;

import static hdfs.jsr203.HadoopUtils.toRegexPattern;
import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import hdfs.jsr203.attribute.HadoopPosixFileAttributes;
import hdfs.jsr203.attribute.HadoopUserPrincipalLookupService;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.ClosedFileSystemException;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.ReadOnlyFileSystemException;
import java.nio.file.StandardOpenOption;
import java.nio.file.WatchService;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

public class HadoopFileSystem extends FileSystem {
	
	private org.apache.hadoop.fs.FileSystem fs;
	private FileSystemProvider provider;
	private boolean readOnly;
	private volatile boolean isOpen = true;
	private UserPrincipalLookupService userPrincipalLookupService;

	public HadoopFileSystem(FileSystemProvider provider, String host, int port) throws IOException {
		
		this.provider = provider;
		
		// Create dynamic configuration
		Configuration conf = new org.apache.hadoop.conf.Configuration();
		conf.set("fs.defaultFS", "hdfs://" + host + ":" + port + "/");

        this.fs = org.apache.hadoop.fs.FileSystem.get(conf);
        
        this.userPrincipalLookupService = new HadoopUserPrincipalLookupService(this);
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
        list.add(new HadoopFileStore(new HadoopPath(this, new byte[]{'/'})));
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
		return new HadoopPath(this, getBytes(path));
	}

    private static final String GLOB_SYNTAX = "glob";
    private static final String REGEX_SYNTAX = "regex";

	@Override
	public PathMatcher getPathMatcher(String syntaxAndPattern) {
		int pos = syntaxAndPattern.indexOf(':');
        if (pos <= 0 || pos == syntaxAndPattern.length()) {
            throw new IllegalArgumentException();
        }
        String syntax = syntaxAndPattern.substring(0, pos);
        String input = syntaxAndPattern.substring(pos + 1);
        String expr;
        if (syntax.equals(GLOB_SYNTAX)) {
            expr = toRegexPattern(input);
        } else {
            if (syntax.equals(REGEX_SYNTAX)) {
                expr = input;
            } else {
                throw new UnsupportedOperationException("Syntax '" + syntax +
                    "' not recognized");
            }
        }
        // return matcher
        final Pattern pattern = Pattern.compile(expr);
        return new PathMatcher() {
            @Override
            public boolean matches(Path path) {
                return pattern.matcher(path.toString()).matches();
            }
        };
	}

	@Override
	public Iterable<Path> getRootDirectories() {
		ArrayList<Path> pathArr = new ArrayList<>();
        pathArr.add(new HadoopPath(this, new byte[]{'/'}));
        return pathArr;
	}

	@Override
	public String getSeparator() {
		return "/";
	}

	@Override
	public UserPrincipalLookupService getUserPrincipalLookupService() {
		return userPrincipalLookupService;
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
		throw new UnsupportedOperationException();
	}

	@Override
	public FileSystemProvider provider() {
		return this.provider;
	}

	private static final Set<String> supportedFileAttributeViews =
            Collections.unmodifiableSet(
                new HashSet<String>(Arrays.asList("basic", "hadoop", "posix")));

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
	
	void createDirectory(byte[] dir, FileAttribute<?>... attrs) throws IOException
	{		
		checkWritable();
        dir = HadoopUtils.toDirectoryPath(dir);
        beginWrite();
        try {
            ensureOpen();
            if (dir.length == 0 || exists(dir))  // root dir, or exiting dir
                throw new FileAlreadyExistsException(getString(dir));
            //checkParents(dir);
            this.fs.mkdirs(new HadoopPath(this, dir).getRawResolvedPath());
        } finally {
            endWrite();
        }
	}

	public Iterator<Path> iteratorOf(HadoopPath path,
			java.nio.file.DirectoryStream.Filter<? super Path> filter) throws IOException, URISyntaxException 
	{
		beginWrite();
        try {
            ensureOpen();
            FileStatus inode = this.fs.getFileStatus(path.getRawResolvedPath());
            if (inode.isDirectory() == false)
                throw new NotDirectoryException(getString(path.getResolvedPath()));
            List<Path> list = new ArrayList<>();
            for (FileStatus stat : this.fs.listStatus(path.getRawResolvedPath())) {
                HadoopPath hp = new HadoopPath(this, stat.getPath().toUri().getPath().getBytes());
                if (filter == null || filter.accept(hp))
                    list.add(hp);
            }
            return list.iterator();
        } finally {
            endWrite();
        }
	}
	
	final byte[] getBytes(String name) {
		// TODO : add charset management
        return name.getBytes();//zc.getBytes(name);
    }
	
	final String getString(byte[] name) {
		// TODO : add charset management
        return new String(name);
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
			if (stat.isDirectory()) {
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
                FileStatus e = this.fs.getFileStatus(path);
                if (e == null || e.isDirectory())
                    throw new NoSuchFileException(path.toString());
                final ReadableByteChannel rbc =
                    Channels.newChannel(getInputStream(path));
                final long size = e.getLen();
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
    
    boolean exists(byte[] path)
            throws IOException
        {
    	beginRead();
        try {
            ensureOpen();
            return this.fs.exists(new HadoopPath(this, path).getRawResolvedPath());
        } finally {
            endRead();
        }
        }
    
    FileStore getFileStore(HadoopPath path) {
        return new HadoopFileStore(path);
    }

	public boolean sameCluster(HadoopFileSystem hdfs) {
		return (this.fs.getUri().equals(hdfs.fs.getUri()));
	}

	void copyFile(boolean deletesrc, byte[]src, byte[] dst, CopyOption... options)
        throws IOException
    {
		checkWritable();
        if (Arrays.equals(src, dst))
            return;    // do nothing, src and dst are the same

        beginWrite();
        try {
            ensureOpen();
            org.apache.hadoop.fs.Path eSrc_path = new HadoopPath(this, src).getRawResolvedPath();
            FileStatus eSrc = this.fs.getFileStatus(eSrc_path);
            if (!this.fs.exists(eSrc_path))
                throw new NoSuchFileException(getString(src));
            if (eSrc.isDirectory()) {    // specification says to create dst directory
                createDirectory(dst);
                return;
            }
            boolean hasReplace = false;
            boolean hasCopyAttrs = false;
            for (CopyOption opt : options) {
                if (opt == REPLACE_EXISTING)
                    hasReplace = true;
                else if (opt == COPY_ATTRIBUTES)
                    hasCopyAttrs = true;
            }
            org.apache.hadoop.fs.Path eDst_path = new HadoopPath(this, dst).getRawResolvedPath();
            FileStatus eDst = this.fs.getFileStatus(eDst_path);
            if (!fs.exists(eDst_path)) {
                if (!hasReplace)
                    throw new FileAlreadyExistsException(getString(dst));
            } else {
                //checkParents(dst);
            }
           
            org.apache.hadoop.fs.Path[] srcs = new org.apache.hadoop.fs.Path[] {eSrc_path};
			this.fs.concat(eDst_path, srcs);
            /*
            Entry u = new Entry(eSrc, Entry.COPY);    // copy eSrc entry
            u.name(dst);                              // change name
            if (eSrc.type == Entry.NEW || eSrc.type == Entry.FILECH)
            {
                u.type = eSrc.type;    // make it the same type
                if (!deletesrc) {      // if it's not "rename", just take the data
                    if (eSrc.bytes != null)
                        u.bytes = Arrays.copyOf(eSrc.bytes, eSrc.bytes.length);
                    else if (eSrc.file != null) {
                        u.file = getTempPathForEntry(null);
                        Files.copy(eSrc.file, u.file, REPLACE_EXISTING);
                    }
                }
            }
            if (!hasCopyAttrs)
                u.mtime = u.atime= u.ctime = System.currentTimeMillis();
            update(u);
            if (deletesrc)
                updateDelete(eSrc);*/
        } finally {
            endWrite();
        }
    }

	public void setTimes(byte[] bs, FileTime mtime, FileTime atime, FileTime ctime) throws IOException
	{
		HadoopPath hp = new HadoopPath(this, bs);
		org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("hdfs://"
				+ this.getHost() + ":" + this.getPort() + new String(hp.getResolvedPath()));
		// Get actual value
		if (mtime == null || atime == null)
		{
			FileStatus stat = this.fs.getFileStatus(path);
			atime = FileTime.fromMillis(stat.getAccessTime());
			mtime = FileTime.fromMillis(stat.getModificationTime());
		}
		this.fs.setTimes(path, mtime.toMillis(), atime.toMillis());
	}

	PosixFileAttributes getPosixFileAttributes(byte[] path) throws IOException
	{
        beginRead();
        try {
            ensureOpen();
            FileStatus stat = this.fs.getFileStatus(toHadoopPath(path));
            return new HadoopPosixFileAttributes(this, stat);
        } finally {
            endRead();
        }
	}
	
	private org.apache.hadoop.fs.Path toHadoopPath(byte[] path) {
		//HadoopPath hdp = new HadoopPath(this, path).normalize().toUri().getPath();
		URI uri = this.fs.getUri().resolve(getString(path));
		return new org.apache.hadoop.fs.Path(uri);
	}
}
