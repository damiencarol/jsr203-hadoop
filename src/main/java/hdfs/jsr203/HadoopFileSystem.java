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

import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.AccessMode;
import java.nio.file.ClosedFileSystemException;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.ReadOnlyFileSystemException;
import java.nio.file.StandardOpenOption;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;

public class HadoopFileSystem extends FileSystem {

  private static final String GLOB_SYNTAX = "glob";
  private static final String REGEX_SYNTAX = "regex";

	private org.apache.hadoop.fs.FileSystem fs;
	private FileSystemProvider provider;
	private boolean readOnly;
	private volatile boolean isOpen = true;
	private UserPrincipalLookupService userPrincipalLookupService;
    private int hashcode = 0;  // cached hash code (created lazily)


  private static final Set<String> supportedFileAttributeViews =
      Collections.unmodifiableSet(new HashSet<String>(Arrays.asList("basic", "hadoop", "owner", "posix")));

	public HadoopFileSystem(FileSystemProvider provider, String host, int uriPort) throws IOException {
		
		this.provider = provider;
		
		int port = uriPort;
		if (port == -1) {
		  port = 8020; // Default Hadoop port
		}

		// Create dynamic configuration
		Configuration conf = new Configuration();
    if (host == null) {
      String defaultScheme =
          org.apache.hadoop.fs.FileSystem.getDefaultUri(conf).getScheme();
      if (!"hdfs".equals(defaultScheme)) {
        throw new NullPointerException("Null host not permitted if default " +
            "Hadoop filesystem is not HDFS.");
      }
    } else {
      conf.set("fs.defaultFS", "hdfs://" + host + ":" + port + "/");
    }

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
                    if (sb.length() > 0) {
                        sb.append('/');
                    }
                    sb.append(segment);
                }
            }
            path = sb.toString();
        }
		return new HadoopPath(this, getBytes(path));
	}

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
            expr = HadoopUtils.toRegexPattern(input);
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
    // Not implemented now
    // The Hadoop API for notification is not enough stable
		throw new IOException("Not implemented");
	}

	@Override
	public FileSystemProvider provider() {
		return this.provider;
	}
	
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
	
	public void createDirectory(final byte[] directory, FileAttribute<?>... attrs) throws IOException
	{		
		checkWritable();
		byte[] dirPath;
		if (directory.length != 0 && directory[directory.length - 1] != '/') {
		  dirPath = Arrays.copyOf(directory, directory.length + 1);
		  dirPath[dirPath.length - 1] = '/';
		} else {
		  dirPath = Arrays.copyOf(directory, directory.length);
		}
    beginWrite();
    try {
        ensureOpen();
        if (dirPath.length == 0 || exists(dirPath))  // root directory, or existing directory
            throw new FileAlreadyExistsException(getString(dirPath));
        //checkParents(dir);
        this.fs.mkdirs(new HadoopPath(this, dirPath).getRawResolvedPath());
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
            //FileStatus inode = this.fs.getFileStatus(path.getRawResolvedPath());
            //if (inode.isDirectory() == false)
            //    throw new NotDirectoryException(getString(path.getResolvedPath()));
            List<Path> list = new ArrayList<Path>();
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
	
	FileChannel newFileChannel(org.apache.hadoop.fs.Path path, 
	        Set<? extends OpenOption> options, 
	        FileAttribute<?>[] attrs) throws IOException
	{
	    checkOptions(options);
        
        // Check that file not exists
        if (options.contains(CREATE_NEW) && 
                this.fs.exists(path)) {
            throw new FileAlreadyExistsException(path.toString());
        }
        // TODO: implement writing
	    return new HadoopFileChannel(newByteChannel(path, options, attrs));
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
		
		// Check that file not exists
		if (options.contains(CREATE_NEW) && 
				this.fs.exists(path)) {
			throw new FileAlreadyExistsException(path.toString());
		}
				
		
		if (options.contains(WRITE) ||
            options.contains(APPEND)) {
            checkWritable();
            beginRead();
            try {
                final WritableByteChannel wbc = Channels.newChannel(
                    newOutputStream(path, options, attrs));
                long leftover = 0;
                if (options.contains(APPEND)) {
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
                final FSDataInputStream inputStream = getInputStream(path);
                final ReadableByteChannel rbc =
                    Channels.newChannel(inputStream);
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
                        // ReadableByteChannel is not buffered, so it reads through
                        inputStream.seek(pos);
                        read = pos;
                        return this;
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
  public OutputStream newOutputStream(org.apache.hadoop.fs.Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs)
        throws IOException
  {
    checkWritable();
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
        
        FSDataOutputStream outputStream = this.fs.create(path);
        /*
        for (int i = 0; i < attrs.length; i++) {
        	FileAttribute<?> item = attrs[i];
			if (item.value().getClass() == PosixFilePermissions.class) {
        		Set<PosixFilePermission> itemPs = (Set<PosixFilePermission>) item.value();
				FsPermission p = FsPermission.valueOf("-" + PosixFilePermissions.toString(itemPs));
        		this.fs.setPermission(path, p);
        		break;
        	}
        	System.out.println(item.getClass());
        }
        */
        return outputStream;
    }
    
    private FSDataInputStream getInputStream(org.apache.hadoop.fs.Path path)
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
    
    public boolean exists(org.apache.hadoop.fs.Path path)
            throws IOException
    {
      beginRead();
      try {
        ensureOpen();
        return this.fs.exists(path);
        } 
      finally {
        endRead();
        }
    }
    
  private boolean exists(byte[] path)
      throws IOException
  {
    beginRead();
    try 
    {
      ensureOpen();
      return this.fs.exists(new HadoopPath(this, path).getRawResolvedPath());
    }
    finally
    {
      endRead();
    }
  }
    
  public FileStore getFileStore(final HadoopPath path) {
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
//            FileStatus eDst = this.fs.getFileStatus(eDst_path); //if eDst_path not exist, it will throw an error
    
            if (fs.exists(eDst_path)) {
                if (!hasReplace)
                    throw new FileAlreadyExistsException(getString(dst));

                if(!fs.delete(eDst_path, false)) {
                	throw new AccessDeniedException("cannot delete hdfs file " + getString(dst));
                }
            } else {
                //checkParents(dst);
            }
            
            //Simply use FileUtil.copy here. Can we use DistCp for very big files here? zongjie@novelbio.com
            boolean isCanDeleteSourceFile = FileUtil.copy(fs, eSrc_path, fs, eDst_path, deletesrc, fs.getConf());
            if (!isCanDeleteSourceFile) {
            	throw new AccessDeniedException("cannot delete source file " + eSrc_path.toString());
            }
            
//            org.apache.hadoop.fs.Path[] srcs = new org.apache.hadoop.fs.Path[] {eSrc_path};
//			this.fs.concat(eDst_path, srcs);
            
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

	void moveFile(byte[]src, byte[] dst, CopyOption... options)
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
    
            if (fs.exists(eDst_path)) {
                if (!hasReplace)
                    throw new FileAlreadyExistsException(getString(dst));

                if(!fs.delete(eDst_path, false)) {
                	throw new AccessDeniedException("cannot delete hdfs file " + getString(dst));
                }
            }
            //Simply rename the path
            if (!fs.rename(eSrc_path, eDst_path)) {
            	throw new AccessDeniedException("cannot move source file " + eSrc_path.toString());
            }
   
        } finally {
            endWrite();
        }
    }

	public void setTimes(byte[] bs, FileTime mtime, FileTime atime, FileTime ctime) throws IOException
	{
		org.apache.hadoop.fs.Path hp = new HadoopPath(this, bs).getRawResolvedPath();
		long mtime_millis = 0;
		long atime_millis = 0;
    // Get actual value
		if (mtime == null || atime == null)
		{
			FileStatus stat = this.fs.getFileStatus(hp);
      mtime_millis = stat.getModificationTime();
			atime_millis = stat.getAccessTime();
		}
    if (mtime != null) {
      mtime_millis = mtime.toMillis();
    }
    if (atime != null) {
      atime_millis = atime.toMillis();
    }
		this.fs.setTimes(hp, mtime_millis, atime_millis);
	}

	PosixFileAttributes getPosixFileAttributes(HadoopPath path) throws IOException
	{
        beginRead();
        try {
            ensureOpen();
            org.apache.hadoop.fs.Path resolvedPath = path.getRawResolvedPath();
            FileStatus fileStatus = path.getFileSystem().getHDFS().getFileStatus(resolvedPath);
            String fileKey = resolvedPath.toString();
            return new HadoopPosixFileAttributes(this, fileKey, fileStatus);
        } finally {
            endRead();
        }
	}
	
	public IAttributeReader getView(HadoopPath path, String type) {
    if ("basic".equals(type))
      return new HadoopBasicFileAttributeView(path, false);
    if ("hadoop".equals(type))
      return new HadoopBasicFileAttributeView(path, true);
    if ("owner".equals(type))
      return new HadoopPosixFileAttributeView(path, false);
    if ("posix".equals(type))
      return new HadoopPosixFileAttributeView(path, true);
    return null;
	}
	
	public IAttributeWriter getAttributeWriter(HadoopPath path, String type) {
    if ("basic".equals(type))
      return new HadoopBasicFileAttributeView(path, false);
    if ("hadoop".equals(type))
      return new HadoopBasicFileAttributeView(path, true);
    if ("owner".equals(type))
      return new HadoopPosixFileAttributeView(path, false);
    if ("posix".equals(type))
      return new HadoopPosixFileAttributeView(path, true);
    return null;
	}
	
    @SuppressWarnings("unchecked")
	<V extends FileAttributeView> V getView(HadoopPath path, Class<V> type) {
        if (type == null)
            throw new NullPointerException();
        if (type == BasicFileAttributeView.class)
            return (V)new HadoopBasicFileAttributeView(path, false);
        if (type == HadoopBasicFileAttributeView.class)
            return (V)new HadoopBasicFileAttributeView(path, true);
        if (type == FileOwnerAttributeView.class)
            return (V)new HadoopPosixFileAttributeView(path, false);
        if (type == PosixFileAttributeView.class)
            return (V)new HadoopPosixFileAttributeView(path, true);
        return null;
    }

	public Map<String, Object> readAttributes(HadoopPath hadoopPath,
			String attributes, LinkOption[] options) throws IOException {
		// TODO Auto-generated method stub

		/*public Map<String, Object> readAttributes(String attributes,
				LinkOption[] options) throws IOException {*/
		
		String view;
        String attrs;
        int colonPos = attributes.indexOf(':');
        if (colonPos == -1) {
            view = "basic";
            attrs = attributes;
        } else {
            view = attributes.substring(0, colonPos++);
            attrs = attributes.substring(colonPos);
        }
        IAttributeReader hfv = getView(hadoopPath, view);
        if (hfv == null) {
            throw new UnsupportedOperationException("view <" + view + "> not supported");
        }
        return hfv.readAttributes(attrs, options);
	}

	public void setAttribute(HadoopPath hadoopPath, String attribute,
			Object value, LinkOption[] options) throws IOException {
		// TODO Auto-generated method stub
		

	    /*void setAttribute(String attribute, Object value, LinkOption... options)
	            throws IOException
		{*/
		    String type;
		    String attr;
		    int colonPos = attribute.indexOf(':');
		    if (colonPos == -1) {
		        type = "basic";
		        attr = attribute;
		    } else {
		        type = attribute.substring(0, colonPos++);
		        attr = attribute.substring(colonPos);
		    }
		    IAttributeWriter view = getAttributeWriter(hadoopPath, type);
		    if (view == null)
		        throw new UnsupportedOperationException("view <" + type + "> is not supported");
		    view.setAttribute(attr, value, options);
	}
	
	void checkAccess(HadoopPath path, AccessMode... modes) throws IOException {
		// Get Raw path
		org.apache.hadoop.fs.Path hdfs_path = path.getRawResolvedPath();
		// First check if the path exists
		if (!path.getFileSystem().getHDFS().exists(hdfs_path))
			throw new NoSuchFileException(toString());
		// Check if ACL is enabled
		if (!path.getFileSystem().getHDFS().getConf().getBoolean("dfs.namenode.acls.enabled", false)) {
			return;
		}
		// TODO implement check access
	}

	@Override
	public int hashCode() {
	  if (hashcode == 0) {
	    hashcode = this.fs.hashCode();
	  }
	  return hashcode;
  } 

    @Override
    public boolean equals(Object obj) {
        return obj != null &&
               obj instanceof HadoopFileSystem &&
               this.fs.getUri().compareTo(((HadoopFileSystem)obj).fs.getUri()) == 0;
	}
}
