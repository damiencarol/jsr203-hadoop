package hdfs.jsr203;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.FileAttribute;
import java.util.Iterator;
import java.util.Set;

public class HadoopPath implements Path {

	private String internalPath;
	private HadoopFileSystem hdfs;
	private org.apache.hadoop.fs.Path hadoopPath;

	/*
	 * public HadoopPath(HadoopFileSystem hdfs, String path) { this(hdfs, new
	 * org.apache.hadoop.fs.Path(path)); }
	 */

	/*
	 * public HadoopPath(HadoopFileSystem hdfs, URI uri) { this(hdfs,
	 * uri.getPath()); }
	 */

	public HadoopPath(HadoopFileSystem hdfs2, org.apache.hadoop.fs.Path path) {
		this(hdfs2, path.toUri().getPath());
	}

	public HadoopPath(HadoopFileSystem hdfs, String path) {
		this.hdfs = hdfs;
		this.hadoopPath = new org.apache.hadoop.fs.Path("hdfs://"
				+ hdfs.getHost() + ":" + hdfs.getPort() + path);
		this.internalPath = path;
	}

	HadoopFileAttributes getAttributes() throws IOException {
		// this.hdfs.fs.getFileAttributes(this);//getResolvedPath());

		HadoopFileAttributes hfas = new HadoopFileAttributes(this.hdfs
				.getHDFS().getFileStatus(hadoopPath));
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
        return this.hadoopPath.compareTo(o.hadoopPath);
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getNameCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Path getParent() {
		return new HadoopPath(this.hdfs, this.hadoopPath.getParent());
	}

	@Override
	public Path getRoot() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isAbsolute() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Iterator<Path> iterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Path normalize() {
		// TODO Auto-generated method stub
		return null;
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Path resolve(String other) {
		// TODO Auto-generated method stub
		return null;
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
	public Path subpath(int beginIndex, int endIndex) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Path toAbsolutePath() {
		// TODO Auto-generated method stub
		return null;
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
		try {
			return new URI(HadoopFileSystemProvider.SCHEME,
					this.hdfs.getHost(), this.internalPath);
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		org.apache.hadoop.fs.Path hdfs_path = this.hadoopPath;
		// ZipFileAttributes attrs = zfs.getFileAttributes(getResolvedPath());
		// if (attrs == null && (path.length != 1 || path[0] != '/'))
		if (!this.hdfs.getHDFS().exists(hdfs_path))
			throw new NoSuchFileException(toString());
		if (w) {
			// if (zfs.isReadOnly())
			throw new AccessDeniedException(toString());
		}
		if (x)
			throw new AccessDeniedException(toString());
	}

	void createDirectory(FileAttribute<?>... attrs) throws IOException {
		this.hdfs.createDirectory(this.hadoopPath, attrs);
	}

	@Override
	public String toString() {
		return this.hadoopPath.toString();
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
	public org.apache.hadoop.fs.Path getRawPath() {
		return this.hadoopPath;
	}

	void delete() throws IOException {
		this.hdfs.deleteFile(this.hadoopPath, true);
	}

	void deleteIfExists() throws IOException {
		this.hdfs.deleteFile(this.hadoopPath, false);
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
		return this.hdfs.newByteChannel(this.hadoopPath, options, attrs);
	}

	/*private HadoopPath getResolvedPath() {
		return this;
	}*/
}
