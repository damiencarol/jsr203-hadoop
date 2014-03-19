package hdfs.jsr203;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

public class HadoopFileSystem extends FileSystem {
	
	private org.apache.hadoop.fs.FileSystem fs;
	private FileSystemProvider provider;

	public HadoopFileSystem(FileSystemProvider provider, String host, int port) throws IOException {
		
		this.provider = provider;
		
		// Create dynamic configuration
		Configuration conf = new org.apache.hadoop.conf.Configuration();
		conf.set("fs.default.name", "hdfs://" + host + ":" + port + "");
		//conf.set("fs.defaultFS", "hdfs://" + host + ":" + port + "/");
        //conf.set("hadoop.job.ugi", "hbase");

        this.fs = org.apache.hadoop.fs.FileSystem.get(conf);
	}

	@Override
	public void close() throws IOException {
		this.fs.close();
	}

	@Override
	public Iterable<FileStore> getFileStores() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Path getPath(String first, String... more) {
		StringBuilder str = new StringBuilder(first);
		for (String more_item : more) {
			str.append(getSeparator());
			str.append(more_item);
		}
		try {
			return new HadoopPath(this, str.toString());
		} catch (URISyntaxException e) {
			throw new InvalidPathException("str.toString()", e.getReason());
		}
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public UserPrincipalLookupService getUserPrincipalLookupService() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isOpen() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isReadOnly() {
		// TODO Auto-generated method stub
		return false;
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

	@Override
	public Set<String> supportedFileAttributeViews() {
		// TODO Auto-generated method stub
		return null;
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
		this.fs.mkdirs(dir);
	}
}
