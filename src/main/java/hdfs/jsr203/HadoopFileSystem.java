package hdfs.jsr203;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

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
		// TODO : manage read only
        //checkWritable();

		this.fs.delete(hadoopPath, failIfNotExists);
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
}
