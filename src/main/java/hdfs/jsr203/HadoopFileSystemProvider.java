package hdfs.jsr203;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;
import java.util.Set;

public class HadoopFileSystemProvider extends FileSystemProvider {

	public static final String SCHEME = "hdfs";

	// Checks that the given file is a HadoopPath
    static final HadoopPath toHadoopPath(Path path) {
        if (path == null)
            throw new NullPointerException();
        if (!(path instanceof HadoopPath))
            throw new ProviderMismatchException();
        return (HadoopPath)path;
    }
	
	@Override
	public void checkAccess(Path path, AccessMode... modes) throws IOException {
		toHadoopPath(path).checkAccess(modes);
	}

	@Override
	public void copy(Path source, Path target, CopyOption... options)
			throws IOException {
		toHadoopPath(source).copy(toHadoopPath(target), options);
	}

	@Override
	public void createDirectory(Path dir, FileAttribute<?>... attrs)
			throws IOException {
		toHadoopPath(dir).createDirectory(attrs);
	}

	@Override
	public void delete(Path path) throws IOException {
		toHadoopPath(path).delete();
	}

	@Override
	public <V extends FileAttributeView> V getFileAttributeView(Path path,
			Class<V> type, LinkOption... options) {
		return HadoopFileAttributeView.get(toHadoopPath(path), type);
	}

	@Override
	public FileStore getFileStore(Path path) throws IOException {
		return toHadoopPath(path).getFileStore();
	}

	@Override
	public FileSystem getFileSystem(URI uri) {
		try {
			int port = uri.getPort();
			if (port == -1)
				port = 8020; // Default hadoop port
			
			return new HadoopFileSystem(this, uri.getHost(), port);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Path getPath(URI uri) {
		return getFileSystem(uri).getPath(uri.getPath());
	}

	@Override
	public String getScheme() {
		return SCHEME;
	}

	@Override
	public boolean isHidden(Path path) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isSameFile(Path path, Path path2) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void move(Path source, Path target, CopyOption... options)
			throws IOException {
		toHadoopPath(source).move(toHadoopPath(target), options);
	}

	@Override
	public SeekableByteChannel newByteChannel(Path path,
			Set<? extends OpenOption> options, FileAttribute<?>... attrs)
			throws IOException {
		return toHadoopPath(path).newByteChannel(options, attrs);
	}

	@Override
	public DirectoryStream<Path> newDirectoryStream(Path dir,
			Filter<? super Path> filter) throws IOException {
		return toHadoopPath(dir).newDirectoryStream(filter);
	}

	@Override
	public FileSystem newFileSystem(URI uri, Map<String, ?> env)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <A extends BasicFileAttributes> A readAttributes(Path path,
			Class<A> type, LinkOption... options) throws IOException {
		
		if (type == BasicFileAttributes.class || type == HadoopFileAttributes.class)
            return (A)toHadoopPath(path).getAttributes();
        
		throw new UnsupportedOperationException();
	}

	@Override
	public Map<String, Object> readAttributes(Path path, String attributes,
			LinkOption... options) throws IOException
	{
		return toHadoopPath(path).readAttributes(attributes, options);
	}

	@Override
	public void setAttribute(Path path, String attribute, Object value,
			LinkOption... options) throws IOException {
		// TODO Auto-generated method stub
		
	}

}
