package hdfs.jsr203;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.ClosedDirectoryStreamException;
import java.nio.file.DirectoryStream;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class HadoopDirectoryStream implements DirectoryStream<Path> 
{
    private final HadoopFileSystem hadoopfs;
    //private final byte[] path;
	private final HadoopPath path;
    private final DirectoryStream.Filter<? super Path> filter;
    private volatile boolean isClosed;
    private volatile Iterator<Path> itr;

    HadoopDirectoryStream(HadoopPath hadoopPath,
                       DirectoryStream.Filter<? super java.nio.file.Path> filter)
        throws IOException
    {
        this.hadoopfs = hadoopPath.getFileSystem();
        //this.path = hadoopPath.getResolvedPath();
    	this.path = hadoopPath;
        this.filter = filter;
        // sanity check
        if (!hadoopPath.getAttributes().isDirectory())
            throw new NotDirectoryException(hadoopPath.toString());
    }

    @Override
    public synchronized Iterator<Path> iterator() {
        if (isClosed)
            throw new ClosedDirectoryStreamException();
        if (itr != null)
            throw new IllegalStateException("Iterator has already been returned");

        try {
        	itr = hadoopfs.iteratorOf(path, filter);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
		}
        return new Iterator<Path>() {
            private Path next;
            @Override
            public boolean hasNext() {
                if (isClosed)
                    return false;
                return itr.hasNext();
            }

            @Override
            public synchronized Path next() {
                if (isClosed)
                    throw new NoSuchElementException();
                return itr.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public synchronized void close() throws IOException {
        isClosed = true;
    }
}