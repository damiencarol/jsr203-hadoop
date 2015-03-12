package hdfs.jsr203;

import java.io.IOException;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.TimeUnit;

public class HadoopWatchService implements WatchService {

	private HadoopFileSystem fileSystem;

	public HadoopWatchService(HadoopFileSystem fileSystem) {
		this.fileSystem = fileSystem;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public WatchKey poll() {
		try {
			return new HadoopWatchKey(this, (HadoopPath) this.fileSystem.getPath("/"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public WatchKey poll(long timeout, TimeUnit unit)
			throws InterruptedException {
		try {
			return new HadoopWatchKey(this, (HadoopPath) this.fileSystem.getPath("/"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public WatchKey take() throws InterruptedException {
		try {
			return new HadoopWatchKey(this, (HadoopPath) this.fileSystem.getPath("/"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
