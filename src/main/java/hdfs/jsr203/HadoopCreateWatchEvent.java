package hdfs.jsr203;

import java.nio.file.Path;
import java.nio.file.WatchEvent;

public class HadoopCreateWatchEvent implements WatchEvent<Path> {
	
	private HadoopPath path;
	private java.nio.file.WatchEvent.Kind<Path> kind;

	public HadoopCreateWatchEvent(HadoopPath path, java.nio.file.WatchEvent.Kind<Path> kind) {
		this.path = path;
		this.kind = kind;
	}

	@Override
	public WatchEvent.Kind<Path> kind() {
		return this.kind;
	}

	@Override
	public int count() {
		return 1;
	}

	@Override
	public HadoopPath context() {
		return this.path;
	}

}
