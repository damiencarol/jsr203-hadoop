package hdfs.jsr203;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.Watchable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.Event.CreateEvent;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;

public class HadoopWatchKey implements WatchKey {

	private HadoopFileSystem fileSystem;
	private HdfsAdmin dfs;
	private DFSInotifyEventInputStream stream;
	private HadoopWatchService watcher;
	private HadoopPath path;

	public HadoopWatchKey(HadoopWatchService watcher, HadoopPath path) throws IOException {
		this.watcher = watcher;
		this.path = path;
		
		URI uri = path.getFileSystem().getHDFS().getUri();
		this.dfs = new HdfsAdmin(uri,
				path.getFileSystem().getHDFS().getConf());
		this.stream = this.dfs.getInotifyEventStream();
	}

	@Override
	public boolean isValid() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public List<WatchEvent<?>> pollEvents() {
		
		ArrayList<WatchEvent<?>> ls = new ArrayList<WatchEvent<?>>();
		Event raw_event = null;
		try {
			raw_event = this.stream.poll();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MissingEventsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (raw_event != null) {
			WatchEvent<Path> event = buildFromHadoop(raw_event);
			if (event != null)
				ls.add(event);
		}
		return ls;
	}

	private WatchEvent<Path> buildFromHadoop(Event raw_event) {
		switch (raw_event.getEventType()) {
		case CREATE:
			CreateEvent createEvent = (CreateEvent) raw_event;
			HadoopPath hadoopPath = new HadoopPath(fileSystem, 
					createEvent.getPath().getBytes());
			return new HadoopCreateWatchEvent(hadoopPath, StandardWatchEventKinds.ENTRY_CREATE);
		default:
			System.err.println("Eventype not know: " + raw_event.getEventType().name());
			return null;
		}
	}

	@Override
	public boolean reset() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public void cancel() {
		// TODO Auto-generated method stub

	}

	@Override
	public Watchable watchable() {
		// TODO Auto-generated method stub
		return this.path;
	}

}
