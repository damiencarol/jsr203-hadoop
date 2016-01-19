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

import java.io.IOException;
import java.net.URI;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.Watchable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.Event.AppendEvent;
import org.apache.hadoop.hdfs.inotify.Event.CloseEvent;
import org.apache.hadoop.hdfs.inotify.Event.CreateEvent;
import org.apache.hadoop.hdfs.inotify.Event.MetadataUpdateEvent;
import org.apache.hadoop.hdfs.inotify.Event.RenameEvent;
import org.apache.hadoop.hdfs.inotify.Event.UnlinkEvent;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;

/**
 * 
 */
public class HadoopWatchKey implements WatchKey {

	//private HadoopFileSystem fileSystem;
	private HdfsAdmin dfs;
	private DFSInotifyEventInputStream stream;
	private HadoopWatchService watcher;
	private HadoopPath path;

	public HadoopWatchKey(HadoopWatchService watcher, HadoopPath path) throws IOException {
		assert path != null;
		
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
		try {
			Event raw_event = null;
			while ((raw_event = this.stream.poll())!=null)
			{
				buildFromHadoop(raw_event, ls);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MissingEventsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ls;
	}

	private void buildFromHadoop(Event raw_event, ArrayList<WatchEvent<?>> ls) {
		switch (raw_event.getEventType()) {
		case APPEND:
			// Java 1.7 doesn't manage "APPEND" event
			// For append we translate to "MODIFY"
			AppendEvent appendEvent = (AppendEvent) raw_event;
			HadoopPath hadoopAppendPath = new HadoopPath(path.getFileSystem(), 
					appendEvent.getPath().getBytes());
			ls.add(new HadoopCreateWatchEvent(hadoopAppendPath, StandardWatchEventKinds.ENTRY_MODIFY));
			break;
		case CREATE:
			// Create events are OK
			CreateEvent createEvent = (CreateEvent) raw_event;
			HadoopPath hadoopPath = new HadoopPath(path.getFileSystem(), 
					createEvent.getPath().getBytes());
			ls.add(new HadoopCreateWatchEvent(path.relativize(hadoopPath), StandardWatchEventKinds.ENTRY_CREATE));
			break;
		case CLOSE:
			// Java 1.7 doesn't manage "CLOSE" event
			// For close we translate to "MODIFY"
			CloseEvent closeEvent = (CloseEvent) raw_event;
			HadoopPath hadoopClosePath = new HadoopPath(path.getFileSystem(), 
					closeEvent.getPath().getBytes());
			ls.add(new HadoopCreateWatchEvent(hadoopClosePath, StandardWatchEventKinds.ENTRY_MODIFY));
			break;
		case METADATA:
			// Java 1.7 doesn't manage "METADATA" event
			// For meta data we translate to "MODIFY"
			MetadataUpdateEvent metadataEvent = (MetadataUpdateEvent) raw_event;
			HadoopPath hadoopMetadataPath = new HadoopPath(path.getFileSystem(), 
					metadataEvent.getPath().getBytes());
			ls.add(new HadoopCreateWatchEvent(hadoopMetadataPath, StandardWatchEventKinds.ENTRY_MODIFY));
			break;
		case RENAME:
			// Java 1.7 doesn't manage "RENAME" event
			// For RENAME we translate to "DELETE" and "CREATE"
			RenameEvent renameEvent = (RenameEvent) raw_event;
			HadoopPath hadoopRenameSrcPath = new HadoopPath(path.getFileSystem(), 
					renameEvent.getSrcPath().getBytes());
			HadoopPath hadoopRenameDstPath = new HadoopPath(path.getFileSystem(), 
					renameEvent.getDstPath().getBytes());
			ls.add(new HadoopCreateWatchEvent(hadoopRenameSrcPath, StandardWatchEventKinds.ENTRY_DELETE));
			ls.add(new HadoopCreateWatchEvent(hadoopRenameDstPath, StandardWatchEventKinds.ENTRY_CREATE));
			break;
		case UNLINK:
			// Java 1.7 doesn't manage "UNLINK" event
			// For UNLINK we translate to "MODIFY"
			UnlinkEvent unlinkEvent = (UnlinkEvent) raw_event;
			HadoopPath hadoopUnlinkPath = new HadoopPath(path.getFileSystem(), 
					unlinkEvent.getPath().getBytes());
			ls.add(new HadoopCreateWatchEvent(hadoopUnlinkPath, StandardWatchEventKinds.ENTRY_MODIFY));
			break;
		default:
			System.err.println("Eventype not know: " + raw_event.getEventType().name());
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

	public HadoopWatchService getWatcher() {
		return watcher;
	}
}
