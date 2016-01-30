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
//import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;

/**
 * Implementation of {@link WatchKey}.
 */
public class HadoopWatchKey implements WatchKey {

  private HadoopWatchService watcher;
  private HadoopPath path;
  private DFSInotifyEventInputStream stream;

  public HadoopWatchKey(HadoopWatchService watcher, HadoopPath path, DFSInotifyEventInputStream streampar)
      throws IOException {
    assert path != null;

    this.watcher = watcher;
    this.path = path;

    URI uri = path.getFileSystem().getHDFS().getUri();
    stream = streampar;
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
      /*
       * For Hadoop 2.7.1 EventBatch raw_batch = null; while ((raw_batch =
       * this.stream.poll())!=null) { for (Event raw_event :
       * raw_batch.getEvents()) { buildFromHadoop(raw_event, ls); } }
       */
      // Hadoop 2.6.0
      Event rawEvent = null;
      while ((rawEvent = this.stream.poll()) != null) {
        buildFromHadoop(rawEvent, ls);
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

  private void buildFromHadoop(Event event, ArrayList<WatchEvent<?>> ls) {
    switch (event.getEventType()) {
    case APPEND:
      // Java 1.7 doesn't manage "APPEND" event
      // For append we translate to "MODIFY"
      AppendEvent appendEvent = (AppendEvent) event;
      HadoopPath hadoopAppendPath = new HadoopPath(path.getFileSystem(),
          appendEvent.getPath().getBytes());
      ls.add(new HadoopCreateWatchEvent(hadoopAppendPath,
          StandardWatchEventKinds.ENTRY_MODIFY));
      break;
    case CREATE:
      // Create events are OK
      CreateEvent createEvent = (CreateEvent) event;
      HadoopPath hadoopPath = new HadoopPath(path.getFileSystem(),
          createEvent.getPath().getBytes());
      ls.add(new HadoopCreateWatchEvent(path.relativize(hadoopPath),
          StandardWatchEventKinds.ENTRY_CREATE));
      break;
    case CLOSE:
      // Java 1.7 doesn't manage "CLOSE" event
      // For close we translate to "MODIFY"
      CloseEvent closeEvent = (CloseEvent) event;
      HadoopPath hadoopClosePath = new HadoopPath(path.getFileSystem(),
          closeEvent.getPath().getBytes());
      ls.add(new HadoopCreateWatchEvent(hadoopClosePath,
          StandardWatchEventKinds.ENTRY_MODIFY));
      break;
    case METADATA:
      // Java 1.7 doesn't manage "METADATA" event
      // For meta data we translate to "MODIFY"
      MetadataUpdateEvent metadataEvent = (MetadataUpdateEvent) event;
      HadoopPath hadoopMetadataPath = new HadoopPath(path.getFileSystem(),
          metadataEvent.getPath().getBytes());
      ls.add(new HadoopCreateWatchEvent(hadoopMetadataPath,
          StandardWatchEventKinds.ENTRY_MODIFY));
      break;
    case RENAME:
      // Java 1.7 doesn't manage "RENAME" event
      // For RENAME we translate to "DELETE" and "CREATE"
      RenameEvent renameEvent = (RenameEvent) event;
      HadoopPath hadoopRenameSrcPath = new HadoopPath(path.getFileSystem(),
          renameEvent.getSrcPath().getBytes());
      HadoopPath hadoopRenameDstPath = new HadoopPath(path.getFileSystem(),
          renameEvent.getDstPath().getBytes());
      ls.add(new HadoopCreateWatchEvent(hadoopRenameSrcPath,
          StandardWatchEventKinds.ENTRY_DELETE));
      ls.add(new HadoopCreateWatchEvent(hadoopRenameDstPath,
          StandardWatchEventKinds.ENTRY_CREATE));
      break;
    case UNLINK:
      // Java 1.7 doesn't manage "UNLINK" event
      // For UNLINK we translate to "MODIFY"
      UnlinkEvent unlinkEvent = (UnlinkEvent) event;
      HadoopPath hadoopUnlinkPath = new HadoopPath(path.getFileSystem(),
          unlinkEvent.getPath().getBytes());
      ls.add(new HadoopCreateWatchEvent(hadoopUnlinkPath,
          StandardWatchEventKinds.ENTRY_MODIFY));
      break;
    default:
      System.err.println("Eventype not know: " + event.getEventType().name());
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

  @Override
  public String toString() {
    return "HadoopWatchKey [stream=" + stream + ", watcher="
        + watcher + ", path=" + path + "]";
  }
}
