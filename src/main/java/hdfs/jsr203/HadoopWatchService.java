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
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;

/**
 * Implement {@link WatchService}.
 */
public class HadoopWatchService implements WatchService {

  private DFSInotifyEventInputStream stream;
  public DFSInotifyEventInputStream getStream() {
    return stream;
  }

  public void setStream(DFSInotifyEventInputStream stream) {
    this.stream = stream;
  }

  private boolean isClosed;
  private HadoopFileSystem fileSystem;

  public HadoopWatchService(HadoopFileSystem fileSystem) throws IOException {
    this.fileSystem = fileSystem;
    HdfsAdmin dfs = new HdfsAdmin(fileSystem.getHDFS().getUri(), fileSystem.getHDFS().getConf());
    stream = dfs.getInotifyEventStream();
  }

  @Override
  public void close() throws IOException {
    this.isClosed=true;
  }
  
  /**
   * Checks that the watch service is open, throwing {@link ClosedWatchServiceException} if not.
   */
  protected final void checkOpen() {
    if (isClosed) {
      throw new ClosedWatchServiceException();
    }
  }

  @Override
  public WatchKey poll() {
    checkOpen();
    try {
      return new HadoopWatchKey(this,
          (HadoopPath) this.fileSystem.getPath("/"),stream);
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
      return new HadoopWatchKey(this,
          (HadoopPath) this.fileSystem.getPath("/"),stream);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public WatchKey take() throws InterruptedException {
    try {
      return new HadoopWatchKey(this,
          (HadoopPath) this.fileSystem.getPath("/"),stream);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

}
