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
import java.net.URISyntaxException;
import java.nio.file.ClosedDirectoryStreamException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Implements {@link DirectoryStream}.
 */
public class HadoopDirectoryStream implements DirectoryStream<Path> {
  private final HadoopFileSystem hadoopfs;
  private final HadoopPath path;
  private final DirectoryStream.Filter<? super Path> filter;
  private volatile boolean isClosed;
  private volatile Iterator<Path> itr;

  HadoopDirectoryStream(HadoopPath hadoopPath,
                        DirectoryStream.Filter<? super java.nio.file.Path> filter)
      throws IOException {
    this.hadoopfs = hadoopPath.getFileSystem();
    this.path = hadoopPath;
    this.filter = filter;
  }

  @Override
  public synchronized Iterator<Path> iterator() {
    if (isClosed) {
      throw new ClosedDirectoryStreamException();
    }
    if (itr != null) {
      throw new IllegalStateException("Iterator has already been returned");
    }
    try {
      itr = hadoopfs.iteratorOf(path, filter);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    } catch (URISyntaxException e) {
      throw new IllegalStateException(e);
    }
    return new Iterator<Path>() {
      @Override
      public boolean hasNext() {
        if (isClosed) {
          return false;
        }
        return itr.hasNext();
      }

      @Override
      public synchronized Path next() {
        if (isClosed) {
          throw new NoSuchElementException();
        }
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