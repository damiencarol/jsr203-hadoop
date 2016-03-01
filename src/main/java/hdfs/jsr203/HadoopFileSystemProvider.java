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
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.spi.FileSystemProvider;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.PathFilter;

/**
 * {@link FileSystemProvider} implementation for HDFS.
 * <p>
 * This provider implements the actual {@code META-INF/services/} entry.
 */
public class HadoopFileSystemProvider extends FileSystemProvider {

  public static final String SCHEME = "hdfs";

  // Copy-cat of
  // org.apache.hadoop.mapreduce.lib.input.FileInputFormat.hiddenFileFilter
  private static final PathFilter HIDDEN_FILE_FILTER = new PathFilter() {
    public boolean accept(org.apache.hadoop.fs.Path p) {
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  // Checks that the given file is a HadoopPath
  static final HadoopPath toHadoopPath(Path path) {
    if (path == null) {
      throw new NullPointerException();
    }
    if (!(path instanceof HadoopPath)) {
      throw new ProviderMismatchException();
    }
    return (HadoopPath) path;
  }

  @Override
  public void checkAccess(Path path, AccessMode... modes) throws IOException {
    toHadoopPath(path).getFileSystem().checkAccess(toHadoopPath(path), modes);
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
    return toHadoopPath(path).getFileSystem().getView(toHadoopPath(path),
        type);
  }

  @Override
  public FileStore getFileStore(Path path) throws IOException {
    return toHadoopPath(path).getFileStore();
  }

  @Override
  public FileSystem getFileSystem(URI uri) {
    try {
      return newFileSystem(uri, Collections.<String, Object>emptyMap());
    } catch (IOException e) {
      throw new FileSystemNotFoundException(e.getMessage());
    }
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
    return !HIDDEN_FILE_FILTER.accept(toHadoopPath(path).getRawResolvedPath());
  }

  @Override
  public boolean isSameFile(Path path, Path path2) throws IOException {
    return toHadoopPath(path).compareTo(toHadoopPath(path2)) == 0;
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
  public FileChannel newFileChannel(Path path,
      Set<? extends OpenOption> options, FileAttribute<?>... attrs)
      throws IOException {
    return toHadoopPath(path).newFileChannel(options, attrs);
  }

  @Override
  public DirectoryStream<Path> newDirectoryStream(Path dir,
      Filter<? super Path> filter) throws IOException {
    return toHadoopPath(dir).newDirectoryStream(filter);
  }

  @Override
  public FileSystem newFileSystem(URI uri, Map<String, ?> env)
      throws IOException {
    return new HadoopFileSystem(this, uri.getHost(), uri.getPort());
  }

  @SuppressWarnings("unchecked")
  @Override
  public <A extends BasicFileAttributes> A readAttributes(Path path,
      Class<A> type, LinkOption... options)
      throws IOException {

    if (type == BasicFileAttributes.class ||
        type == HadoopBasicFileAttributes.class) {
      return (A) toHadoopPath(path).getAttributes();
    }

    if (type == PosixFileAttributes.class) {
      return (A) toHadoopPath(path).getPosixAttributes();
    }

    throw new UnsupportedOperationException("readAttributes:" + type.getName());
  }

  @Override
  public Map<String, Object> readAttributes(Path path, String attributes,
      LinkOption... options) throws IOException {
    return toHadoopPath(path).getFileSystem().readAttributes(
        toHadoopPath(path), attributes, options);
  }

  @Override
  public void setAttribute(Path path, String attribute, Object value,
      LinkOption... options) throws IOException {
    toHadoopPath(path).getFileSystem().setAttribute(toHadoopPath(path),
        attribute, value, options);
  }

  @Override
  public void createLink(Path link, Path existing) throws IOException {
    toHadoopPath(existing).getFileSystem().getHDFS()
      .createSymlink(toHadoopPath(existing).getRawResolvedPath(), 
          toHadoopPath(existing).getRawResolvedPath(), false);
  }
}
