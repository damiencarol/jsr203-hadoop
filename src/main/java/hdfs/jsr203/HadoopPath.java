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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileStore;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;

/**
 * Implement {@link Path}.
 */
public class HadoopPath implements Path {

  private byte[] path;
  /** Store offsets of '/' chars. */
  private volatile int[] offsets;
  private String internalPath;
  private final HadoopFileSystem hdfs;
  private int hashcode = 0; // cached hash code (created lazily)

  HadoopPath(HadoopFileSystem hdfs, byte[] path) {
    this(hdfs, path, false);
  }

  HadoopPath(HadoopFileSystem hdfs, byte[] path, boolean normalized) {
    assert hdfs != null;

    this.hdfs = hdfs;
    if (normalized) {
      this.path = path;
    } else {
      this.path = normalize(path);
    }
    // TODO : add charset management
    this.internalPath = new String(path);
  }

  HadoopBasicFileAttributes getAttributes() throws IOException {
    org.apache.hadoop.fs.Path resolvedPath = this.getRawResolvedPath();
    FileStatus fileStatus = hdfs.getHDFS().getFileStatus(resolvedPath);
    String fileKey = resolvedPath.toString();
    return new HadoopBasicFileAttributes(fileKey, fileStatus);
  }

  PosixFileAttributes getPosixAttributes() throws IOException {
    return this.hdfs.getPosixFileAttributes(this);
  }

  private HadoopPath checkPath(Path pathToCheck) {
    if (pathToCheck == null) {
      throw new NullPointerException();
    }
    if (!(pathToCheck instanceof HadoopPath)) {
      throw new ProviderMismatchException();
    }
    return (HadoopPath) pathToCheck;
  }

  @Override
  public int hashCode() {
    int h = hashcode;
    if (h == 0) {
      hashcode = Arrays.hashCode(path);
    }
    return hashcode;
  }

  @Override
  public boolean equals(Object obj) {
    return obj != null && obj instanceof HadoopPath
        && this.hdfs.equals(((HadoopPath) obj).hdfs)
        && compareTo((Path) obj) == 0;
  }

  @Override
  public int compareTo(Path other) {
    final HadoopPath o = checkPath(other);
    return this.internalPath.compareTo(o.internalPath);
  }

  @Override
  public boolean endsWith(Path other) {
    final HadoopPath o = checkPath(other);
    int olast = o.path.length - 1;
    if (olast > 0 && o.path[olast] == '/') {
      olast--;
    }
    int last = this.path.length - 1;
    if (last > 0 && this.path[last] == '/')
      last--;
    if (olast == -1) // o.path.length == 0
      return last == -1;
    if ((o.isAbsolute() && (!this.isAbsolute() || olast != last))
        || (last < olast))
      return false;
    for (; olast >= 0; olast--, last--) {
      if (o.path[olast] != this.path[last]) {
        return false;
      }
    }
    return o.path[olast + 1] == '/' || last == -1 || this.path[last] == '/';
  }

  @Override
  public boolean endsWith(String other) {
    return endsWith(getFileSystem().getPath(other));
  }

  @Override
  public Path getFileName() {
    initOffsets();
    int count = offsets.length;
    if (count == 0) {
      return null; // no elements so no name
    }
    if (count == 1 && path[0] != '/') {
      return this;
    }
    int lastOffset = offsets[count - 1];
    int len = path.length - lastOffset;
    byte[] result = new byte[len];
    System.arraycopy(path, lastOffset, result, 0, len);
    return new HadoopPath(this.hdfs, result);
  }

  @Override
  public HadoopFileSystem getFileSystem() {
    return this.hdfs;
  }

  @Override
  public Path getName(int index) {
    initOffsets();
    if (index < 0 || index >= offsets.length)
      throw new IllegalArgumentException();
    int begin = offsets[index];
    int len;
    if (index == (offsets.length - 1))
      len = path.length - begin;
    else
      len = offsets[index + 1] - begin - 1;
    // construct result
    byte[] result = new byte[len];
    System.arraycopy(path, begin, result, 0, len);
    return new HadoopPath(this.hdfs, result);
  }

  @Override
  public int getNameCount() {
    initOffsets();
    return offsets.length;
  }

  @Override
  public Path getParent() {
    initOffsets();
    int count = offsets.length;
    if (count == 0) // no elements so no parent
      return null;
    int len = offsets[count - 1] - 1;
    if (len <= 0) // parent is root only (may be null)
      return getRoot();
    byte[] result = new byte[len];
    System.arraycopy(path, 0, result, 0, len);
    return new HadoopPath(this.hdfs, result);
  }

  @Override
  public Path getRoot() {
    if (this.isAbsolute())
      return new HadoopPath(this.hdfs, new byte[] { path[0] });
    else
      return null;
  }

  @Override
  public boolean isAbsolute() {
    return (this.path.length > 0 && path[0] == '/');
  }

  @Override
  public Iterator<Path> iterator() {
    return asList().iterator();
  }

  private List<Path> asList() {
    return new AbstractList<Path>() {
      @Override
      public Path get(int index) {
        return getName(index);
      }

      @Override
      public int size() {
        return getNameCount();
      }
    };
  }

  @Override
  public Path normalize() {
    byte[] resolved = getResolved();
    if (resolved == path) // no change
      return this;
    return new HadoopPath(this.hdfs, resolved, true);
  }

  // removes redundant slashs, replace "\" to hadoop separator "/"
  // and check for invalid characters
  private byte[] normalize(byte[] path) {
    if (path.length == 0)
      return path;
    byte prevC = 0;
    for (int i = 0; i < path.length; i++) {
      byte c = path[i];
      if (c == '\\')
        return normalize(path, i);
      if (c == (byte) '/' && prevC == '/')
        return normalize(path, i - 1);
      if (c == '\u0000')
        throw new InvalidPathException(this.hdfs.getString(path),
            "Path: nul character not allowed");
      prevC = c;
    }
    return path;
  }

  private byte[] normalize(byte[] path, int off) {
    byte[] to = new byte[path.length];
    int n = 0;
    while (n < off) {
      to[n] = path[n];
      n++;
    }
    int m = n;
    byte prevC = 0;
    while (n < path.length) {
      byte c = path[n++];
      if (c == (byte) '\\')
        c = (byte) '/';
      if (c == (byte) '/' && prevC == (byte) '/')
        continue;
      if (c == '\u0000')
        throw new InvalidPathException(this.hdfs.getString(path),
            "Path: nul character not allowed");
      to[m++] = c;
      prevC = c;
    }
    if (m > 1 && to[m - 1] == '/')
      m--;
    return (m == to.length) ? to : Arrays.copyOf(to, m);
  }

  @Override
  public WatchKey register(WatchService watcher, Kind<?>... events)
      throws IOException {
    return register(watcher, events, new WatchEvent.Modifier[0]);
  }

  @Override
  public WatchKey register(WatchService watcher, Kind<?>[] events,
      Modifier... modifiers) throws IOException {
    if (watcher == null || events == null || modifiers == null) {
      throw new NullPointerException();
    }

    // Not implemented now
    // The Hadoop API for notification is not stable
    throw new IOException("Not implemented");
  }

  private boolean equalsNameAt(HadoopPath other, int index) {
    int mbegin = offsets[index];
    int mlen;
    if (index == (offsets.length - 1))
      mlen = path.length - mbegin;
    else
      mlen = offsets[index + 1] - mbegin - 1;
    int obegin = other.offsets[index];
    int olen;
    if (index == (other.offsets.length - 1))
      olen = other.path.length - obegin;
    else
      olen = other.offsets[index + 1] - obegin - 1;
    if (mlen != olen)
      return false;
    int n = 0;
    while (n < mlen) {
      if (path[mbegin + n] != other.path[obegin + n])
        return false;
      n++;
    }
    return true;
  }

  @Override
  public Path relativize(Path other) {
    final HadoopPath o = checkPath(other);
    if (o.equals(this))
      return new HadoopPath(getFileSystem(), new byte[0], true);
    if (/* this.getFileSystem() != o.getFileSystem() || */
    this.isAbsolute() != o.isAbsolute()) {
      throw new IllegalArgumentException();
    }
    int mc = this.getNameCount();
    int oc = o.getNameCount();
    int n = Math.min(mc, oc);
    int i = 0;
    while (i < n) {
      if (!equalsNameAt(o, i))
        break;
      i++;
    }
    int dotdots = mc - i;
    int len = dotdots * 3 - 1;
    if (i < oc)
      len += (o.path.length - o.offsets[i] + 1);
    byte[] result = new byte[len];

    int pos = 0;
    while (dotdots > 0) {
      result[pos++] = (byte) '.';
      result[pos++] = (byte) '.';
      if (pos < len) // no tailing slash at the end
        result[pos++] = (byte) '/';
      dotdots--;
    }
    if (i < oc)
      System.arraycopy(o.path, o.offsets[i], result, pos,
          o.path.length - o.offsets[i]);
    return new HadoopPath(getFileSystem(), result);
  }

  @Override
  public Path resolve(Path other) {
    final HadoopPath o = checkPath(other);
    if (o.isAbsolute())
      return o;
    byte[] resolved;
    if (this.path.length == 0) {
      // this method contract explicitly specifies this behavior in the case of
      // an empty path
      return o;
    } else if (this.path[path.length - 1] == '/') {
      resolved = new byte[path.length + o.path.length];
      System.arraycopy(path, 0, resolved, 0, path.length);
      System.arraycopy(o.path, 0, resolved, path.length, o.path.length);
    } else {
      resolved = new byte[path.length + 1 + o.path.length];
      System.arraycopy(path, 0, resolved, 0, path.length);
      resolved[path.length] = '/';
      System.arraycopy(o.path, 0, resolved, path.length + 1, o.path.length);
    }
    return new HadoopPath(hdfs, resolved);
  }

  @Override
  public Path resolve(String other) {
    return resolve(getFileSystem().getPath(other));
  }

  @Override
  public Path resolveSibling(Path other) {
    if (other == null)
      throw new NullPointerException();
    Path parent = getParent();
    return (parent == null) ? other : parent.resolve(other);
  }

  @Override
  public Path resolveSibling(String other) {
    return resolveSibling(getFileSystem().getPath(other));
  }

  @Override
  public boolean startsWith(Path other) {
    final HadoopPath o = checkPath(other);
    if (o.isAbsolute() != this.isAbsolute() || o.path.length > this.path.length)
      return false;
    int olast = o.path.length;
    for (int i = 0; i < olast; i++) {
      if (o.path[i] != this.path[i])
        return false;
    }
    olast--;
    return o.path.length == this.path.length || o.path[olast] == '/'
        || this.path[olast + 1] == '/';
  }

  @Override
  public boolean startsWith(String other) {
    return startsWith(getFileSystem().getPath(other));
  }

  @Override
  public HadoopPath subpath(int beginIndex, int endIndex) {
    initOffsets();
    if (beginIndex < 0 || beginIndex >= offsets.length
        || endIndex > offsets.length || beginIndex >= endIndex)
      throw new IllegalArgumentException();

    // starting offset and length
    int begin = offsets[beginIndex];
    int len;
    if (endIndex == offsets.length)
      len = path.length - begin;
    else
      len = offsets[endIndex] - begin - 1;
    // construct result
    byte[] result = new byte[len];
    System.arraycopy(path, begin, result, 0, len);
    return new HadoopPath(this.hdfs, result);
  }

  @Override
  public HadoopPath toAbsolutePath() {
    if (isAbsolute()) {
      return this;
    } else {
      // add / before the existing path
      byte[] defaultdir = "/".getBytes(); // this.hdfs.getDefaultDir().path;
      int defaultlen = defaultdir.length;
      boolean endsWith = (defaultdir[defaultlen - 1] == '/');
      byte[] t;
      if (endsWith)
        t = new byte[defaultlen + path.length];
      else
        t = new byte[defaultlen + 1 + path.length];
      System.arraycopy(defaultdir, 0, t, 0, defaultlen);
      if (!endsWith)
        t[defaultlen++] = '/';
      System.arraycopy(path, 0, t, defaultlen, path.length);
      return new HadoopPath(this.hdfs, t, true); // normalized
    }
  }

  @Override
  public File toFile() {
    // No, just no.
    throw new UnsupportedOperationException();
  }

  @Override
  public Path toRealPath(LinkOption... options) throws IOException {
    HadoopPath realPath = new HadoopPath(this.hdfs, getResolvedPath())
        .toAbsolutePath();
    return realPath;
  }

  @Override
  public URI toUri() {
    try {
      return new URI(HadoopFileSystemProvider.SCHEME, null, hdfs.getHost(),
          hdfs.getPort(), new String(toAbsolutePath().path), null, null);
    } catch (Exception ex) {
      throw new AssertionError(ex);
    }
  }

  void createDirectory(FileAttribute<?>... attrs) throws IOException {
    this.hdfs.createDirectory(getResolvedPath(), attrs);
  }

  @Override
  public String toString() {
    // TODO add char set management
    return new String(this.path, StandardCharsets.UTF_8);
  }

  DirectoryStream<Path> newDirectoryStream(Filter<? super Path> filter)
      throws IOException {
    return new HadoopDirectoryStream(this, filter);
  }

  /**
   * Helper to get the raw interface of HDFS path.
   * 
   * @return raw HDFS path object
   */
  public org.apache.hadoop.fs.Path getRawResolvedPath() {
    return new org.apache.hadoop.fs.Path("hdfs://" + hdfs.getHost() + ":"
        + hdfs.getPort() + new String(getResolvedPath()));
  }

  void delete() throws IOException {
    this.hdfs.deleteFile(getRawResolvedPath(), true);
  }

  void deleteIfExists() throws IOException {
    this.hdfs.deleteFile(getRawResolvedPath(), false);
  }

  void move(HadoopPath target, CopyOption... options) throws IOException {
    if (this.hdfs.sameCluster(target.hdfs)) {
      this.hdfs.moveFile(getResolvedPath(), target.getResolvedPath(), options);
    } else {
      copyToTarget(target, options);
      delete();
    }
  }

  SeekableByteChannel newByteChannel(Set<? extends OpenOption> options,
      FileAttribute<?>... attrs) throws IOException {
    return this.hdfs.newByteChannel(getRawResolvedPath(), options, attrs);
  }

  FileChannel newFileChannel(Set<? extends OpenOption> options,
      FileAttribute<?>[] attrs) throws IOException {
    return this.hdfs.newFileChannel(getRawResolvedPath(), options, attrs);
  }

  // the result path does not contain ./ and .. components
  private volatile byte[] resolved = null;

  byte[] getResolvedPath() {
    byte[] r = resolved;
    if (r == null) {
      if (isAbsolute())
        r = getResolved();
      else
        r = toAbsolutePath().getResolvedPath();
      // if (r[0] == '/')
      // r = Arrays.copyOfRange(r, 1, r.length);
      resolved = r;
    }
    return resolved;
  }

  // Remove DotSlash(./) and resolve DotDot (..) components
  private byte[] getResolved() {
    if (path.length == 0)
      return path;
    for (int i = 0; i < path.length; i++) {
      byte c = path[i];
      if (c == (byte) '.')
        return resolve0();
    }
    return path;
  }

  // TBD: performance, avoid initOffsets
  private byte[] resolve0() {
    byte[] to = new byte[path.length];
    int nc = getNameCount();
    int[] lastM = new int[nc];
    int lastMOff = -1;
    int m = 0;
    for (int i = 0; i < nc; i++) {
      int n = offsets[i];
      int len = (i == offsets.length - 1) ? (path.length - n)
          : (offsets[i + 1] - n - 1);
      if (len == 1 && path[n] == (byte) '.') {
        if (m == 0 && path[0] == '/') // absolute path
          to[m++] = '/';
        continue;
      }
      if (len == 2 && path[n] == '.' && path[n + 1] == '.') {
        if (lastMOff >= 0) {
          m = lastM[lastMOff--]; // retreat
          continue;
        }
        if (path[0] == '/') { // "/../xyz" skip
          if (m == 0)
            to[m++] = '/';
        } else { // "../xyz" -> "../xyz"
          if (m != 0 && to[m - 1] != '/')
            to[m++] = '/';
          while (len-- > 0)
            to[m++] = path[n++];
        }
        continue;
      }
      if (m == 0 && path[0] == '/' || // absolute path
          m != 0 && to[m - 1] != '/') { // not the first name
        to[m++] = '/';
      }
      lastM[++lastMOff] = m;
      while (len-- > 0)
        to[m++] = path[n++];
    }
    if (m > 1 && to[m - 1] == '/')
      m--;
    return (m == to.length) ? to : Arrays.copyOf(to, m);
  }

  public void setTimes(FileTime mtime, FileTime atime, FileTime ctime)
      throws IOException {
    this.hdfs.setTimes(getResolvedPath(), mtime, atime, ctime);
  }

  FileStore getFileStore() throws IOException {
    // each HadoopFileSystem only has one root (for each cluster)
    if (exists())
      return hdfs.getFileStore(this);
    throw new NoSuchFileException(this.hdfs.getString(path));
  }

  boolean exists() {
    // Root case
    if ("/".equals(internalPath))
      return true;
    try {
      return hdfs.exists(getRawResolvedPath());
    } catch (IOException x) {
    }
    return false;
  }

  // create offset list if not already created
  private void initOffsets() {
    if (offsets == null) {
      int count, index;
      // count names
      count = 0;
      index = 0;
      while (index < path.length) {
        byte c = path[index++];
        if (c != '/') {
          count++;
          while (index < path.length && path[index] != '/')
            index++;
        }
      }
      // populate offsets
      int[] result = new int[count];
      count = 0;
      index = 0;
      while (index < path.length) {
        byte c = path[index];
        if (c == '/') {
          index++;
        } else {
          result[count++] = index++;
          while (index < path.length && path[index] != '/')
            index++;
        }
      }
      synchronized (this) {
        if (offsets == null)
          offsets = result;
      }
    }
  }

  void copy(HadoopPath target, CopyOption... options) throws IOException {
    if (this.hdfs.sameCluster(target.hdfs))
      this.hdfs.copyFile(false, getResolvedPath(), target.getResolvedPath(),
          options);
    else
      copyToTarget(target, options);
  }

  private void copyToTarget(HadoopPath target, CopyOption... options)
      throws IOException {
    throw new IOException("Copy beetween cluster is not implemented");
  }
}
