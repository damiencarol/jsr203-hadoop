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

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TestWatchService extends TestHadoop {

  private static MiniDFSCluster cluster;
  private static URI clusterUri;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    cluster = startMini(TestWatchService.class.getName());
    clusterUri = formalizeClusterURI(cluster.getFileSystem().getUri());
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private static MiniDFSCluster startMini(String testName) throws IOException {
    File baseDir = new File("./target/hdfs/" + testName).getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);
    Configuration conf = new Configuration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
    MiniDFSCluster hdfsCluster = builder.clusterId(testName).build();
    hdfsCluster.waitActive();
    return hdfsCluster;
  }
 /* 
  static void checkKey(WatchKey key, Path dir) {
    if (!key.isValid())
        throw new RuntimeException("Key is not valid");
    if (key.watchable() != dir)
        throw new RuntimeException("Unexpected watchable");
}*/

static void takeExpectedKey(WatchService watcher, WatchKey expected) {
    System.out.println("take events...");
    WatchKey key;
    try {
        key = watcher.take();
        System.out.println("key taken: " + key);
    } catch (InterruptedException x) {
        // not expected
        throw new RuntimeException(x);
    }
    System.out.println("expected key: " + expected);
    if (key != expected) {
        throw new RuntimeException("removed unexpected key");
    }
}

static void checkExpectedEvent(Iterable<WatchEvent<?>> events,
                               WatchEvent.Kind<?> expectedKind,
                               Object expectedContext)
{
    WatchEvent<?> event = events.iterator().next();
    System.out.format("got event: type=%s, count=%d, context=%s\n",
        event.kind(), event.count(), event.context());
    if (event.kind() != expectedKind)
        throw new RuntimeException("unexpected event");
    if (!expectedContext.equals(event.context()))
        throw new RuntimeException("unexpected context");
}

  /**
   * Simple test of each of the standard events
   * <p>
   * This is a complete test that checks many feature.
   * TODO split this one in 3 test (create/modify/delete).
   */
  @Test
  public void testEvents() throws IOException {
    Path dir = Paths.get(clusterUri);
    System.out.println("-- Standard Events --");


    Path name = dir.resolve("foo");
    System.out.println("name: " + name);

    try (WatchService watcher = dir.getFileSystem().newWatchService()) {
      // --- ENTRY_CREATE ---

      // register for event
      System.out.format("register %s for ENTRY_CREATE\n", dir);
      WatchKey myKey = dir.register(watcher,
          new WatchEvent.Kind<?>[] { ENTRY_CREATE });
      Assert.assertTrue(myKey.isValid());
      Assert.assertEquals(dir, myKey.watchable());

      // create file
      Path file = dir.resolve("foo");
      System.out.format("create %s\n", file);
      Files.createFile(file);

      // remove key and check that we got the ENTRY_CREATE event
      takeExpectedKey(watcher, myKey);
      checkExpectedEvent(myKey.pollEvents(),
          StandardWatchEventKinds.ENTRY_CREATE, name);

      System.out.println("reset key");
      if (!myKey.reset())
        throw new RuntimeException("key has been cancalled");

      System.out.println("OKAY");

      // --- ENTRY_DELETE ---

      System.out.format("register %s for ENTRY_DELETE\n", dir);
      WatchKey deleteKey = dir.register(watcher,
          new WatchEvent.Kind<?>[] { ENTRY_DELETE });
      if (deleteKey != myKey)
        throw new RuntimeException("register did not return existing key");
      Assert.assertTrue(deleteKey.isValid());
      Assert.assertEquals(dir, deleteKey.watchable());

      System.out.format("delete %s\n", file);
      Files.delete(file);
      takeExpectedKey(watcher, myKey);
      checkExpectedEvent(myKey.pollEvents(),
          StandardWatchEventKinds.ENTRY_DELETE, name);

      System.out.println("reset key");
      if (!myKey.reset())
        throw new RuntimeException("key has been cancalled");

      System.out.println("OKAY");

      // create the file for the next test
      Files.createFile(file);

      // --- ENTRY_MODIFY ---

      System.out.format("register %s for ENTRY_MODIFY\n", dir);
      WatchKey newKey = dir.register(watcher,
          new WatchEvent.Kind<?>[] { ENTRY_MODIFY });
      if (newKey != myKey)
        throw new RuntimeException("register did not return existing key");
      Assert.assertTrue(newKey.isValid());
      Assert.assertEquals(dir, newKey.watchable());

      System.out.format("update: %s\n", file);
      try (OutputStream out = Files.newOutputStream(file,
          StandardOpenOption.APPEND)) {
        out.write("I am a small file".getBytes("UTF-8"));
      }

      // remove key and check that we got the ENTRY_MODIFY event
      takeExpectedKey(watcher, myKey);
      checkExpectedEvent(myKey.pollEvents(),
          StandardWatchEventKinds.ENTRY_MODIFY, name);
      System.out.println("OKAY");

      // done
      Files.delete(file);
    }
  }
  
  @Test
  public void testSimple() throws IOException {
    Path rootPath = Paths.get(clusterUri);
    
    WatchService watcher = rootPath.getFileSystem().newWatchService();
    rootPath.register(watcher, 
          new WatchEvent.Kind<?>[] { ENTRY_MODIFY });
    watcher.close();
  }

  @Test(expected=ClosedWatchServiceException.class)
  public void testSimpleEx() throws IOException {
    Path rootPath = Paths.get(clusterUri);
    
    WatchService watcher = rootPath.getFileSystem().newWatchService();
    rootPath.register(watcher, 
          new WatchEvent.Kind<?>[] { ENTRY_MODIFY });
    watcher.close();
    // Should throw ClosedWatchServiceException
    watcher.poll();
  }
}
