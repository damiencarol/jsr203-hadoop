package testhdfs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

public class FileSystemTest {

	private static int port = 8020;
	private static String host = "nc-h04";

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		port = 8020;
		host = "nc-h04";
	}

	@Test
	public void testProvider() throws URISyntaxException {
		URI uri = new URI("hdfs://" + host + ":" + port + "/tmp/test_file");
		Path path = Paths.get(uri);
		assertNotNull(path.getFileSystem().provider());
	}

	@Test(expected = NoSuchFileException.class)
	public void testNoSuchFileExceptionOnDelete() throws URISyntaxException,
			IOException {
		URI uri = new URI("hdfs://" + host + ":" + port + "/tmp/test_file");
		Path path = Paths.get(uri);
		Files.createFile(path);
		assertTrue(Files.exists(path));
		Files.delete(path);
		assertFalse(Files.exists(path));

		Files.delete(path); // this one generate the exception

	}

	@Test(expected = DirectoryNotEmptyException.class)
	public void testDirectoryNotEmptyExceptionOnDelete()
			throws URISyntaxException, IOException {
		// Create the dir
		URI uriDir = new URI("hdfs://" + host + ":" + port + "/tmp/test_dir");
		Path pathDir = Paths.get(uriDir);
		Files.createDirectory(pathDir);
		assertTrue(Files.exists(pathDir));
		// Create the file
		URI uri = new URI("hdfs://" + host + ":" + port
				+ "/tmp/test_dir/test_file");
		Path path = Paths.get(uri);
		Files.createFile(path);
		assertTrue(Files.exists(path));

		Files.delete(pathDir); // this one generate the exception

	}

	/*
	 * try { assert } catch (NoSuchFileException x) {
	 * System.err.format("%s: no such" + " file or directory%n", path); } catch
	 * (DirectoryNotEmptyException x) { System.err.format("%s not empty%n",
	 * path); } catch (IOException x) { // File permission problems are caught
	 * here. System.err.println(x); }
	 */

	@Test
	public void testsetLastModifiedTime() throws URISyntaxException,
			IOException {
		URI uri = new URI("hdfs://" + host + ":" + port + "/tmp/test_file");
		Path file = Paths.get(uri);
		Files.createFile(file);
		assertTrue(Files.exists(file));
		BasicFileAttributes attr = Files.readAttributes(file,
				BasicFileAttributes.class);
		assertNotNull(attr);
		long currentTime = System.currentTimeMillis();
		FileTime ft = FileTime.fromMillis(currentTime);
		Files.setLastModifiedTime(file, ft);
		// TODO : fix that
		//Assert.assertEquals(ft, Files.getLastModifiedTime(file));
	}
	
	/*@Test
	public void testsetLastModifiedTime() throws URISyntaxException,
			IOException {
		URI uri = new URI("hdfs://" + host + ":" + port + "/tmp/test_file");
		Path file = Paths.get(uri);
		Files.createFile(file);
		assertTrue(Files.exists(file));
		BasicFileAttributes attr = Files.readAttributes(file,
				BasicFileAttributes.class);
		assertNotNull(attr);
		long currentTime = System.currentTimeMillis();
		FileTime ft = FileTime.fromMillis(currentTime);
		Files.setLastModifiedTime(file, ft);
		Assert.assertEquals(ft, Files.getLastModifiedTime(file));
	}*/
}
