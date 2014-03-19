package testhdfs;

import hdfs.jsr203.HadoopFileSystemProvider;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.spi.FileSystemProvider;

import junit.framework.TestCase;

public class HadoopFileSystemTest extends TestCase {
	
	private static final int port = 8020;
	public static String host = "nc-h04";

	/**
	 * Check that a FileSystemProvider handle 'hdfs' scheme.
	 */
	public void testAutoRegister() {
		
		boolean found = false;
		for (FileSystemProvider fp : FileSystemProvider.installedProviders())
			if (fp.getScheme().equals(HadoopFileSystemProvider.SCHEME))
				found = true;
		// Check auto register of the provider
		assertTrue(found);
	}

	public void testCreateTemp() throws URISyntaxException, IOException {
		URI uri = new URI("hdfs://" + host + ":" + port + "/tmp/toto");
		Path path = Paths.get(uri);
		Files.createDirectory(path);
		assertTrue(Files.exists(path));
	}
}
