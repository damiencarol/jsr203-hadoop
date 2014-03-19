package test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Test {
	
	private static final int port = 8020;
	public static String host = "nc-h04";

	public static void main(String[] args) throws URISyntaxException, IOException
	{
		/*String path_hadoop = "hdfs://nc-h04:8020/user/hduser/achat.txt";
		
		//Path p5 = Paths.get(System.getProperty("user.home"),"logs", "foo.log");
		Path p5 = Paths.get(new URI(path_hadoop));
		
		FileSystem fs = p5.getFileSystem();
		System.out.println("File " + p5.getFileName() + " " + Files.exists(p5));
		System.out.println(fs.provider().getScheme());*/
		
		URI uri = new URI("hdfs://" + host + ":" + port + "/toto");
		Path path = Paths.get(uri);
		Files.createDirectory(path);
		System.out.println(Files.exists(path));
		
		Path path2 = Paths.get(new URI("hdfs://" + host + ":" + port + "/tmp/10/part-r-00000"));
		System.out.println("Size: " + Files.size(path2));
	}

}
