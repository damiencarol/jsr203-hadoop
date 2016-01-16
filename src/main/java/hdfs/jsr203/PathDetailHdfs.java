package hdfs.jsr203;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PathDetailHdfs {
	private static Properties properties;
	
	static {
		initial();
	}
	private static void initial() {
		String configPath = "config.properties";
		InputStream in = PathDetailHdfs.class.getClassLoader().getResourceAsStream(configPath);
		properties = new Properties();
		try {
			properties.load(in);
		} catch (IOException e1) {
			e1.printStackTrace();
			throw new RuntimeException(e1);
		} finally{
			try {
				if(in != null){
					in.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static String getHdpHdfsXml() {
		return properties.getProperty("hdfs-xml");
	}
	public static String getHdpCoreXml() {
		return properties.getProperty("hdfs-core-xml");
	}
	public static String getHdpYarnXml() {
		return properties.getProperty("yarn-xml");
	}
	public static String getHdpHdfsHeadSymbol() {
		return properties.getProperty("hdfsHeadSymbol");
	}
}
