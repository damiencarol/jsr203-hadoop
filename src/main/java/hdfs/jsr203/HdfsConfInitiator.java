package hdfs.jsr203;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsConfInitiator {
	private static Configuration conf;
	private static FileSystem fs;
	private static String hadoophome = System.getenv("HADOOP_HOME");
	static {
		initialContext();
	
	}
	
	private static void initialContext() {
		if (!hadoophome.endsWith("/") && !hadoophome.endsWith("\\")) {
			hadoophome = hadoophome + File.pathSeparator;
		}
		
		conf = new Configuration();
		conf.addResource(new Path( hadoophome + "etc/hadoop/hdfs-site.xml"));
		conf.addResource(new Path( hadoophome + "etc/hadoop/core-site.xml"));
		
		conf.set("dfs.permissions.enabled", "false");
		conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER"); 
		conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true"); 
		try {
	        fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("cannot initial hadoop fs", e);
        }
	}
	
	public static Configuration getConf() {
		return conf;
	}
	
	public static FileSystem getHdfs() {
		return fs;
	}
}