package hdfs.jsr203;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsConfInitiator {
	private static Configuration conf;
	private static FileSystem fs;
	private static String hadoophome;
	static {
		initialContext();
	
	}
	
	private static void initialContext() {
		hadoophome = System.getenv("HADOOP_HOME");
		if (hadoophome == null) {
			hadoophome = "/home/novelbio/software/hadoop/";
		} else {
			hadoophome = addSep(hadoophome);
		}
		
		if (!hadoophome.endsWith("/") && !hadoophome.endsWith("\\")) {
			hadoophome = hadoophome + File.separator;
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
	
	public static String getHadoopHome() {
		return hadoophome;
	}
	
	public static Configuration getConf() {
		return conf;
	}
	
	public static FileSystem getHdfs() {
		return fs;
	}
	
	private static String addSep(String path) {
		path = path.trim();
		if (!path.endsWith(File.separator)) {
			if (!path.equals("")) {
				path = path + File.separator;
            }
		}
		return path;
	}
}