package hdfs.jsr203;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsConfInitiator {
	private static Configuration conf;
	private static FileSystem fs;
	static {
		conf = new Configuration();
		conf.addResource(new Path( PathDetailHdfs.getHdpHdfsXml()));
		conf.addResource(new Path(PathDetailHdfs.getHdpCoreXml()));
		
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