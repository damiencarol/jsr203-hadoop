package test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestLisDir {

	private static final String host = "nc-h04";
	private static final int port = 8020;

	public static void main(String[] args) throws IOException {
		 Configuration conf = new Configuration();
		 conf.set("fs.default.name", "hdfs://" + host + ":" + port + "");
		FileSystem fs = FileSystem.get(conf );
         FileStatus[] status = fs.listStatus(new Path("hdfs://nc-h04:8020/tmp"));
         for (int i=0;i<status.length;i++){
                 /*BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                 String line;
                 line=br.readLine();
                 while (line != null){
                         System.out.println(line);
                         line=br.readLine();
                 }*/
        	 System.out.println(status[i].getPath() + " is dir ? " + status[i].isDir());
         }
	}

}
