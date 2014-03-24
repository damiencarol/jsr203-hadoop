package hdfs.jsr203;

import org.apache.hadoop.security.UserGroupInformation;


public class HadoopUserPrincipal implements java.nio.file.attribute.UserPrincipal {

	private UserGroupInformation ugi;
	private HadoopFileSystem hdfs;

	public HadoopUserPrincipal(HadoopFileSystem hdfs, String name) {
		this.ugi = UserGroupInformation.createRemoteUser(name);
		this.hdfs = hdfs;
	}

	@Override
	public String getName() {
		return this.ugi.getUserName();
	}

}
