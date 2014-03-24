package hdfs.jsr203;

import java.nio.file.attribute.GroupPrincipal;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.HadoopLoginModule;

public class HadoopGroupPrincipal implements GroupPrincipal {

	private UserGroupInformation ugi;
	private HadoopFileSystem hdfs;

	public HadoopGroupPrincipal(HadoopFileSystem hdfs, String name) {
		this.ugi = UserGroupInformation.createRemoteUser(name);
		this.hdfs = hdfs;
	}

	@Override
	public String getName() {
		return this.ugi.getUserName();
	}

}
