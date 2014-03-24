package hdfs.jsr203;

import java.io.IOException;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;

import org.apache.hadoop.security.UserGroupInformation;

public class HadoopUserPrincipalLookupService extends UserPrincipalLookupService {

	private HadoopFileSystem hdfs;

	public HadoopUserPrincipalLookupService(HadoopFileSystem hadoopFileSystem) {
		this.hdfs = hadoopFileSystem;
	}

	@Override
	public UserPrincipal lookupPrincipalByName(String name) throws IOException {	
		return new HadoopUserPrincipal(this.hdfs, name);
	}

	@Override
	public GroupPrincipal lookupPrincipalByGroupName(String group)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
