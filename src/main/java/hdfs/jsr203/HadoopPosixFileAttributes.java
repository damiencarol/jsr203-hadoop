package hdfs.jsr203;

import java.io.IOException;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

public class HadoopPosixFileAttributes extends HadoopFileAttributes implements PosixFileAttributes {

	private HadoopFileSystem hdfs;

	public HadoopPosixFileAttributes(HadoopFileSystem hdfs, FileStatus fileStatus) {
		super(fileStatus);
		this.hdfs = hdfs;
	}

	@Override
	public UserPrincipal owner() {
		try {
			return this.hdfs.getUserPrincipalLookupService().lookupPrincipalByName(getFileStatus().getOwner());
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public GroupPrincipal group() {
		try {
			return this.hdfs.getUserPrincipalLookupService().lookupPrincipalByGroupName(getFileStatus().getGroup());
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public Set<PosixFilePermission> permissions() {
		return toPosixFilePermissionSet(getFileStatus().getPermission());
	}

	private Set<PosixFilePermission> toPosixFilePermissionSet(FsPermission fsPermission){
		String perms = fsPermission.getUserAction().SYMBOL +
				fsPermission.getGroupAction().SYMBOL +
				fsPermission.getOtherAction().SYMBOL;
		return PosixFilePermissions.fromString(perms);
	}

}
