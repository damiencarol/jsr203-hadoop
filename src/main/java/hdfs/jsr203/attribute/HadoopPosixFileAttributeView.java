package hdfs.jsr203.attribute;

import hdfs.jsr203.HadoopPath;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;

public class HadoopPosixFileAttributeView implements PosixFileAttributeView {
	
	private final HadoopPath path;

	public HadoopPosixFileAttributeView(HadoopPath path) {
		this.path = path;
	}

	@Override
	public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime,
			FileTime createTime) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public UserPrincipal getOwner() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setOwner(UserPrincipal owner) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public String name() {
		return "posix";
	}

	@Override
	public PosixFileAttributes readAttributes() throws IOException {
		FileStatus fileStatus = path.getFileSystem().getHDFS().getFileStatus(path.getRawResolvedPath());
		return new HadoopPosixFileAttributes(this.path.getFileSystem(), fileStatus);
	}

	@Override
	public void setPermissions(Set<PosixFilePermission> perms)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setGroup(GroupPrincipal group) throws IOException {
		// TODO Auto-generated method stub

	}

}
