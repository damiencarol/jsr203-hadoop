package hdfs.jsr203;

import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileStatus;

public class HadoopFileAttributes implements BasicFileAttributes
{

	private FileStatus fileStatus;

	public HadoopFileAttributes(FileStatus fileStatus) {
		this.fileStatus = fileStatus;
	}

	public FileStatus getFileStatus() {
		return fileStatus;
	}

	public void setFileStatus(FileStatus fileStatus) {
		this.fileStatus = fileStatus;
	}

	@Override
	public FileTime creationTime() {
		return FileTime.from(this.fileStatus.getModificationTime(), TimeUnit.MILLISECONDS);
	}

	@Override
	public Object fileKey() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isDirectory() {
		return this.fileStatus.isDir();
	}

	@Override
	public boolean isOther() {
		return false;
	}

	@Override
	public boolean isRegularFile() {
		return !this.fileStatus.isDir();
	}

	@Override
	public boolean isSymbolicLink() {
		return false;
	}

	@Override
	public FileTime lastAccessTime() {
		return FileTime.from(this.fileStatus.getAccessTime(), TimeUnit.MILLISECONDS);
	}

	@Override
	public FileTime lastModifiedTime() {
		return FileTime.from(this.fileStatus.getModificationTime(), TimeUnit.MILLISECONDS);
	}

	@Override
	public long size() {
		return this.fileStatus.getLen();
	}

	@Override
	public String toString() {
		return "[IS DIR : " + this.fileStatus.isDir() + "]";
	}

}
