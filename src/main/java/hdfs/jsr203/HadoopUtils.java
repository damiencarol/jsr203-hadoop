package hdfs.jsr203;

import java.util.Arrays;

public class HadoopUtils {

	/**
     * Append a slash at the end, if it does not have one yet
     */
    public static byte[] toDirectoryPath(byte[] dir) {
        if (dir.length != 0 && dir[dir.length - 1] != '/') {
            dir = Arrays.copyOf(dir, dir.length + 1);
            dir[dir.length - 1] = '/';
        }
        return dir;
    }
}
