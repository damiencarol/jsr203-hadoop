package hdfs.jsr203.attribute;

import java.io.IOException;
import java.nio.file.LinkOption;
import java.util.Map;

public interface IAttributeReader {
	public Map<String, Object> readAttributes(String attributes, LinkOption[] options) throws IOException;
}
