package hdfs.jsr203.attribute;

import java.io.IOException;
import java.nio.file.LinkOption;

public interface IAttributeWriter {

	void setAttribute(String attr, Object value, LinkOption[] options) throws IOException;

}
