package hdfs.jsr203;

import java.net.URI;
import java.net.URISyntaxException;

public abstract class TestHadoop {
    protected static URI formalizeClusterURI(URI clusterUri) throws URISyntaxException {
    	if (clusterUri.getPath()==null) {
        	return new URI(clusterUri.getScheme(), null,
        			clusterUri.getHost(), clusterUri.getPort(),
        			"/", null, null);
        } else if (clusterUri.getPath().trim()=="") {
        	return new URI(clusterUri.getScheme(), null,
        			clusterUri.getHost(), clusterUri.getPort(),
        			"/", null, null);
        }
		return clusterUri;
	}
}
