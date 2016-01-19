/*
 * Copyright 2016 Damien Carol <damien.carol@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package hdfs.jsr203;

import java.io.IOException;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.TimeUnit;

public class HadoopWatchService implements WatchService {

	private HadoopFileSystem fileSystem;

	public HadoopWatchService(HadoopFileSystem fileSystem) {
		this.fileSystem = fileSystem;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public WatchKey poll() {
		try {
			return new HadoopWatchKey(this, (HadoopPath) this.fileSystem.getPath("/"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public WatchKey poll(long timeout, TimeUnit unit)
			throws InterruptedException {
		try {
			return new HadoopWatchKey(this, (HadoopPath) this.fileSystem.getPath("/"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public WatchKey take() throws InterruptedException {
		try {
			return new HadoopWatchKey(this, (HadoopPath) this.fileSystem.getPath("/"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
