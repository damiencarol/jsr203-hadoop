/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hdfs.jsr203.attribute;

import hdfs.jsr203.HadoopFileSystem;

import java.io.IOException;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;

public class HadoopUserPrincipalLookupService extends
		UserPrincipalLookupService {

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
		return new HadoopGroupPrincipal(this.hdfs, group);
	}

}
