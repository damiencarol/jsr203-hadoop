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

import java.nio.file.Path;
import java.nio.file.WatchEvent;

/**
 * Implementation for {@link WatchEvent}.
 */
public class HadoopCreateWatchEvent implements WatchEvent<Path> {

  private final Path path;
  private final WatchEvent.Kind<Path> kind;

  HadoopCreateWatchEvent(Path path, WatchEvent.Kind<Path> kind) {
    this.path = path;
    this.kind = kind;
  }

  @Override
  public WatchEvent.Kind<Path> kind() {
    return this.kind;
  }

  @Override
  public int count() {
    return 1;
  }

  @Override
  public Path context() {
    return this.path;
  }

}
