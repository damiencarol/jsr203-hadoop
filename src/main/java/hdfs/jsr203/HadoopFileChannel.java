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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;

public class HadoopFileChannel extends FileChannel {

    private SeekableByteChannel internalChannel;
    
    public HadoopFileChannel(SeekableByteChannel newByteChannel) {
        this.internalChannel = newByteChannel;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return this.internalChannel.read(dst);
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        throw new IOException("Imcomplete implementation");
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        throw new IOException("Imcomplete implementation");
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        throw new IOException("Imcomplete implementation");
    }

    @Override
    public long position() throws IOException {
        return this.internalChannel.position();
    }

    @Override
    public FileChannel position(long newPosition) throws IOException {
        this.internalChannel.position(newPosition);
        return this;
    }

    @Override
    public long size() throws IOException {
        return this.internalChannel.size();
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
        throw new IOException("Imcomplete implementation");
    }

    @Override
    public void force(boolean metaData) throws IOException {
        throw new IOException("Imcomplete implementation");
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        throw new IOException("Imcomplete implementation");
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        throw new IOException("Imcomplete implementation");
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        this.internalChannel = this.internalChannel.position(position);
        return this.internalChannel.read(dst);
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        throw new IOException("Imcomplete implementation");
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
        throw new IOException("Imcomplete implementation");
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException {
        throw new IOException("Imcomplete implementation");
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        throw new IOException("Imcomplete implementation");
    }

    @Override
    protected void implCloseChannel() throws IOException {
        this.internalChannel.close();
    }

}
