/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.albertus82.filestore.io;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * An {@link InputStream} that counts the number of bytes read.
 * <p>
 * Inspired by the homonymous classes contained in <em>Google Guava</em> and
 * <em>Apache Commons IO</em> libraries. Many thanks to the original authors.
 *
 * @see <a href="https://github.com/google/guava">Google Guava</a>
 * @see <a href="https://commons.apache.org/io/">Apache Commons IO</a>
 */
public class CountingInputStream extends FilterInputStream {

	private static final byte EOF = -1;

	private long count;
	private Long mark;

	/**
	 * Wraps another input stream, counting the number of bytes read.
	 *
	 * @param in the input stream to be wrapped
	 */
	public CountingInputStream(final InputStream in) {
		super(Objects.requireNonNull(in, "InputStream must not be null"));
	}

	/**
	 * Returns the number of bytes read.
	 *
	 * @return the number of bytes read.
	 */
	public long getCount() {
		return count;
	}

	/**
	 * Set the byte count back to 0.
	 *
	 * @return the count previous to resetting.
	 */
	public synchronized long resetCount() {
		final long prev = count;
		count = 0;
		return prev;
	}

	@Override
	public int read() throws IOException {
		final int result = super.read();
		if (result != EOF) {
			count++;
		}
		return result;
	}

	@Override
	public int read(final byte[] b, final int off, final int len) throws IOException {
		final int result = super.read(b, off, len);
		if (result != EOF) {
			count += result;
		}
		return result;
	}

	@Override
	public long skip(final long n) throws IOException {
		final long result = super.skip(n);
		count += result;
		return result;
	}

	@Override
	public synchronized void mark(final int readlimit) {
		super.mark(readlimit);
		mark = count;
		// it's okay to mark even if mark isn't supported, as reset won't work
	}

	@Override
	public synchronized void reset() throws IOException {
		if (!super.markSupported()) {
			throw new IOException("Mark not supported");
		}
		if (mark == null) {
			throw new IOException("Mark not set");
		}
		super.reset();
		count = mark;
	}

}
