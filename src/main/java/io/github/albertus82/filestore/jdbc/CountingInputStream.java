/*
 * Copyright (C) 2007 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.github.albertus82.filestore.jdbc;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * An {@link InputStream} that counts the number of bytes read.
 *
 * @author Chris Nokleberg
 */
class CountingInputStream extends FilterInputStream {

	private static final byte EOF = -1;

	private long count;
	private Long mark;

	/**
	 * Wraps another input stream, counting the number of bytes read.
	 *
	 * @param in the input stream to be wrapped
	 */
	CountingInputStream(final InputStream in) {
		super(Objects.requireNonNull(in, "in must not be null"));
	}

	/**
	 * Returns the number of bytes read.
	 *
	 * @return the number of bytes read
	 */
	public long getCount() {
		return count;
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
