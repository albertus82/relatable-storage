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

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

/**
 * Tests the CountingInputStream.
 */
class CountingInputStreamTest {

	@Test
	void testCounting() throws Exception {
		final String text = "A piece of text";
		try (CountingInputStream cis = new CountingInputStream(new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8)))) {

			// have to declare this larger as we're going to read
			// off the end of the stream and input stream seems
			// to do bounds checking
			final byte[] result = new byte[21];

			final byte[] ba = new byte[5];
			int found = cis.read(ba);
			System.arraycopy(ba, 0, result, 0, 5);
			assertEquals(found, cis.getCount());

			final int value = cis.read();
			found++;
			result[5] = (byte) value;
			assertEquals(found, cis.getCount());

			found += cis.read(result, 6, 5);
			assertEquals(found, cis.getCount());

			found += cis.read(result, 11, 10); // off the end
			assertEquals(found, cis.getCount());

			// trim to get rid of the 6 empty values
			final String textResult = new String(result).trim();
			assertEquals(textResult, text);
		}
	}

	@Test
	void testEOF1() throws Exception {
		final ByteArrayInputStream bais = new ByteArrayInputStream(new byte[2]);
		try (CountingInputStream cis = new CountingInputStream(bais)) {

			int found = cis.read();
			assertEquals(0, found);
			assertEquals(1, cis.getCount());
			found = cis.read();
			assertEquals(0, found);
			assertEquals(2, cis.getCount());
			found = cis.read();
			assertEquals(-1, found);
			assertEquals(2, cis.getCount());
		}
	}

	@Test
	void testEOF2() throws Exception {
		final ByteArrayInputStream bais = new ByteArrayInputStream(new byte[2]);
		try (CountingInputStream cis = new CountingInputStream(bais)) {

			final byte[] result = new byte[10];

			final int found = cis.read(result);
			assertEquals(2, found);
			assertEquals(2, cis.getCount());
		}
	}

	@Test
	void testEOF3() throws Exception {
		final ByteArrayInputStream bais = new ByteArrayInputStream(new byte[2]);
		try (CountingInputStream cis = new CountingInputStream(bais)) {

			final byte[] result = new byte[10];

			final int found = cis.read(result, 0, 5);
			assertEquals(2, found);
			assertEquals(2, cis.getCount());
		}
	}

	@Test
	void testSkipping() throws IOException {
		final String text = "Hello World!";
		try (CountingInputStream cis = new CountingInputStream(new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8)))) {

			assertEquals(6, cis.skip(6));
			assertEquals(6, cis.getCount());
			final byte[] result = new byte[6];
			cis.read(result);

			assertEquals("World!", new String(result));
			assertEquals(12, cis.getCount());
		}
	}

	@Test
	void testZeroLength1() throws Exception {
		final ByteArrayInputStream bais = new ByteArrayInputStream(IOUtils.EMPTY_BYTE_ARRAY);
		try (CountingInputStream cis = new CountingInputStream(bais)) {

			final int found = cis.read();
			assertEquals(-1, found);
			assertEquals(0, cis.getCount());
		}
	}

	@Test
	void testZeroLength2() throws Exception {
		final ByteArrayInputStream bais = new ByteArrayInputStream(IOUtils.EMPTY_BYTE_ARRAY);
		try (CountingInputStream cis = new CountingInputStream(bais)) {

			final byte[] result = new byte[10];

			final int found = cis.read(result);
			assertEquals(-1, found);
			assertEquals(0, cis.getCount());
		}
	}

	@Test
	void testZeroLength3() throws Exception {
		final ByteArrayInputStream bais = new ByteArrayInputStream(IOUtils.EMPTY_BYTE_ARRAY);
		try (CountingInputStream cis = new CountingInputStream(bais)) {

			final byte[] result = new byte[10];

			final int found = cis.read(result, 0, 5);
			assertEquals(-1, found);
			assertEquals(0, cis.getCount());
		}
	}

	@Test
	void testMarkReset() throws IOException {
		final String text = "Hello World!";
		try (CountingInputStream cis = new CountingInputStream(new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8)))) {
			assertThrows(IOException.class, () -> cis.reset());

			final byte[] result = new byte[text.length()];

			cis.read(result, 0, 3);
			cis.mark(text.length());

			assertEquals(3, cis.getCount());

			final int n = cis.read(result);
			assertEquals(-1, cis.read());
			assertEquals(text.length() - 3, n);
			assertEquals(text.length(), cis.getCount());

			cis.reset();
			assertEquals(3, cis.getCount());
		}
	}

}
