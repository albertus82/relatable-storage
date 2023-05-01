package io.github.albertus82.filestore.jdbc;

import java.util.UUID;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.github.albertus82.filestore.util.UUIDConverter;

class UUIDConverterTest {

	@Test
	void test() {
		final UUID a = UUID.randomUUID();
		final String s = UUIDConverter.toBase64Url(a);
		Assertions.assertEquals(22, s.length());
		final UUID b = UUIDConverter.fromBase64Url(s);
		Assertions.assertEquals(a, b);
	}

}
