package io.github.albertus82.relatastor.jdbc;

import java.util.UUID;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.github.albertus82.relatastor.util.UUIDUtils;

class UUIDUtilsTest {

	@Test
	void test() {
		final UUID a = UUID.randomUUID();
		final String s = UUIDUtils.toBase64Url(a);
		Assertions.assertEquals(22, s.length());
		final UUID b = UUIDUtils.fromBase64Url(s);
		Assertions.assertEquals(a, b);
	}

}
