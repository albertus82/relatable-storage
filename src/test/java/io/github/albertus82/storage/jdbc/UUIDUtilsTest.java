package io.github.albertus82.storage.jdbc;

import java.util.UUID;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.github.albertus82.storage.util.UUIDUtils;

class UUIDUtilsTest {

	@Test
	void test() {
		test(new UUID(0, 0));
		for (short i = 0; i < Short.MAX_VALUE; i++) {
			test(UUID.randomUUID());
		}
		test(new UUID(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL));
	}

	private static void test(final UUID a) {
		final String s = UUIDUtils.toBase64Url(a);
		Assertions.assertEquals(22, s.length());
		final UUID b = UUIDUtils.fromBase64Url(s);
		Assertions.assertEquals(a, b);
	}

}
