package io.github.albertus82.storage.jdbc;

import java.math.BigInteger;
import java.util.UUID;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.github.albertus82.storage.util.UUIDUtils;

class UUIDUtilsTest {

	@Test
	void test() {
		BigInteger max = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16);
		Assertions.assertDoesNotThrow(() -> UUIDUtils.fromBigInteger(BigInteger.ZERO));
		Assertions.assertDoesNotThrow(() -> UUIDUtils.fromBigInteger(max));
		Assertions.assertThrows(IllegalArgumentException.class, () -> UUIDUtils.fromBigInteger(max.add(BigInteger.ONE)));
		Assertions.assertThrows(IllegalArgumentException.class, () -> UUIDUtils.fromBigInteger(BigInteger.ZERO.subtract(BigInteger.ONE)));
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
		final BigInteger i = UUIDUtils.toBigInteger(b);
		final UUID c = UUIDUtils.fromBigInteger(i);
		Assertions.assertEquals(a, c);
	}

}
