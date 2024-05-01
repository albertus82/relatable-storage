package io.github.albertus82.storage.util;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.UUID;

/**
 * Utility methods for manipulating {@link UUID}s.
 *
 * @see <a href="https://www.ietf.org/rfc/rfc4122.txt">RFC 4122 - A Universally
 *      Unique IDentifier (UUID) URN Namespace</a>
 */
public class UUIDUtils {

	private static final BigInteger B = BigInteger.ONE.shiftLeft(64);
	private static final BigInteger L = BigInteger.valueOf(Long.MAX_VALUE);
	private static final BigInteger X = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16);

	private static final Encoder encoder = Base64.getUrlEncoder().withoutPadding();
	private static final Decoder decoder = Base64.getUrlDecoder();

	private UUIDUtils() {}

	/**
	 * Decodes a <strong>base64url</strong> encoded {@link UUID} object into a new
	 * UUID object.
	 *
	 * @param encodedUUID the string representing the UUID to decode, e.g.
	 *        {@code S2LzZ8f5S_6e5fT_p5N0Hw}
	 *
	 * @return A new UUID object constructed using the decoded bytes.
	 *
	 * @throws IllegalArgumentException if the argument is not in valid
	 *         <strong>base64url</strong> scheme as defined in RFC 4648.
	 *
	 * @see #toBase64Url(UUID)
	 * @see <a href="https://www.ietf.org/rfc/rfc4648.txt">RFC 4648 - The Base16,
	 *      Base32, and Base64 Data Encodings</a>
	 */
	public static UUID fromBase64Url(final String encodedUUID) {
		final LongBuffer buf = ByteBuffer.wrap(decoder.decode(encodedUUID)).asLongBuffer();
		return new UUID(buf.get(0), buf.get(1));
	}

	/**
	 * Encodes the specified {@link UUID} into a String using the
	 * <strong>base64url</strong> encoding scheme, as defined in RFC 4648.
	 *
	 * @param uuid the UUID object to encode
	 *
	 * @return A String containing the resulting <strong>base64url</strong> encoded
	 *         characters, e.g. {@code S2LzZ8f5S_6e5fT_p5N0Hw}
	 *
	 * @see #fromBase64Url(String)
	 * @see <a href="https://www.ietf.org/rfc/rfc4648.txt">RFC 4648 - The Base16,
	 *      Base32, and Base64 Data Encodings</a>
	 */
	public static String toBase64Url(final UUID uuid) {
		return encoder.encodeToString(ByteBuffer.allocate(Long.BYTES * 2).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()).array());
	}

	/**
	 * Converts the specified number into a new UUID object.
	 * 
	 * @param num the numeric representation of the UUID, e.g.
	 *        {@code 281831033846701114428451845565925261418}
	 * 
	 * @return A new UUID object based on the integer value.
	 * 
	 * @throws IllegalArgumentException if the argument cannot be converted to UUID.
	 * 
	 * @see <a href=
	 *      "https://gist.github.com/drmalex07/9008c611ffde6cb2ef3a2db8668bc251">drmalex07/convertUuidToBigInteger.java
	 *      - GitHub</a>
	 */
	public static UUID fromBigInteger(final BigInteger num) {
		if (BigInteger.ZERO.compareTo(num) > 0 || X.compareTo(num) < 0) {
			throw new IllegalArgumentException("0x" + num.toString(16));
		}
		final BigInteger[] parts = num.divideAndRemainder(B);
		BigInteger hi = parts[0];
		if (L.compareTo(hi) < 0) {
			hi = hi.subtract(B);
		}
		BigInteger lo = parts[1];
		if (L.compareTo(lo) < 0) {
			lo = lo.subtract(B);
		}
		return new UUID(hi.longValueExact(), lo.longValueExact());
	}

	/**
	 * Convert the specified {@link UUID} into a {@link BigInteger}.
	 * 
	 * @param uuid the UUID object to convert
	 * 
	 * @return A numeric representation of the specified UUID, e.g.
	 *         {@code 281831033846701114428451845565925261418}
	 * 
	 * @see <a href=
	 *      "https://gist.github.com/drmalex07/9008c611ffde6cb2ef3a2db8668bc251">drmalex07/convertUuidToBigInteger.java
	 *      - GitHub</a>
	 */
	public static BigInteger toBigInteger(final UUID uuid) {
		BigInteger hi = BigInteger.valueOf(uuid.getMostSignificantBits());
		if (hi.signum() < 0) {
			hi = hi.add(B);
		}
		BigInteger lo = BigInteger.valueOf(uuid.getLeastSignificantBits());
		if (lo.signum() < 0) {
			lo = lo.add(B);
		}
		return lo.add(hi.multiply(B));
	}

}
