package io.github.albertus82.filestore.util;

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
 * @see <a href="https://www.ietf.org/rfc/rfc4648.txt">RFC 4648 - The Base16,
 *      Base32, and Base64 Data Encodings</a>
 */
public class UUIDUtils {

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
	 * @throws IllegalArgumentException if {@code uuidBase64Url} is not in valid
	 *         <strong>base64url</strong> scheme as defined in RFC 4648.
	 *
	 * @see #toBase64Url(UUID)
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
	 */
	public static String toBase64Url(final UUID uuid) {
		return encoder.encodeToString(ByteBuffer.allocate(Long.BYTES * 2).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()).array());
	}

}
