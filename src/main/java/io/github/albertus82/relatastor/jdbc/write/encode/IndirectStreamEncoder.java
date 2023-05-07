package io.github.albertus82.relatastor.jdbc.write.encode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import io.github.albertus82.relatastor.jdbc.write.BlobStoreParameters;

/**
 * Stream encoder that reads bytes from an {@link InputStream}, encodes the data
 * and writes them to an {@link OutputStream}.
 */
public interface IndirectStreamEncoder {

	/**
	 * Reads bytes from an {@link InputStream}, encodes the data and writes them to
	 * an {@link OutputStream}.
	 *
	 * @param in the stream containing the data to encode
	 * @param out the target stream
	 * @param parameters the parameters that determine how the data will be encoded
	 *
	 * @throws IOException if an I/O error occurs
	 */
	void encodeStream(InputStream in, OutputStream out, BlobStoreParameters parameters) throws IOException;

}
