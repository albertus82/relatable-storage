package io.github.albertus82.filestore.jdbc.write;

import java.io.IOException;
import java.io.InputStream;

/** BLOB binary stream provider. */
public interface BinaryStreamProvider {

	/**
	 * Returns an {@link InputStream} to be used as the BLOB content binary stream.
	 *
	 * @param in the original data to persist (this stream will NOT be closed)
	 * @param parameters the parameters of the BLOB that will be created
	 *
	 * @return an {@link InputStream} to be used as the BLOB content binary stream
	 *
	 * @throws IOException if an I/O error occurs
	 */
	InputStream getContentStream(InputStream in, BlobStoreParameters parameters) throws IOException;

}
