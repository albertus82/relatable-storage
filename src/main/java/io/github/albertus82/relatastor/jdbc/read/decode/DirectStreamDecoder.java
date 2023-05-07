package io.github.albertus82.relatastor.jdbc.read.decode;

import java.io.InputStream;

import io.github.albertus82.relatastor.jdbc.read.BlobAccessor;

/**
 * Stream decoder that decodes data from an {@link InputStream} on-the-fly.
 */
public interface DirectStreamDecoder {

	/**
	 * Decodes data from an {@link InputStream} on-the-fly.
	 *
	 * @param in the stream containing the data to decode
	 * @param blobAccessor helper object containing the informations needed to
	 *        properly decode the data
	 *
	 * @return a new {@link InputStream} that decodes data on-the-fly
	 */
	InputStream decodeStream(InputStream in, BlobAccessor blobAccessor);

}
