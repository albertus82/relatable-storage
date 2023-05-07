package io.github.albertus82.filestore.jdbc.read.decode.zip;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

import org.springframework.jdbc.LobRetrievalFailureException;

import io.github.albertus82.filestore.jdbc.read.BlobAccessor;
import io.github.albertus82.filestore.jdbc.read.decode.DirectStreamDecoder;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.io.inputstream.ZipInputStream;

/**
 * Stream decoder that decrypts and inflates data from an {@link InputStream}
 * on-the-fly.
 */
public class ZipStreamDecoder implements DirectStreamDecoder {

	/** Default empty constructor. */
	public ZipStreamDecoder() { /* Javadoc */ }

	@Override
	public ZipInputStream decodeStream(final InputStream in, final BlobAccessor blobAccessor) {
		Objects.requireNonNull(in, "InputStream must not be null");
		Objects.requireNonNull(blobAccessor, "blobAccessor must not be null");
		final ZipInputStream zis = blobAccessor.isEncrypted() ? new ZipInputStream(in, blobAccessor.getPassword()) : new ZipInputStream(in);
		try {
			if (zis.getNextEntry() == null) {
				throw new ZipException("Invalid ZIP file");
			}
		}
		catch (final IOException e) {
			throw new LobRetrievalFailureException("Error while reading data", e);
		}
		return zis;
	}

}
