package io.github.albertus82.filestore.jdbc.write;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

import io.github.albertus82.filestore.jdbc.read.BlobExtractor;
import io.github.albertus82.filestore.jdbc.write.encode.IndirectStreamEncoder;
import io.github.albertus82.filestore.jdbc.write.encode.zip.ZipStreamEncoder;

/**
 * BLOB binary stream provider that buffers the entire BLOB content in memory.
 * Note that this strategy is intended for simple cases where it is convenient
 * to write all bytes into a byte array. It is not intended for large files.
 */
public class MemoryBufferedBinaryStreamProvider implements BinaryStreamProvider {

	private final IndirectStreamEncoder encoder;

	/**
	 * Creates a new instance of this provider that uses {@link ZipStreamEncoder} to
	 * encode data.
	 */
	public MemoryBufferedBinaryStreamProvider() {
		this(new ZipStreamEncoder());
	}

	/**
	 * Creates a new instance of this provider that uses the provided stream
	 * encoder. Note that in order to successfully decode the BLOB contents, the
	 * encoder format must match the one of the decoder chosen for
	 * {@link BlobExtractor}.
	 *
	 * @param encoder the stream encoder that will be used to to encode data
	 */
	public MemoryBufferedBinaryStreamProvider(final IndirectStreamEncoder encoder) {
		Objects.requireNonNull(encoder, "encoder must not be null");
		this.encoder = encoder;
	}

	@Override
	public InputStream getContentStream(final InputStream in, final BlobStoreParameters parameters) throws IOException {
		Objects.requireNonNull(in, "in must not be null");
		Objects.requireNonNull(parameters, "parameters must not be null");
		try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			encoder.encodeStream(in, out, parameters);
			return new ByteArrayInputStream(out.toByteArray());
		}
	}

}
