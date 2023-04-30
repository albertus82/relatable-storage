package io.github.albertus82.filestore.jdbc.write;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.UncheckedIOException;
import java.util.Objects;

import io.github.albertus82.filestore.jdbc.read.BlobExtractor;
import io.github.albertus82.filestore.jdbc.write.encode.IndirectStreamEncoder;
import io.github.albertus82.filestore.jdbc.write.encode.zip.ZipStreamEncoder;

/**
 * BLOB binary stream provider that pipes streams using a small fixed-size
 * memory buffer.
 */
public class PipeBasedBinaryStreamProvider implements BinaryStreamProvider {

	private static final short DEFAULT_PIPE_SIZE = 16384;

	private final int pipeSize;
	private final IndirectStreamEncoder encoder;

	/**
	 * Creates a new instance of this provider with the default pipe size and that
	 * uses {@link ZipStreamEncoder} to encode data.
	 */
	public PipeBasedBinaryStreamProvider() {
		this(DEFAULT_PIPE_SIZE, new ZipStreamEncoder());
	}

	/**
	 * Sets a custom pipe size.
	 *
	 * @param pipeSize the size of the pipe's buffer
	 *
	 * @return a new instance configured with a custom pipe size
	 */
	public PipeBasedBinaryStreamProvider withPipeSize(final int pipeSize) {
		return new PipeBasedBinaryStreamProvider(pipeSize, this.encoder);
	}

	/**
	 * Sets a custom stream encoder. Note that in order to successfully decode the
	 * BLOB contents, the encoder format must match the one of the decoder chosen
	 * for {@link BlobExtractor}.
	 *
	 * @param encoder the stream encoder that will be used to to encode data
	 *
	 * @return a new instance configured with the provided stream encoder
	 */
	public PipeBasedBinaryStreamProvider withEncoder(final IndirectStreamEncoder encoder) {
		return new PipeBasedBinaryStreamProvider(this.pipeSize, encoder);
	}

	/**
	 * Creates a new instance of this provider.
	 * 
	 * @param pipeSize the size of the pipe's buffer
	 * @param encoder the stream encoder that will be used to to encode data
	 */
	protected PipeBasedBinaryStreamProvider(final int pipeSize, final IndirectStreamEncoder encoder) {
		this.encoder = Objects.requireNonNull(encoder, "encoder must not be null");
		this.pipeSize = pipeSize;
	}

	@Override
	public InputStream getContentStream(final InputStream in, final BlobStoreParameters parameters) throws IOException {
		Objects.requireNonNull(in, "in must not be null");
		Objects.requireNonNull(parameters, "parameters must not be null");
		final PipedInputStream pis = new PipedInputStream(pipeSize); // NOSONAR Use try-with-resources or close this "PipedInputStream" in a "finally" clause. Resources should be closed (java:S2095)
		final PipedOutputStream pos = new PipedOutputStream(pis); // NOSONAR Use try-with-resources or close this "PipedInputStream" in a "finally" clause. Resources should be closed (java:S2095)
		final Thread t = new Thread(() -> {
			try (pos) {
				new ZipStreamEncoder().encodeStream(in, pos, parameters);
			}
			catch (final IOException e) {
				throw new UncheckedIOException(e);
			}
		});
		t.setDaemon(true);
		t.start();
		return pis;
	}

}
