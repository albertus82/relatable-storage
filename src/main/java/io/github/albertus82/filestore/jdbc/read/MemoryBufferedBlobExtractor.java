package io.github.albertus82.filestore.jdbc.read;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.sql.SQLException;
import java.util.Objects;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import io.github.albertus82.filestore.io.Compression;
import io.github.albertus82.filestore.jdbc.read.decode.DirectStreamDecoder;
import io.github.albertus82.filestore.jdbc.read.decode.zip.ZipStreamDecoder;
import io.github.albertus82.filestore.jdbc.write.BinaryStreamProvider;

/**
 * BLOB extraction strategy that buffers the entire BLOB content in memory. Note
 * that this strategy is intended for simple cases where it is convenient to
 * read all bytes into a byte array. It is not intended for large files.
 */
public class MemoryBufferedBlobExtractor implements BlobExtractor {

	private final Compression compression;
	private final DirectStreamDecoder decoder;

	/**
	 * Creates a new instance of this extractor that buffers the entire BLOB content
	 * in memory without applying any compression, and uses {@link ZipStreamDecoder}
	 * to decode the BLOB contents.
	 */
	public MemoryBufferedBlobExtractor() {
		this(Compression.NONE, new ZipStreamDecoder());
	}

	/**
	 * Enables in-memory data compression (effective only in case the BLOB content
	 * is not already compressed, otherwise this setting will be ignored).
	 *
	 * @param compression the compression level
	 *
	 * @return a new instance configured with the provided compression level
	 */
	public MemoryBufferedBlobExtractor withCompression(final Compression compression) {
		return new MemoryBufferedBlobExtractor(compression, this.decoder);
	}

	/**
	 * Sets a custom stream decoder. Note that in order to successfully decode the
	 * BLOB contents, the decoder format must match the one of the encoder chosen
	 * for {@link BinaryStreamProvider}.
	 *
	 * @param decoder the stream decoder
	 *
	 * @return a new instance configured with the provided stream decoder
	 */
	public MemoryBufferedBlobExtractor withDecoder(final DirectStreamDecoder decoder) {
		return new MemoryBufferedBlobExtractor(this.compression, decoder);
	}

	/**
	 * Creates a new instance of this extractor.
	 *
	 * @param compression the in-memory data compression level
	 * @param decoder the stream decoder used to decode the BLOB contents
	 */
	protected MemoryBufferedBlobExtractor(final Compression compression, final DirectStreamDecoder decoder) {
		this.compression = Objects.requireNonNull(compression, "compression must not be null");
		this.decoder = Objects.requireNonNull(decoder, "decoder must not be null");
	}

	@Override
	public InputStream getInputStream(final BlobAccessor blobAccessor) throws SQLException {
		Objects.requireNonNull(blobAccessor, "blobAccessor must not be null");
		return decoder.decodeStream(newInputStream(blobAccessor), blobAccessor);
	}

	private InputStream newInputStream(final BlobAccessor blobAccessor) throws SQLException {
		if (blobAccessor.isCompressed() || Compression.NONE.equals(compression)) {
			return new ByteArrayInputStream(blobAccessor.getBytes());
		}
		else {
			try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
				try (final InputStream in = blobAccessor.getBinaryStream(); final DeflaterOutputStream dos = new DeflaterOutputStream(baos, new Deflater(compression.getDeflaterLevel()))) {
					in.transferTo(dos);
				}
				return new InflaterInputStream(new ByteArrayInputStream(baos.toByteArray()));
			}
			catch (final IOException e) {
				throw new UncheckedIOException(e);
			}
		}
	}

}
