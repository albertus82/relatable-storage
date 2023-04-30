package io.github.albertus82.filestore.jdbc.read;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.SQLException;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import org.springframework.jdbc.LobRetrievalFailureException;

import io.github.albertus82.filestore.io.Compression;
import io.github.albertus82.filestore.jdbc.read.decode.DirectStreamDecoder;
import io.github.albertus82.filestore.jdbc.read.decode.zip.ZipStreamDecoder;
import io.github.albertus82.filestore.jdbc.write.BinaryStreamProvider;

/**
 * BLOB extraction strategy that buffers the entire BLOB content on a temporary
 * file on the file system.
 */
public class FileBufferedBlobExtractor implements BlobExtractor {

	private static final Logger log = Logger.getLogger(FileBufferedBlobExtractor.class.getName());

	private final Path directory;
	private final Compression compression;
	private final DirectStreamDecoder decoder;

	/**
	 * Creates a new instance of this extractor that stores temporary files in a
	 * directory whose name corresponds to the value of the {@code java.io.tmpdir}
	 * system property (usually it is the default system temp dir) without applying
	 * any compression, and using {@link ZipStreamDecoder} to decode the BLOB
	 * contents.
	 */
	public FileBufferedBlobExtractor() {
		this(Path.of(System.getProperty("java.io.tmpdir")), Compression.NONE, new ZipStreamDecoder());
	}

	/**
	 * Sets the directory in which temporary files will be stored.
	 *
	 * @param directory the directory in which temporary files will be stored
	 *
	 * @return a new instance that stores temporary files in the specified directory
	 */
	public FileBufferedBlobExtractor withDirectory(final Path directory) {
		return new FileBufferedBlobExtractor(directory, this.compression, this.decoder);
	}

	/**
	 * Enables in-memory data compression (effective only in case the BLOB content
	 * is not already compressed, otherwise this setting will be ignored).
	 *
	 * @param compression the compression level
	 *
	 * @return a new instance configured with the provided compression level
	 */
	public FileBufferedBlobExtractor withCompression(final Compression compression) {
		return new FileBufferedBlobExtractor(this.directory, compression, this.decoder);
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
	public FileBufferedBlobExtractor withDecoder(final DirectStreamDecoder decoder) {
		return new FileBufferedBlobExtractor(this.directory, this.compression, decoder);
	}

	/**
	 * Creates a new instance of this extractor.
	 *
	 * @param directory the directory in which temporary files will be stored
	 * @param compression the in-memory data compression level
	 * @param decoder the stream decoder used to decode the BLOB contents
	 */
	protected FileBufferedBlobExtractor(final Path directory, final Compression compression, final DirectStreamDecoder decoder) {
		this.directory = Objects.requireNonNull(directory, "directory must not be null");
		this.compression = Objects.requireNonNull(compression, "compression must not be null");
		this.decoder = Objects.requireNonNull(decoder, "decoder must not be null");
	}

	@Override
	public InputStream getInputStream(final BlobAccessor blobAccessor) throws SQLException {
		Objects.requireNonNull(blobAccessor, "blobAccessor must not be null");
		try {
			final Path bufferFile = Files.createTempFile(Files.createDirectories(directory), null, null);
			return newInputStream(blobAccessor, bufferFile);
		}
		catch (final IOException e) {
			throw new LobRetrievalFailureException("Error while reading data", e);
		}
	}

	private InputStream newInputStream(final BlobAccessor blobAccessor, final Path bufferFile) throws SQLException, IOException {
		try {
			final boolean compress = !blobAccessor.isCompressed() && !Compression.NONE.equals(compression);
			setPosixFilePermissions(bufferFile, "rw-------");
			try (final InputStream in = blobAccessor.getBinaryStream(); final OutputStream fos = Files.newOutputStream(bufferFile); final OutputStream out = compress ? new DeflaterOutputStream(fos, new Deflater(compression.getDeflaterLevel())) : fos) {
				in.transferTo(out);
			}
			return decoder.decodeStream(compress ? new InflaterInputStream(newInputStream(bufferFile)) : newInputStream(bufferFile), blobAccessor);
		}
		catch (final Exception e) {
			deleteIfExists(bufferFile);
			throw e;
		}
	}

	private static InputStream newInputStream(final Path bufferFile) throws IOException {
		return Files.newInputStream(bufferFile, StandardOpenOption.DELETE_ON_CLOSE);
	}

	private void setPosixFilePermissions(final Path file, final String rwxrwxrwx) throws IOException {
		try {
			Files.setPosixFilePermissions(file, PosixFilePermissions.fromString(rwxrwxrwx));
		}
		catch (final UnsupportedOperationException e) {
			logException(e, () -> "Cannot set POSIX permissions for file \"" + file + "\":");
		}
	}

	private void deleteIfExists(final Path file) {
		if (file != null) {
			try {
				Files.deleteIfExists(file);
			}
			catch (final IOException e) {
				logException(e, () -> "Cannot delete file \"" + file + "\":");
				file.toFile().deleteOnExit();
			}
		}
	}

	/**
	 * Logs non-fatal exceptions that might be useful for debug. Can be overridden
	 * to customize logging logic.
	 *
	 * @param thrown the exception to log
	 * @param msgSupplier a supplier returning the log message
	 */
	protected void logException(final Throwable thrown, final Supplier<String> msgSupplier) {
		Objects.requireNonNull(thrown);
		Objects.requireNonNull(msgSupplier);
		log.log(Level.FINE, thrown, msgSupplier);
	}

}
