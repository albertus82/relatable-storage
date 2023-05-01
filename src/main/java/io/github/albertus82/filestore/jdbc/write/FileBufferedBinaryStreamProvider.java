package io.github.albertus82.filestore.jdbc.write;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.github.albertus82.filestore.jdbc.read.BlobExtractor;
import io.github.albertus82.filestore.jdbc.write.encode.IndirectStreamEncoder;
import io.github.albertus82.filestore.jdbc.write.encode.zip.ZipStreamEncoder;

/** BLOB binary stream provider that uses a file system buffer. */
public class FileBufferedBinaryStreamProvider implements BinaryStreamProvider {

	private static final Logger log = Logger.getLogger(FileBufferedBinaryStreamProvider.class.getName());

	private final Path directory;
	private final IndirectStreamEncoder encoder;

	/**
	 * Creates a new instance of this provider that stores temporary files in a
	 * directory whose name corresponds to the value of the {@code java.io.tmpdir}
	 * system property (usually it's the default system temp dir) and uses
	 * {@link ZipStreamEncoder} to encode data.
	 */
	public FileBufferedBinaryStreamProvider() {
		this(Path.of(System.getProperty("java.io.tmpdir")), new ZipStreamEncoder());
	}

	/**
	 * Sets the directory in which temporary files will be stored.
	 *
	 * @param directory the directory in which temporary files will be stored
	 *
	 * @return a new instance that stores temporary files in the specified directory
	 */
	public FileBufferedBinaryStreamProvider withDirectory(final Path directory) {
		return new FileBufferedBinaryStreamProvider(directory, this.encoder);
	}

	/**
	 * Sets a custom stream encoder. Note that in order to successfully decode the
	 * BLOB contents, the encoder format must match the one of the decoder chosen
	 * for {@link BlobExtractor}.
	 *
	 * @param encoder the stream encoder that will be used to encode data
	 *
	 * @return a new instance configured with the provided stream encoder
	 */
	public FileBufferedBinaryStreamProvider withEncoder(final IndirectStreamEncoder encoder) {
		return new FileBufferedBinaryStreamProvider(this.directory, encoder);
	}

	/**
	 * Creates a new instance of this provider.
	 * 
	 * @param directory the directory in which temporary files will be stored
	 * @param encoder the stream encoder that will be used to to encode data
	 */
	protected FileBufferedBinaryStreamProvider(final Path directory, final IndirectStreamEncoder encoder) {
		this.directory = Objects.requireNonNull(directory, "directory must not be null");
		this.encoder = Objects.requireNonNull(encoder, "encoder must not be null");
	}

	@Override
	public InputStream getContentStream(final InputStream in, final BlobStoreParameters parameters) throws IOException {
		Objects.requireNonNull(in, "InputStream must not be null");
		Objects.requireNonNull(parameters, "parameters must not be null");
		final Path bufferFile = Files.createTempFile(Files.createDirectories(directory), null, null);
		return getContentStream(in, parameters, bufferFile);
	}

	private InputStream getContentStream(final InputStream in, final BlobStoreParameters parameters, final Path bufferFile) throws IOException {
		try {
			setPosixFilePermissions(bufferFile, "rw-------");
			try (final OutputStream out = Files.newOutputStream(bufferFile, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
				encoder.encodeStream(in, out, parameters);
			}
			return Files.newInputStream(bufferFile, StandardOpenOption.DELETE_ON_CLOSE);
		}
		catch (final Exception e) {
			deleteIfExists(bufferFile);
			throw e;
		}
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
		Objects.requireNonNull(thrown, "Throwable must not be null");
		Objects.requireNonNull(msgSupplier, "msgSupplier must not be null");
		log.log(Level.FINE, thrown, msgSupplier);
	}

}
