package io.github.albertus82.filestore.jdbc.extractor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.jdbc.LobRetrievalFailureException;

/**
 * BLOB extraction strategy that buffers the entire BLOB content on a temporary
 * file on the file system.
 */
public class FileBufferedBlobExtractor implements BlobExtractor {

	private static final Logger log = Logger.getLogger(FileBufferedBlobExtractor.class.getName());

	private final Path bufferDirectory;

	/**
	 * Creates a new instance of this strategy that stores temporary files in the
	 * specified directory.
	 *
	 * @param bufferDirectory the directory in which temporary files will be stored
	 */
	public FileBufferedBlobExtractor(final Path bufferDirectory) {
		Objects.requireNonNull(bufferDirectory, "bufferDirectory must not be null");
		this.bufferDirectory = bufferDirectory;
	}

	/**
	 * Creates a new instance of this strategy that stores temporary files in a
	 * directory whose name corresponds to the value of the {@code java.io.tmpdir}
	 * system property (usually it's the default system temp dir).
	 */
	public FileBufferedBlobExtractor() {
		this(Path.of(System.getProperty("java.io.tmpdir")));
	}

	@Override
	public InputStream getInputStream(final ResultSet resultSet, final int blobColumnIndex) throws SQLException {
		try {
			final Path bufferFile = Files.createTempFile(Files.createDirectories(bufferDirectory), null, null);
			return getInputStream(resultSet, blobColumnIndex, bufferFile);
		}
		catch (final IOException e) {
			throw new LobRetrievalFailureException("Error while reading compressed data", e);
		}
	}

	private InputStream getInputStream(final ResultSet resultSet, final int blobColumnIndex, final Path bufferFile) throws SQLException, IOException {
		try {
			setPosixFilePermissions(bufferFile, "rw-------");
			try (final InputStream in = resultSet.getBinaryStream(blobColumnIndex); final OutputStream out = Files.newOutputStream(bufferFile)) {
				in.transferTo(out);
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
		Objects.requireNonNull(thrown);
		Objects.requireNonNull(msgSupplier);
		log.log(Level.FINE, thrown, msgSupplier);
	}

}
