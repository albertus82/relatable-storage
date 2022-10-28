package io.github.albertus82.filestore;

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
	 * directory specified.
	 *
	 * @param bufferDirectory the directory in which temporary files will be stored
	 */
	public FileBufferedBlobExtractor(final Path bufferDirectory) {
		Objects.requireNonNull(bufferDirectory);
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

	private static InputStream getInputStream(final ResultSet resultSet, final int blobColumnIndex, final Path bufferFile) throws SQLException, IOException {
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

	private static void setPosixFilePermissions(final Path file, final String rwxrwxrwx) throws IOException {
		try {
			Files.setPosixFilePermissions(file, PosixFilePermissions.fromString(rwxrwxrwx));
		}
		catch (final UnsupportedOperationException e) {
			log.log(Level.FINE, e, () -> "Cannot set POSIX permissions for file \"" + file + "\":");
		}
	}

	private static void deleteIfExists(final Path file) {
		if (file != null) {
			try {
				Files.deleteIfExists(file);
			}
			catch (final IOException e) {
				log.log(Level.FINE, e, () -> "Cannot delete file \"" + file + "\":");
				file.toFile().deleteOnExit();
			}
		}
	}

}
