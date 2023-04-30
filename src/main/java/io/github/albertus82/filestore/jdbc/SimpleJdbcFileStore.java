package io.github.albertus82.filestore.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.Resource;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.StatementCallback;
import org.springframework.jdbc.core.support.AbstractLobCreatingPreparedStatementCallback;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobCreator;

import io.github.albertus82.filestore.SimpleFileStore;
import io.github.albertus82.filestore.io.Compression;
import io.github.albertus82.filestore.jdbc.read.AbstractBlobAccessor;
import io.github.albertus82.filestore.jdbc.read.BlobExtractor;
import io.github.albertus82.filestore.jdbc.write.BinaryStreamProvider;
import io.github.albertus82.filestore.jdbc.write.BlobStoreParameters;
import io.github.albertus82.filestore.jdbc.write.PipeBasedBinaryStreamProvider;

/** Basic RDBMS-based implementation of a filestore. */
@SuppressWarnings("java:S1130") // "throws" declarations should not be superfluous
public class SimpleJdbcFileStore implements SimpleFileStore {

	private static final String SQL_ESCAPE = "\\";

	private static final Logger log = Logger.getLogger(SimpleJdbcFileStore.class.getName());

	private final JdbcOperations jdbcOperations;
	private final String table;
	private final BlobExtractor blobExtractor;
	private final BinaryStreamProvider binaryStreamProvider;
	private final Compression compression;
	private final String schema;
	private final char[] password;
	private final boolean alwaysQuotedIdentifiers;

	/**
	 * Creates a new instance based on a database table. All the parameters are
	 * mandatory.
	 *
	 * @param jdbcOperations the JDBC executor
	 * @param table the database table name (see also
	 *        {@link #withAlwaysQuotedIdentifiers(boolean)})
	 * @param blobExtractor the BLOB extraction strategy
	 */
	public SimpleJdbcFileStore(final JdbcOperations jdbcOperations, final String table, final BlobExtractor blobExtractor) {
		this(jdbcOperations, table, blobExtractor, new PipeBasedBinaryStreamProvider(), Compression.NONE, false, null, null);
	}

	/**
	 * Creates a new instance based on a database table.
	 *
	 * @param jdbcOperations the JDBC executor (mandatory)
	 * @param table the database table name (mandatory)
	 * @param blobExtractor the BLOB extraction strategy (mandatory)
	 * @param binaryStreamProvider a provider that produces the binary stream to be
	 *        stored as BLOB (mandatory)
	 * @param compression the data compression level (mandatory, use
	 *        {@link Compression#NONE} to store uncompressed data).
	 * @param alwaysQuotedIdentifiers when enabled, the database
	 *        <strong>schema</strong> and <strong>table</strong> identifiers are
	 *        always quoted.
	 * @param schema the database schema name (can be null, so that no schema name
	 *        will be included in the generated SQL).
	 * @param password the encryption/decryption password (can be null, so that
	 *        neither encryption nor decryption will be performed).
	 */
	protected SimpleJdbcFileStore(final JdbcOperations jdbcOperations, final String table, final BlobExtractor blobExtractor, final BinaryStreamProvider binaryStreamProvider, final Compression compression, final boolean alwaysQuotedIdentifiers, final String schema, final char[] password) {
		Objects.requireNonNull(jdbcOperations, "jdbcOperations must not be null");
		Objects.requireNonNull(table, "table must not be null");
		Objects.requireNonNull(blobExtractor, "blobExtractor must not be null");
		Objects.requireNonNull(binaryStreamProvider, "binaryStreamProvider must not be null");
		Objects.requireNonNull(compression, "compression must not be null");
		if (table.isEmpty()) {
			throw new IllegalArgumentException("table must not be empty");
		}
		this.jdbcOperations = jdbcOperations;
		this.table = table;
		this.blobExtractor = blobExtractor;
		this.binaryStreamProvider = binaryStreamProvider;
		this.compression = compression;
		this.alwaysQuotedIdentifiers = alwaysQuotedIdentifiers;
		this.schema = schema;
		this.password = password;
	}

	/**
	 * Enables data compression.
	 *
	 * @param compression the data compression level
	 *
	 * @return a new instance configured with the provided compression level.
	 */
	public SimpleJdbcFileStore withCompression(final Compression compression) {
		return new SimpleJdbcFileStore(this.jdbcOperations, this.table, this.blobExtractor, this.binaryStreamProvider, compression, this.alwaysQuotedIdentifiers, this.schema, this.password);
	}

	/**
	 * When enabled, the database <strong>schema</strong> and <strong>table</strong>
	 * identifiers are always quoted.
	 *
	 * @param alwaysQuotedIdentifiers the database identifier quoting strategy
	 *
	 * @return a new instance configured with the provided database identifier
	 *         quoting strategy.
	 */
	public SimpleJdbcFileStore withAlwaysQuotedIdentifiers(final boolean alwaysQuotedIdentifiers) {
		return new SimpleJdbcFileStore(this.jdbcOperations, this.table, this.blobExtractor, this.binaryStreamProvider, this.compression, alwaysQuotedIdentifiers, this.schema, this.password);
	}

	/**
	 * Sets a custom database schema name.
	 *
	 * @param schema the database schema name (see also
	 *        {@link #withAlwaysQuotedIdentifiers(boolean)})
	 *
	 * @return a new instance configured with the provided schema name.
	 */
	public SimpleJdbcFileStore withSchema(final String schema) {
		Objects.requireNonNull(schema, "schema must not be null");
		if (schema.isEmpty()) {
			throw new IllegalArgumentException("schema must not be empty");
		}
		return new SimpleJdbcFileStore(this.jdbcOperations, this.table, this.blobExtractor, this.binaryStreamProvider, this.compression, this.alwaysQuotedIdentifiers, schema, this.password);
	}

	/**
	 * Enables data encryption (and decryption).
	 *
	 * @param password the password used for encryption and decryption
	 *
	 * @return a new instance with encryption/decryption support.
	 */
	public SimpleJdbcFileStore withEncryption(final char[] password) {
		Objects.requireNonNull(password, "password must not be null");
		if (password.length == 0) {
			throw new IllegalArgumentException("password must not be empty");
		}
		return new SimpleJdbcFileStore(this.jdbcOperations, this.table, this.blobExtractor, this.binaryStreamProvider, this.compression, this.alwaysQuotedIdentifiers, this.schema, password.clone());
	}

	/**
	 * Sets a custom BLOB binary stream provider.
	 *
	 * @param binaryStreamProvider a provider that produces the binary stream to be
	 *        stored as BLOB
	 *
	 * @return a new instance configured with the provided binary stream provider.
	 */
	public SimpleJdbcFileStore withBinaryStreamProvider(final BinaryStreamProvider binaryStreamProvider) {
		Objects.requireNonNull(binaryStreamProvider, "binaryStreamProvider must not be null");
		return new SimpleJdbcFileStore(this.jdbcOperations, this.table, this.blobExtractor, binaryStreamProvider, this.compression, this.alwaysQuotedIdentifiers, this.schema, this.password);
	}

	/**
	 * Returns the database schema name, or an empty {@link Optional} if no schema
	 * was specified.
	 *
	 * @return the database schema name, or an empty {@link Optional} if no schema
	 *         was specified.
	 */
	public Optional<String> getSchema() {
		return Optional.ofNullable(schema);
	}

	/**
	 * Returns the database table name.
	 *
	 * @return the database table name
	 */
	public String getTable() {
		return table;
	}

	/**
	 * Returns the compression level.
	 *
	 * @return the compression level
	 */
	public Compression getCompression() {
		return compression;
	}

	/**
	 * Returns {@code true} if the database identifiers are always quoted, otherwise
	 * {@code false}.
	 *
	 * @return {@code true} if the database identifiers are always quoted, otherwise
	 *         {@code false}.
	 */
	public boolean isAlwaysQuotedIdentifiers() {
		return alwaysQuotedIdentifiers;
	}

	@Override
	public List<Resource> list(final String... patterns) throws IOException {
		final StringBuilder sb = new StringBuilder("SELECT filename, content_length, last_modified, uuid_base64url FROM ");
		appendSchemaAndTableName(sb);
		final List<Object> args = new ArrayList<>();
		if (patterns != null && patterns.length > 0) {
			boolean first = true;
			for (final String pattern : patterns) {
				final String like = pattern.replace(SQL_ESCAPE, SQL_ESCAPE + SQL_ESCAPE).replace("%", SQL_ESCAPE + "%").replace("_", SQL_ESCAPE + "_").replace('*', '%').replace('?', '_');
				if (first) {
					sb.append(" WHERE ");
					first = false;
				}
				else {
					sb.append(" OR ");
				}
				sb.append("filename LIKE ? ESCAPE ?");
				args.add(like);
				args.add(SQL_ESCAPE);
			}
		}
		final String sql = sb.toString();
		logStatement(sql);
		try {
			return jdbcOperations.query(sql, (rs, rowNum) -> new DatabaseResource(rs.getString(1), rs.getLong(2), rs.getTimestamp(3).getTime(), rs.getString(4)), args.toArray(new Object[args.size()]));
		}
		catch (final DataAccessException e) {
			throw new IOException(e);
		}
	}

	@Override
	public DatabaseResource get(final String fileName) throws NoSuchFileException, IOException {
		Objects.requireNonNull(fileName, "fileName must not be null");
		final StringBuilder sb = new StringBuilder("SELECT content_length, last_modified, uuid_base64url FROM ");
		appendSchemaAndTableName(sb).append(" WHERE filename=?");
		final String sql = sb.toString();
		logStatement(sql);
		try {
			return jdbcOperations.query(sql, rs -> {
				if (rs.next()) {
					return new DatabaseResource(fileName, rs.getLong(1), rs.getTimestamp(2).getTime(), rs.getString(3));
				}
				else {
					throw new EmptyResultDataAccessException(1);
				}
			}, fileName);
		}
		catch (final EmptyResultDataAccessException e) {
			logException(e, () -> fileName);
			throw new NoSuchFileException(fileName);
		}
		catch (final DataAccessException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void store(final Resource resource, final String fileName) throws FileAlreadyExistsException, IOException {
		Objects.requireNonNull(resource, "resource must not be null");
		Objects.requireNonNull(fileName, "fileName must not be null");
		final long contentLength = insert(resource, fileName);
		final StringBuilder sb = new StringBuilder("UPDATE ");
		appendSchemaAndTableName(sb).append(" SET content_length=? WHERE filename=?");
		final String sql = sb.toString();
		logStatement(sql);
		try {
			jdbcOperations.update(sql, contentLength, fileName);
		}
		catch (final DataAccessException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void rename(final String oldFileName, final String newFileName) throws NoSuchFileException, FileAlreadyExistsException, IOException {
		Objects.requireNonNull(oldFileName, "oldFileName must not be null");
		Objects.requireNonNull(newFileName, "newFileName must not be null");
		final StringBuilder sb = new StringBuilder("UPDATE ");
		appendSchemaAndTableName(sb).append(" SET filename=? WHERE filename=?");
		final String sql = sb.toString();
		logStatement(sql);
		try {
			if (jdbcOperations.update(sql, newFileName, oldFileName) == 0) {
				throw new NoSuchFileException(oldFileName);
			}
		}
		catch (final DuplicateKeyException e) {
			throw new FileAlreadyExistsException(newFileName);
		}
		catch (final DataAccessException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void delete(final String fileName) throws NoSuchFileException, IOException {
		Objects.requireNonNull(fileName, "fileName must not be null");
		final StringBuilder sb = new StringBuilder("DELETE FROM ");
		appendSchemaAndTableName(sb).append(" WHERE filename=?");
		final String sql = sb.toString();
		logStatement(sql);
		try {
			if (jdbcOperations.update(sql, fileName) == 0) {
				throw new NoSuchFileException(fileName);
			}
		}
		catch (final DataAccessException e) {
			throw new IOException(e);
		}
	}

	/** {@link Resource} implementation with a RDBMS table target. */
	public class DatabaseResource extends AbstractResource { // NOSONAR Override the "equals" method in this class. Subclasses that add fields should override "equals" (java:S2160)

		private final String fileName;
		private final long contentLength;
		private final long lastModified;
		private final String uuidBase64Url;

		private DatabaseResource(final String fileName, final long contentLength, final long lastModified, final String uuidBase64Url) {
			Objects.requireNonNull(fileName, "fileName must not be null");
			Objects.requireNonNull(uuidBase64Url, "uuidBase64Url must not be null");
			this.fileName = fileName;
			this.contentLength = contentLength;
			this.lastModified = lastModified;
			this.uuidBase64Url = uuidBase64Url;
		}

		/** Returns the resource key, that usually is the file name. */
		@Override
		public String getFilename() {
			return fileName;
		}

		/** Returns the original (uncompressed) file size. */
		@Override
		public long contentLength() {
			return contentLength;
		}

		/** Returns the time that the file was last modified. */
		@Override
		public long lastModified() {
			return lastModified;
		}

		/**
		 * Returns the UUID of the stored object.
		 *
		 * @return the UUID of the stored object.
		 */
		public UUID getUUID() {
			return UUIDConverter.fromBase64Url(uuidBase64Url);
		}

		/** Checks if the resource exists in the database. */
		@Override
		public boolean exists() {
			try {
				final StringBuilder sb = new StringBuilder("SELECT COUNT(*) FROM ");
				appendSchemaAndTableName(sb).append(" WHERE filename=?");
				final String sql = sb.toString();
				logStatement(sql);
				return jdbcOperations.queryForObject(sql, boolean.class, fileName);
			}
			catch (final DataAccessException | IOException e) {
				logException(e, () -> "Could not retrieve data for existence check of " + getDescription());
				return false;
			}
		}

		/**
		 * Returns a description for this resource, to be used for error output when
		 * working with the resource.
		 */
		@Override
		public String getDescription() {
			return "Database resource [" + fileName + "]";
		}

		/**
		 * Returns an {@link InputStream} for the content, applying the
		 * {@link BlobExtractor} strategy specified specified in the constructor of
		 * {@link SimpleJdbcFileStore}.
		 *
		 * @return an {@link InputStream} to read the file content
		 */
		@Override
		public InputStream getInputStream() throws IOException {
			final StringBuilder sb = new StringBuilder("SELECT compressed, encrypted, file_contents FROM ");
			appendSchemaAndTableName(sb).append(" WHERE filename=?");
			final String sql = sb.toString();
			logStatement(sql);
			try {
				return jdbcOperations.query(sql, rs -> {
					if (rs.next()) {
						return getInputStream(rs);
					}
					else {
						throw new EmptyResultDataAccessException(1);
					}
				}, fileName);
			}
			catch (final EmptyResultDataAccessException e) {
				logException(e, () -> fileName);
				throw new NoSuchFileException(fileName);
			}
			catch (final DataAccessException e) {
				throw new IOException(e);
			}
		}

		private InputStream getInputStream(final ResultSet rs) throws SQLException {
			int columnIndex = 0;
			final boolean compressed = rs.getBoolean(++columnIndex);
			final boolean encrypted = rs.getBoolean(++columnIndex);
			final int blobColumnIndex = ++columnIndex;
			return blobExtractor.getInputStream(new AbstractBlobAccessor(compressed, encrypted ? password : null) {
				@Override
				public byte[] getBytes() throws SQLException {
					return rs.getBytes(blobColumnIndex);
				}

				@Override
				public InputStream getBinaryStream() throws SQLException {
					return rs.getBinaryStream(blobColumnIndex);
				}
			});
		}
	}

	/**
	 * Logs the execution of a SQL statement. Can be overridden to customize logging
	 * logic.
	 *
	 * @param sql the statement to log
	 */
	protected void logStatement(final String sql) {
		Objects.requireNonNull(sql);
		log.log(Level.FINE, "{0}", sql);
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

	private long insert(final Resource resource, final String fileName) throws IOException {
		final Long contentLength = resource.isOpen() ? null : resource.contentLength();
		final StringBuilder sb = new StringBuilder("INSERT INTO ");
		appendSchemaAndTableName(sb);
		sb.append(" (filename, last_modified, compressed, encrypted, uuid_base64url, file_contents) VALUES (?, ?, ?, ?, ?, ?)");
		final String sql = sb.toString();
		logStatement(sql);
		try (final InputStream ris = resource.getInputStream(); final CountingInputStream cis = new CountingInputStream(ris)) {
			try (final InputStream inputStream = binaryStreamProvider.getContentStream(cis, new BlobStoreParameters(compression, password))) {
				jdbcOperations.execute(sql, new AbstractLobCreatingPreparedStatementCallback(new DefaultLobHandler()) {
					@Override
					protected void setValues(final PreparedStatement ps, final LobCreator lobCreator) throws SQLException {
						int columnIndex = 0;
						ps.setString(++columnIndex, fileName);
						ps.setTimestamp(++columnIndex, determineLastModifiedTimestamp(resource));
						ps.setBoolean(++columnIndex, !Compression.NONE.equals(compression));
						ps.setBoolean(++columnIndex, password != null);
						ps.setString(++columnIndex, UUIDConverter.toBase64Url(UUID.randomUUID()));
						lobCreator.setBlobAsBinaryStream(ps, ++columnIndex, inputStream, -1);
					}
				});
			}
			if (contentLength != null && contentLength.longValue() != cis.getCount()) {
				throw new StreamCorruptedException("Inconsistent content length (expected: " + contentLength + ", actual: " + cis.getCount() + ")");
			}
			return cis.getCount();
		}
		catch (final DuplicateKeyException e) {
			logException(e, () -> fileName);
			throw new FileAlreadyExistsException(fileName);
		}
		catch (final DataAccessException e) {
			throw new IOException(e);
		}
	}

	private String sanitizeIdentifier(final String identifier) throws IOException {
		try {
			return jdbcOperations.execute(new StatementCallback<String>() {
				@Override
				public String doInStatement(final Statement stmt) throws SQLException {
					return stmt.enquoteIdentifier(identifier, alwaysQuotedIdentifiers);
				}
			});
		}
		catch (final DataAccessException e) {
			throw new IOException(e);
		}
	}

	private <T extends Appendable> T appendSchemaAndTableName(final T sql) throws IOException {
		if (schema != null) {
			sql.append(sanitizeIdentifier(schema)).append('.');
		}
		sql.append(sanitizeIdentifier(table));
		return sql;
	}

	private Timestamp determineLastModifiedTimestamp(final Resource resource) {
		try {
			final long lastModified = resource.lastModified();
			if (lastModified > 0) {
				return new Timestamp(lastModified);
			}
		}
		catch (final IOException e) {
			logException(e, resource::toString);
		}
		return new Timestamp(System.currentTimeMillis());
	}

	static class UUIDConverter {

		private static final Encoder encoder = Base64.getUrlEncoder().withoutPadding();
		private static final Decoder decoder = Base64.getUrlDecoder();

		private UUIDConverter() {}

		static UUID fromBase64Url(final String uuidBase64Url) {
			final LongBuffer buf = ByteBuffer.wrap(decoder.decode(uuidBase64Url)).asLongBuffer();
			return new UUID(buf.get(0), buf.get(1));
		}

		static String toBase64Url(final UUID uuid) {
			return encoder.encodeToString(ByteBuffer.allocate(Long.BYTES * 2).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()).array());
		}
	}

}
