package io.github.albertus82.filestore.jdbc;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StreamCorruptedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.Resource;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.LobRetrievalFailureException;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.StatementCallback;
import org.springframework.jdbc.core.support.AbstractLobCreatingPreparedStatementCallback;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobCreator;

import io.github.albertus82.filestore.SimpleFileStore;
import io.github.albertus82.filestore.compression.Compression;
import io.github.albertus82.filestore.compression.GzipCompressingInputStream;
import io.github.albertus82.filestore.jdbc.crypto.EncryptionEquipment;
import io.github.albertus82.filestore.jdbc.extractor.BlobExtractor;

/** Basic RDBMS-based implementation of a filestore. */
@SuppressWarnings("java:S1130") // "throws" declarations should not be superfluous
public class SimpleJdbcFileStore implements SimpleFileStore {

	private static final String SQL_ESCAPE = "\\";

	private static final Logger log = Logger.getLogger(SimpleJdbcFileStore.class.getName());

	private final JdbcOperations jdbcOperations;
	private final String table;
	private final BlobExtractor blobExtractor;
	private final Compression compression;
	private final String schema;
	private final char[] password;

	/**
	 * Creates a new instance based on a database table. All the parameters are
	 * mandatory.
	 *
	 * @param jdbcOperations the JDBC executor
	 * @param table the database table name
	 * @param blobExtractor the BLOB extraction strategy
	 */
	public SimpleJdbcFileStore(final JdbcOperations jdbcOperations, final String table, final BlobExtractor blobExtractor) {
		this(jdbcOperations, table, blobExtractor, Compression.NONE, null, null);
	}

	/**
	 * Creates a new instance based on a database table.
	 *
	 * @param jdbcOperations the JDBC executor (mandatory)
	 * @param table the database table name (mandatory)
	 * @param blobExtractor the BLOB extraction strategy (mandatory)
	 * @param compression the data compression level (mandatory, use
	 *        {@link Compression#NONE} to store uncompressed data).
	 * @param schema the database schema name (can be null, so that no schema name
	 *        will be included in the generated SQL).
	 * @param password the encryption/decryption password (can be null, so that
	 *        neither encryption nor decryption will be performed).
	 */
	protected SimpleJdbcFileStore(final JdbcOperations jdbcOperations, final String table, final BlobExtractor blobExtractor, final Compression compression, final String schema, final char[] password) {
		Objects.requireNonNull(jdbcOperations, "jdbcOperations must not be null");
		Objects.requireNonNull(table, "table must not be null");
		Objects.requireNonNull(blobExtractor, "blobExtractor must not be null");
		Objects.requireNonNull(compression, "compression must not be null");
		if (table.isEmpty()) {
			throw new IllegalArgumentException("table must not be empty");
		}
		this.jdbcOperations = jdbcOperations;
		this.table = table;
		this.blobExtractor = blobExtractor;
		this.compression = compression;
		this.schema = schema;
		this.password = password;
	}

	/**
	 * Enable data compression.
	 *
	 * @param compression the data compression level
	 *
	 * @return a new instance configured with the provided compression level.
	 */
	public SimpleJdbcFileStore withCompression(final Compression compression) {
		return new SimpleJdbcFileStore(this.jdbcOperations, this.table, this.blobExtractor, compression, this.schema, this.password);
	}

	/**
	 * Set a custom database schema name.
	 *
	 * @param schema the database schema name
	 *
	 * @return a new instance configured with the provided schema name.
	 */
	public SimpleJdbcFileStore withSchema(final String schema) {
		Objects.requireNonNull(schema, "schema must not be null");
		if (schema.isEmpty()) {
			throw new IllegalArgumentException("schema must not be empty");
		}
		return new SimpleJdbcFileStore(this.jdbcOperations, this.table, this.blobExtractor, this.compression, schema, this.password);
	}

	/**
	 * Enable data encryption (and decryption).
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
		return new SimpleJdbcFileStore(this.jdbcOperations, this.table, this.blobExtractor, compression, this.schema, password.clone());
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

	@Override
	public List<Resource> list(final String... patterns) throws IOException {
		final StringBuilder sb = new StringBuilder("SELECT filename, content_length, last_modified FROM ");
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
			return jdbcOperations.query(sql, (rs, rowNum) -> new DatabaseResource(rs.getString(1), rs.getLong(2), rs.getTimestamp(3).getTime()), args.toArray(new Object[args.size()]));
		}
		catch (final DataAccessException e) {
			throw new IOException(e);
		}
	}

	@Override
	public DatabaseResource get(final String fileName) throws NoSuchFileException, IOException {
		Objects.requireNonNull(fileName, "fileName must not be null");
		final StringBuilder sb = new StringBuilder("SELECT content_length, last_modified FROM ");
		appendSchemaAndTableName(sb).append(" WHERE filename=?");
		final String sql = sb.toString();
		logStatement(sql);
		try {
			return jdbcOperations.query(sql, rs -> {
				if (rs.next()) {
					return new DatabaseResource(fileName, rs.getLong(1), rs.getTimestamp(2).getTime());
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

		private DatabaseResource(final String fileName, final long contentLength, final long lastModified) {
			Objects.requireNonNull(fileName, "fileName must not be null");
			this.fileName = fileName;
			this.contentLength = contentLength;
			this.lastModified = lastModified;
		}

		/** Returns the file name. */
		@Override
		public String getFilename() {
			return fileName;
		}

		/** Returns the (uncompressed) file size. */
		@Override
		public long contentLength() {
			return contentLength;
		}

		/** Returns the time that the file was last modified. */
		@Override
		public long lastModified() {
			return lastModified;
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
			final StringBuilder sb = new StringBuilder();
			if (password != null) {
				sb.append("SELECT encrypt_params, file_contents FROM ");
			}
			else {
				sb.append("SELECT file_contents FROM ");
			}
			appendSchemaAndTableName(sb).append(" WHERE filename=?");
			final String sql = sb.toString();
			logStatement(sql);
			try {
				return jdbcOperations.query(sql, rs -> {
					if (rs.next()) {
						int columnIndex = 0;
						final String encryptParams = password != null ? rs.getString(++columnIndex) : null;
						final InputStream raw = blobExtractor.getInputStream(rs, ++columnIndex);
						final InputStream in = password == null ? raw : new CipherInputStream(new BufferedInputStream(raw), createDecryptionCipher(password, encryptParams));
						try {
							return new GZIPInputStream(new BufferedInputStream(in));
						}
						catch (final IOException e) {
							throw new LobRetrievalFailureException("Could not extract compressed data", e);
						}
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
	}

	protected Optional<EncryptionEquipment> createEncryptionEquipment(final char[] password) {
		return Optional.ofNullable(password != null ? DefaultEncryptionEquipment.getInstance(password) : null);
	}

	protected Cipher createDecryptionCipher(final char[] password, final String parameters) {
		return DefaultEncryptionEquipment.getDecryptionCipher(password, parameters);
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
		final long contentLength = resource.isOpen() ? -1 : resource.contentLength();
		final StringBuilder sb = new StringBuilder("INSERT INTO ");
		appendSchemaAndTableName(sb);
		final EncryptionEquipment enc = createEncryptionEquipment(password).orElse(null);
		if (enc != null && enc.getParameters() != null) {
			sb.append(" (filename, last_modified, encrypt_params, file_contents) VALUES (?, ?, ?, ?)");
		}
		else {
			sb.append(" (filename, last_modified, file_contents) VALUES (?, ?, ?)");
		}
		final String sql = sb.toString();
		logStatement(sql);
		try (final InputStream ris = resource.getInputStream(); final InputStream bis = new BufferedInputStream(ris); final CountingInputStream cis = new CountingInputStream(bis)) {
			try (final InputStream plainTextInputStream = new GzipCompressingInputStream(cis, compression.getDeflaterLevel())) {
				try (final InputStream inputStream = enc != null ? new CipherInputStream(plainTextInputStream, enc.getCipher()) : plainTextInputStream) {
					jdbcOperations.execute(sql, new AbstractLobCreatingPreparedStatementCallback(new DefaultLobHandler()) {
						@Override
						protected void setValues(final PreparedStatement ps, final LobCreator lobCreator) throws SQLException {
							int columnIndex = 0;
							ps.setString(++columnIndex, fileName);
							ps.setTimestamp(++columnIndex, determineLastModifiedTimestamp(resource));
							if (enc != null && enc.getParameters() != null) {
								ps.setString(++columnIndex, enc.getParameters());
							}
							lobCreator.setBlobAsBinaryStream(ps, ++columnIndex, inputStream, -1);
						}
					});
				}
			}
			if (contentLength != -1 && cis.getCount() != contentLength) {
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
					return stmt.enquoteIdentifier(identifier, true);
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

	private static class DefaultEncryptionEquipment implements EncryptionEquipment {

		private static final String ALGORITHM = "AES";
		private static final String TRANSFORMATION = ALGORITHM + "/CTR/NoPadding";
		private static final byte INITIALIZATION_VECTOR_LENGTH = 16; // bytes
		private static final String SECRET_KEY_ALGORITHM = "PBKDF2WithHmacSHA256";
		private static final short SECRET_KEY_LENGTH = 256; // bits
		private static final int SECRET_KEY_ITERATION_COUNT = 65536;
		private static final byte SECRET_KEY_SALT_LENGTH = 32; // bytes

		private final byte[] salt;
		private final byte[] initializationVector;
		private final Cipher cipher;

		private DefaultEncryptionEquipment(final char[] password) {
			final SecureRandom random = new SecureRandom();
			salt = new byte[SECRET_KEY_SALT_LENGTH];
			random.nextBytes(salt);
			initializationVector = new byte[INITIALIZATION_VECTOR_LENGTH];
			random.nextBytes(initializationVector);
			try {
				cipher = Cipher.getInstance(TRANSFORMATION);
				cipher.init(Cipher.ENCRYPT_MODE, generateKeyFromPassword(password, salt), new IvParameterSpec(initializationVector));
			}
			catch (final GeneralSecurityException e) {
				throw new IllegalStateException(e);
			}
		}

		@Override
		public Cipher getCipher() {
			return cipher;
		}

		@Override
		public String getParameters() {
			final byte[] bytes = Arrays.copyOf(salt, salt.length + initializationVector.length);
			System.arraycopy(initializationVector, 0, bytes, salt.length, initializationVector.length);
			return Base64.getEncoder().withoutPadding().encodeToString(bytes);
		}

		private static EncryptionEquipment getInstance(final char[] password) {
			return new DefaultEncryptionEquipment(password);
		}

		private static Cipher getDecryptionCipher(final char[] password, final String parameters) {
			Objects.requireNonNull(parameters);
			final byte[] bytes = Base64.getDecoder().decode(parameters);
			final byte[] salt = Arrays.copyOf(bytes, SECRET_KEY_SALT_LENGTH);
			final byte[] initializationVector = Arrays.copyOfRange(bytes, SECRET_KEY_SALT_LENGTH, SECRET_KEY_SALT_LENGTH + INITIALIZATION_VECTOR_LENGTH);
			try {
				final Cipher cipher = Cipher.getInstance(TRANSFORMATION);
				cipher.init(Cipher.DECRYPT_MODE, generateKeyFromPassword(password, salt), new IvParameterSpec(initializationVector));
				return cipher;
			}
			catch (final GeneralSecurityException e) {
				throw new IllegalStateException(e);
			}
		}

		private static SecretKey generateKeyFromPassword(final char[] password, final byte[] salt) {
			Objects.requireNonNull(password);
			Objects.requireNonNull(salt);
			try {
				final SecretKeyFactory factory = SecretKeyFactory.getInstance(SECRET_KEY_ALGORITHM);
				final KeySpec spec = new PBEKeySpec(password, salt, SECRET_KEY_ITERATION_COUNT, SECRET_KEY_LENGTH);
				final SecretKey generateSecret = factory.generateSecret(spec);
				return new SecretKeySpec(generateSecret.getEncoded(), ALGORITHM);
			}
			catch (final InvalidKeySpecException | NoSuchAlgorithmException | RuntimeException e) {
				throw new IllegalStateException(e);
			}
		}
	}

}
