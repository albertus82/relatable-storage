package io.github.albertus82.filestore;

import java.io.IOException;
import java.io.InputStream;
import java.io.StreamCorruptedException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.security.DigestInputStream;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
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
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.Deflater;
import java.util.zip.DeflaterInputStream;
import java.util.zip.InflaterInputStream;

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
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.StatementCallback;
import org.springframework.jdbc.core.support.AbstractLobCreatingPreparedStatementCallback;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobCreator;

/** Basic RDBMS-based implementation of a filestore. */
@SuppressWarnings("java:S1130") // "throws" declarations should not be superfluous
public class SimpleJdbcFileStore implements SimpleFileStore {

	private static final byte[] HEX_ARRAY = "0123456789abcdef".getBytes(StandardCharsets.US_ASCII);
	private static final String DIGEST_ALGORITHM = "SHA-256";
	private static final String SQL_ESCAPE = "\\";

	private static final String SECRET_KEY_ALGORITHM = "PBKDF2WithHmacSHA256";
	private static final String TRANSFORMATION = "AES/CBC/PKCS5PADDING";
	private static final byte INITIALIZATION_VECTOR_LENGTH = 16;
	private static final byte SALT_LENGTH = 32;

	private static final Logger log = Logger.getLogger(SimpleJdbcFileStore.class.getName());

	private final JdbcOperations jdbcOperations;
	private final String schema;
	private final String table;
	private final Compression compression;
	private final BlobExtractor blobExtractor;
	private final char[] password;

	/**
	 * Creates a new instance based on a database table in the specified schema,
	 * with encryption.
	 * 
	 * @param jdbcOperations the JDBC executor
	 * @param schema the database schema name
	 * @param table the database table name
	 * @param compression the data compression level
	 * @param blobExtractor the BLOB extraction strategy
	 * @param password the encryption/decryption password
	 */
	public SimpleJdbcFileStore(final JdbcOperations jdbcOperations, final String schema, final String table, final Compression compression, final BlobExtractor blobExtractor, final char[] password) {
		Objects.requireNonNull(jdbcOperations, "jdbcOperations must not be null");
		Objects.requireNonNull(table, "table must not be null");
		Objects.requireNonNull(compression, "compression must not be null");
		Objects.requireNonNull(blobExtractor, "blobExtractor must not be null");
		if (table.isBlank()) {
			throw new IllegalArgumentException("table must not be blank");
		}
		if (schema != null && schema.isBlank()) {
			throw new IllegalArgumentException("schema must not be blank");
		}
		if (password != null && password.length == 0) {
			throw new IllegalArgumentException("password must not be empty");
		}
		this.jdbcOperations = jdbcOperations;
		this.schema = schema;
		this.table = table;
		this.compression = compression;
		this.blobExtractor = blobExtractor;
		this.password = password != null ? password.clone() : null;
	}

	/**
	 * Creates a new instance based on a database table in the default schema, with
	 * encryption.
	 * 
	 * @param jdbcOperations the JDBC executor
	 * @param table the SQL table name
	 * @param compression the data compression level
	 * @param blobExtractor the BLOB extraction strategy
	 * @param password the encryption/decryption password
	 */
	public SimpleJdbcFileStore(final JdbcOperations jdbcOperations, final String table, final Compression compression, final BlobExtractor blobExtractor, final char[] password) {
		this(jdbcOperations, null, table, compression, blobExtractor, password);
	}

	/**
	 * Creates a new instance based on a database table in the default schema,
	 * without encryption.
	 *
	 * @param jdbcOperations the JDBC executor
	 * @param table the SQL table name
	 * @param compression the data compression level
	 * @param blobExtractor the BLOB extraction strategy
	 */
	public SimpleJdbcFileStore(final JdbcOperations jdbcOperations, final String table, final Compression compression, final BlobExtractor blobExtractor) {
		this(jdbcOperations, null, table, compression, blobExtractor, null);
	}

	/**
	 * Creates a new instance based on a database table in the specified schema,
	 * without encryption.
	 *
	 * @param jdbcOperations the JDBC executor
	 * @param schema the database schema name
	 * @param table the database table name
	 * @param compression the data compression level
	 * @param blobExtractor the BLOB extraction strategy
	 */
	public SimpleJdbcFileStore(final JdbcOperations jdbcOperations, final String schema, final String table, final Compression compression, final BlobExtractor blobExtractor) {
		this(jdbcOperations, schema, table, compression, blobExtractor, null);
	}

	/**
	 * Returns the schema name, or null if no schema name was specified.
	 *
	 * @return the schema name, can be null.
	 */
	public String getSchema() {
		return schema;
	}

	/**
	 * Returns the table name.
	 *
	 * @return the table name
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
		final StringBuilder sb = new StringBuilder("SELECT filename, content_length, last_modified, sha256_base64 FROM ");
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
			return jdbcOperations.query(sql, (rs, rowNum) -> new DatabaseResource(rs.getString(1), rs.getLong(2), rs.getTimestamp(3).getTime(), bytesToHex(Base64.getDecoder().decode(rs.getString(4)))), args.toArray(new Object[args.size()]));
		}
		catch (final DataAccessException e) {
			throw new IOException(e);
		}
	}

	@Override
	public DatabaseResource get(final String fileName) throws NoSuchFileException, IOException {
		Objects.requireNonNull(fileName, "fileName must not be null");
		final StringBuilder sb = new StringBuilder("SELECT content_length, last_modified, sha256_base64 FROM ");
		appendSchemaAndTableName(sb).append(" WHERE filename=?");
		final String sql = sb.toString();
		logStatement(sql);
		try {
			return jdbcOperations.query(sql, rs -> {
				if (rs.next()) {
					return new DatabaseResource(fileName, rs.getLong(1), rs.getTimestamp(2).getTime(), bytesToHex(Base64.getDecoder().decode(rs.getString(3))));
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
		final InsertResult ir = insert(resource, fileName);
		final StringBuilder sb = new StringBuilder("UPDATE ");
		appendSchemaAndTableName(sb).append(" SET content_length=?, sha256_base64=? WHERE filename=?");
		final String sql = sb.toString();
		logStatement(sql);
		try {
			jdbcOperations.update(sql, ir.getContentLength(), Base64.getEncoder().withoutPadding().encodeToString(ir.getSha256Digest()), fileName);
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

	private InsertResult insert(final Resource resource, final String fileName) throws IOException {
		final long contentLength = resource.isOpen() ? -1 : resource.contentLength();
		final StringBuilder sb = new StringBuilder("INSERT INTO ");
		appendSchemaAndTableName(sb);
		if (password == null) {
			sb.append(" (filename, last_modified, compressed, file_contents) VALUES (?, ?, ?, ?)");
		}
		else {
			sb.append(" (filename, last_modified, compressed, file_contents, iv_salt_base64) VALUES (?, ?, ?, ?, ?)");
		}
		final String sql = sb.toString();
		logStatement(sql);
		try (final InputStream ris = resource.getInputStream(); final DigestInputStream dis = new DigestInputStream(ris, MessageDigest.getInstance(DIGEST_ALGORITHM)); final CountingInputStream cis = new CountingInputStream(dis)) {
			try (final InputStream plainTextInputStream = Compression.NONE.equals(compression) ? cis : new DeflaterInputStream(cis, new Deflater(getDeflaterLevel(compression)))) {
				final EncryptionCipherHolder holder = password != null ? new EncryptionCipherHolder(password) : null;
				try (final InputStream inputStream = holder != null ? new CipherInputStream(plainTextInputStream, holder.getCipher()) : plainTextInputStream) {
					jdbcOperations.execute(sql, new AbstractLobCreatingPreparedStatementCallback(new DefaultLobHandler()) {
						@Override
						protected void setValues(final PreparedStatement ps, final LobCreator lobCreator) throws SQLException {
							ps.setString(1, fileName);
							ps.setTimestamp(2, determineLastModifiedTimestamp(resource));
							ps.setBoolean(3, !Compression.NONE.equals(compression));
							lobCreator.setBlobAsBinaryStream(ps, 4, inputStream, Compression.NONE.equals(compression) && contentLength < Integer.MAX_VALUE ? (int) contentLength : -1);
							if (password != null) {
								final byte[] ivSalt = Arrays.copyOf(holder.getInitializationVector(), holder.getInitializationVector().length + holder.getSalt().length);
								System.arraycopy(holder.getSalt(), 0, ivSalt, holder.getInitializationVector().length, holder.getSalt().length);
								ps.setString(5, Base64.getEncoder().withoutPadding().encodeToString(ivSalt));
							}
						}
					});
				}
			}
			if (contentLength != -1 && cis.getCount() != contentLength) {
				throw new StreamCorruptedException("Inconsistent content length (expected: " + contentLength + ", actual: " + cis.getCount() + ")");
			}
			return new InsertResult(cis.getCount(), dis.getMessageDigest().digest());
		}
		catch (final DuplicateKeyException e) {
			logException(e, () -> fileName);
			throw new FileAlreadyExistsException(fileName);
		}
		catch (final DataAccessException e) {
			throw new IOException(e);
		}
		catch (final NoSuchAlgorithmException e) {
			throw new UnsupportedOperationException(DIGEST_ALGORITHM, e);
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

	/** {@link Resource} implementation with a RDBMS table target. */
	public class DatabaseResource extends AbstractResource { // NOSONAR Override the "equals" method in this class. Subclasses that add fields should override "equals" (java:S2160)

		private final String fileName;
		private final long contentLength;
		private final long lastModified;
		private final String sha256Hex;

		private DatabaseResource(final String fileName, final long contentLength, final long lastModified, final String sha256Hex) {
			Objects.requireNonNull(fileName, "fileName must not be null");
			this.fileName = fileName;
			this.contentLength = contentLength;
			this.lastModified = lastModified;
			this.sha256Hex = sha256Hex;
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

		/**
		 * Returns the SHA-256 checksum of the content, in hexadecimal format.
		 *
		 * @return the SHA-256 checksum of the content, in hexadecimal format.
		 */
		public String getSha256Hex() {
			return sha256Hex;
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
			final StringBuilder sb = new StringBuilder("SELECT compressed, iv_salt_base64, file_contents FROM ");
			appendSchemaAndTableName(sb).append(" WHERE filename=?");
			final String sql = sb.toString();
			logStatement(sql);
			try {
				return jdbcOperations.query(sql, rs -> {
					if (rs.next()) {
						final boolean compressed = rs.getBoolean(1);
						final String ivSaltBase64 = rs.getString(2);
						final InputStream plainTextInputStream = blobExtractor.getInputStream(rs, 3);
						final InputStream inputStream;
						if (ivSaltBase64 != null) {
							final byte[] ivSalt = Base64.getDecoder().decode(ivSaltBase64);
							final Cipher cipher = createDecryptionCipher(password, Arrays.copyOf(ivSalt, INITIALIZATION_VECTOR_LENGTH), Arrays.copyOfRange(ivSalt, INITIALIZATION_VECTOR_LENGTH, INITIALIZATION_VECTOR_LENGTH + SALT_LENGTH));
							inputStream = new CipherInputStream(plainTextInputStream, cipher);
						}
						else {
							inputStream = plainTextInputStream;
						}
						return compressed ? new InflaterInputStream(inputStream) : inputStream;
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

		private Cipher createDecryptionCipher(final char[] password, final byte[] initializationVector, final byte[] salt) {
			try {
				final Cipher cipher = Cipher.getInstance(TRANSFORMATION);
				cipher.init(Cipher.DECRYPT_MODE, generateKeyFromPassword(password, salt, TRANSFORMATION), new IvParameterSpec(initializationVector));
				return cipher;
			}
			catch (final GeneralSecurityException e) {
				throw new IllegalStateException(e);
			}
		}
	}

	private static int getDeflaterLevel(final Compression compression) {
		switch (compression) {
		case HIGH:
			return Deflater.BEST_COMPRESSION;
		case LOW:
			return Deflater.BEST_SPEED;
		case MEDIUM:
			return Deflater.DEFAULT_COMPRESSION;
		default:
			throw new IllegalArgumentException(compression.toString());
		}
	}

	private static String bytesToHex(final byte[] bytes) {
		final byte[] hexChars = new byte[bytes.length * 2];
		for (int j = 0; j < bytes.length; j++) {
			final int v = bytes[j] & 0xFF;
			hexChars[j * 2] = HEX_ARRAY[v >>> 4];
			hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
		}
		return new String(hexChars, StandardCharsets.UTF_8);
	}

	private static class InsertResult {
		private final long contentLength;
		private final byte[] sha256Digest;

		private InsertResult(final long contentLength, final byte[] sha256Digest) {
			this.contentLength = contentLength;
			this.sha256Digest = sha256Digest;
		}

		private long getContentLength() {
			return contentLength;
		}

		private byte[] getSha256Digest() {
			return sha256Digest;
		}
	}

	private static class EncryptionCipherHolder {
		private final Cipher cipher;
		private final byte[] initializationVector;
		private final byte[] salt;

		private EncryptionCipherHolder(final char[] password) {
			Objects.requireNonNull(password);
			final SecureRandom random = new SecureRandom();
			initializationVector = new byte[INITIALIZATION_VECTOR_LENGTH];
			random.nextBytes(initializationVector);
			salt = new byte[SALT_LENGTH];
			random.nextBytes(salt);
			try {
				cipher = Cipher.getInstance(TRANSFORMATION);
				cipher.init(Cipher.ENCRYPT_MODE, generateKeyFromPassword(password, salt, TRANSFORMATION), new IvParameterSpec(initializationVector));
			}
			catch (final GeneralSecurityException e) {
				throw new IllegalStateException(e);
			}
		}

		private Cipher getCipher() {
			return cipher;
		}

		private byte[] getInitializationVector() {
			return initializationVector;
		}

		private byte[] getSalt() {
			return salt;
		}
	}

	private static SecretKey generateKeyFromPassword(final char[] password, final byte[] salt, final String algorithm) {
		Objects.requireNonNull(password);
		Objects.requireNonNull(salt);
		Objects.requireNonNull(algorithm);
		try {
			final SecretKeyFactory factory = SecretKeyFactory.getInstance(SECRET_KEY_ALGORITHM);
			final KeySpec spec = new PBEKeySpec(password, salt, 65536, 256);
			final int indexOfSlash = algorithm.indexOf('/');
			return new SecretKeySpec(factory.generateSecret(spec).getEncoded(), indexOfSlash == -1 ? algorithm : algorithm.substring(0, indexOfSlash));
		}
		catch (final InvalidKeySpecException | NoSuchAlgorithmException | RuntimeException e) {
			throw new IllegalStateException(e);
		}
	}

}
