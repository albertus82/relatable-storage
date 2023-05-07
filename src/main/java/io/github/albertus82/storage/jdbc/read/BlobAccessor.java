package io.github.albertus82.storage.jdbc.read;

import java.io.InputStream;
import java.sql.SQLException;

/**
 * Methods that retrieve the value of the BLOB column directly from the database
 * as well as other informations required to properly decode the data.
 */
public interface BlobAccessor {

	/**
	 * Retrieves the value of the BLOB column as a stream of uninterpreted bytes.
	 * The value can then be read in chunks from the stream.
	 *
	 * @return a Java input stream that delivers the database column value as a
	 *         stream of uninterpreted bytes; if the value is SQL {@code NULL}, the
	 *         value returned is null.
	 *
	 * @throws SQLException if a database access error occurs or this method is
	 *         called on a closed result set
	 */
	InputStream getBinaryStream() throws SQLException;

	/**
	 * Retrieves the value of the BLOB column as a {@code byte} array in the Java
	 * programming language. The bytes represent the raw values returned by the
	 * driver.
	 *
	 * @return the column value; if the value is SQL {@code NULL}, the value
	 *         returned is null.
	 *
	 * @throws SQLException if a database access error occurs or this method is
	 *         called on a closed result set
	 */
	byte[] getBytes() throws SQLException;

	/**
	 * Returns whether the BLOB content is compressed or not.
	 *
	 * @return {@code true} if BLOB content is compressed, otherwise {@code false}.
	 */
	boolean isCompressed();

	/**
	 * Returns whether the BLOB content is encrypted or not.
	 *
	 * @return {@code true} if BLOB content is encrypted, otherwise {@code false}.
	 *
	 * @see #getPassword()
	 */
	boolean isEncrypted();

	/**
	 * Returns the password needed to decrypt the BLOB content, or null if no
	 * password is needed.
	 *
	 * @return the password needed to decrypt the BLOB content, or null if no
	 *         password is needed.
	 *
	 * @see #isEncrypted()
	 */
	char[] getPassword();

}
