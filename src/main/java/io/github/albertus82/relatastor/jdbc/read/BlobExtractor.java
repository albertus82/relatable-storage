package io.github.albertus82.relatastor.jdbc.read;

import java.io.InputStream;
import java.sql.SQLException;

/** BLOB extraction strategy. */
public interface BlobExtractor {

	/**
	 * Returns an {@link InputStream} to read the BLOB content.
	 *
	 * @param blobAccessor methods that retrieve the value of the BLOB column
	 *        directly from the database
	 *
	 * @return an {@link InputStream} to read the BLOB content
	 *
	 * @throws SQLException if a database access error occurs
	 */
	InputStream getInputStream(BlobAccessor blobAccessor) throws SQLException;

}
