package io.github.albertus82.filestore;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;

/** BLOB extraction strategy. */
public interface BlobExtractor {

	/**
	 * Returns an {@link InputStream} to read the file content.
	 *
	 * @param resultSet the SQL result set
	 * @param blobColumnIndex the index of the BLOB column
	 *
	 * @return an {@link InputStream} to read the file content.
	 *
	 * @throws SQLException if a database access error occurs
	 */
	InputStream getInputStream(ResultSet resultSet, int blobColumnIndex) throws SQLException;

}
