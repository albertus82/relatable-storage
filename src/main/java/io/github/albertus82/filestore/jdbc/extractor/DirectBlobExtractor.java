package io.github.albertus82.filestore.jdbc.extractor;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * BLOB extraction strategy that directly returns the {@link InputStream}
 * created by the JDBC driver.
 */
public class DirectBlobExtractor implements BlobExtractor {

	@Override
	public InputStream getInputStream(final ResultSet resultSet, final int blobColumnIndex) throws SQLException {
		return resultSet.getBinaryStream(blobColumnIndex);
	}

}
