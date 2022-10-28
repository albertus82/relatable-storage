package io.github.albertus82.filestore;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * BLOB extraction strategy that buffers the entire BLOB content in memory. Note
 * that this strategy is intended for simple cases where it is convenient to
 * read all bytes into a byte array. It is not intended for reading in large
 * files.
 */
public class MemoryBufferedBlobExtractor implements BlobExtractor {

	@Override
	public InputStream getInputStream(final ResultSet resultSet, final int blobColumnIndex) throws SQLException {
		return new ByteArrayInputStream(resultSet.getBytes(blobColumnIndex));
	}

}
