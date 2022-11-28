package io.github.albertus82.filestore.jdbc.write;

import java.util.Objects;

import io.github.albertus82.filestore.io.Compression;

/** Parameters that determine how the data will be stored in the BLOB column */
public class BlobStoreParameters {

	private final Compression compression;
	private final char[] password;

	/**
	 * Creates a new instance initialized with the provided parameters.
	 * 
	 * @param compression the compression level to apply to the data
	 * @param password the password to be used to encrypt data (null for no
	 *        encryption)
	 */
	public BlobStoreParameters(final Compression compression, final char[] password) {
		Objects.requireNonNull(compression, "compression must not be null");
		this.compression = compression;
		this.password = password;
	}

	/**
	 * Returns the compression level to apply to the data.
	 * 
	 * @return the compression level to apply to the data
	 */
	public Compression getCompression() {
		return compression;
	}

	/**
	 * Returns whether the data must be encrypted or not.
	 * 
	 * @return {@code true} if the data must be stored in encrypted form, otherwise
	 *         {@code false}.
	 */
	public boolean isEncryptionRequired() {
		return password != null;
	}

	/**
	 * Returns the password to be used to encrypt data.
	 * 
	 * @return the password to be used to encrypt data
	 */
	public char[] getPassword() {
		return password != null ? password.clone() : null;
	}

}
