package io.github.albertus82.filestore.jdbc.read;

/**
 * Abstract implementation of {@link BlobAccessor} containing only basic fields
 * and accessors.
 */
public abstract class AbstractBlobAccessor implements BlobAccessor {

	private final boolean compressed;
	private final char[] password;

	/**
	 * Initialize basic fields.
	 *
	 * @param compressed whether the BLOB content is compressed or not
	 * @param password the password needed to decrypt the BLOB content, or null if
	 *        no password is needed
	 */
	protected AbstractBlobAccessor(final boolean compressed, final char[] password) {
		this.compressed = compressed;
		this.password = password;
	}

	@Override
	public boolean isCompressed() {
		return compressed;
	}

	@Override
	public boolean isEncrypted() {
		return password != null;
	}

	@Override
	public char[] getPassword() {
		return password != null ? password.clone() : null;
	}

}
