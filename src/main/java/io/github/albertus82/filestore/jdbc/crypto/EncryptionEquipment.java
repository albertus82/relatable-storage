package io.github.albertus82.filestore.jdbc.crypto;

import javax.crypto.Cipher;

/** Provides the objects needed to properly encrypt data. */
public interface EncryptionEquipment {

	/**
	 * Returns the {@link Cipher} initialized in {@link Cipher#ENCRYPT_MODE}.
	 * 
	 * @return the {@link Cipher} initialized in {@link Cipher#ENCRYPT_MODE}.
	 */
	Cipher getCipher();

	/**
	 * Returns the cryptographic parameters as well as any other information which
	 * must be saved in plaintext.
	 * 
	 * @return the cryptographic parameters as well as any other information which
	 *         must be saved in plaintext.
	 */
	String getParameters();

}
