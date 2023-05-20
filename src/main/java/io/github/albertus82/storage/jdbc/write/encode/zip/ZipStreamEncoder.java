package io.github.albertus82.storage.jdbc.write.encode.zip;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Month;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Objects;

import io.github.albertus82.storage.io.Compression;
import io.github.albertus82.storage.jdbc.write.BlobStoreParameters;
import io.github.albertus82.storage.jdbc.write.encode.IndirectStreamEncoder;
import net.lingala.zip4j.io.outputstream.ZipOutputStream;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.model.enums.AesKeyStrength;
import net.lingala.zip4j.model.enums.CompressionLevel;
import net.lingala.zip4j.model.enums.EncryptionMethod;

/**
 * Stream encoder that reads bytes from an {@link InputStream}, deflates and
 * encrypts data, and finally writes them to an {@link OutputStream}.
 */
public class ZipStreamEncoder implements IndirectStreamEncoder {

	private static final long FILE_TIME = ZonedDateTime.of(2001, Month.JANUARY.getValue(), 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
	private static final String FILE_NAME = "0";

	/** Default empty constructor. */
	public ZipStreamEncoder() { /* Javadoc */ }

	@Override
	public void encodeStream(final InputStream in, final OutputStream out, final BlobStoreParameters parameters) throws IOException {
		Objects.requireNonNull(in, "InputStream must not be null");
		Objects.requireNonNull(out, "OutputStream must not be null");
		Objects.requireNonNull(parameters, "parameters must not be null");
		try (final ZipOutputStream zos = parameters.isEncryptionRequired() ? new ZipOutputStream(out, parameters.getPassword()) : new ZipOutputStream(out)) {
			zos.putNextEntry(toZipParameters(parameters));
			in.transferTo(zos);
			zos.closeEntry();
		}
	}

	private static ZipParameters toZipParameters(final BlobStoreParameters parameters) {
		Objects.requireNonNull(parameters, "parameters must not be null");
		final ZipParameters zp = new ZipParameters();
		zp.setLastModifiedFileTime(FILE_TIME); // Make ZIP files reproducible to allow deduplication if supported by the DBMS
		zp.setCompressionLevel(toZipCompressionLevel(parameters.getCompression()));
		zp.setFileNameInZip(FILE_NAME); // Make ZIP files reproducible to allow deduplication if supported by the DBMS
		if (parameters.isEncryptionRequired()) {
			zp.setEncryptFiles(true);
			zp.setEncryptionMethod(EncryptionMethod.AES);
			zp.setAesKeyStrength(AesKeyStrength.KEY_STRENGTH_256);
		}
		return zp;
	}

	private static CompressionLevel toZipCompressionLevel(final Compression compression) {
		switch (compression) {
		case HIGH:
			return CompressionLevel.ULTRA;
		case LOW:
			return CompressionLevel.FASTEST;
		case MEDIUM:
			return CompressionLevel.NORMAL;
		case NONE:
			return CompressionLevel.NO_COMPRESSION;
		default:
			throw new IllegalArgumentException(compression.toString());
		}
	}

}
