package io.github.albertus82.filestore.io;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.util.unit.DataSize;

import io.github.albertus82.filestore.TestUtils;

class GzipCompressingInputStreamTest {

	private static final Logger log = Logger.getLogger(GzipCompressingInputStreamTest.class.getName());

	@Test
	void test() throws IOException {
		Path original = null;
		Path compressed = null;
		Path decompressed = null;
		try {
			original = TestUtils.createDummyFile(DataSize.ofMegabytes(32));
			log.log(Level.INFO, "Original length: {0}", original.toFile().length());
			final String srcHash = TestUtils.getSha256Hex(original);
			for (final int compressionLevel : new int[] { Deflater.NO_COMPRESSION, Deflater.BEST_SPEED, Deflater.DEFAULT_COMPRESSION, Deflater.BEST_COMPRESSION }) {
				log.log(Level.INFO, "Compression level: {0}", compressionLevel);
				compressed = Files.createTempFile(null, null);
				try (final var is = Files.newInputStream(original); final var bis = new BufferedInputStream(is); final var gcis = new GzipCompressingInputStream(bis, compressionLevel); final var os = Files.newOutputStream(compressed, StandardOpenOption.TRUNCATE_EXISTING)) {
					gcis.transferTo(os);
				}
				final long compressedLength = compressed.toFile().length();
				log.log(Level.INFO, "Compressed length: {0}", compressedLength);
				if (compressionLevel == Deflater.NO_COMPRESSION) {
					Assertions.assertTrue(compressedLength > original.toFile().length());
				}
				else {
					Assertions.assertTrue(compressedLength < original.toFile().length());
				}

				decompressed = Files.createTempFile(null, null);
				try (final var is = Files.newInputStream(compressed); final var bis = new BufferedInputStream(is); final var gzis = new GZIPInputStream(bis); final var os = Files.newOutputStream(decompressed, StandardOpenOption.TRUNCATE_EXISTING)) {
					gzis.transferTo(os);
				}

				final String decompressedHash = TestUtils.getSha256Hex(decompressed);
				Assertions.assertEquals(srcHash, decompressedHash);
			}
		}
		finally {
			TestUtils.deleteIfExists(original, compressed, decompressed);
		}
	}

}
