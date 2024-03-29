package io.github.albertus82.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.util.unit.DataSize;

import com.thedeanda.lorem.LoremIpsum;

public class TestUtils {

	private static final Logger log = Logger.getLogger(TestUtils.class.getName());

	private static final LoremIpsum lorem = LoremIpsum.getInstance();

	public static byte[] createDummyByteArray(final DataSize size) throws IOException {
		if (size == null) {
			throw new NullPointerException();
		}
		if (size.toBytes() < 0) {
			throw new IllegalArgumentException(size.toString());
		}
		final var words = 200;
		final ByteBuffer buf = ByteBuffer.allocate((int) size.toBytes() + words * 9);
		while (buf.position() < size.toBytes()) {
			buf.put(lorem.getWords(words).getBytes(StandardCharsets.US_ASCII));
		}
		return Arrays.copyOf(buf.array(), (int) size.toBytes());
	}

	public static Path createDummyFile(final DataSize size) throws IOException {
		if (size == null) {
			throw new NullPointerException();
		}
		if (size.toBytes() < 0) {
			throw new IllegalArgumentException(size.toString());
		}
		log.log(Level.FINE, "Creating {0} dummy file...", size);
		long currSize = 0;
		final Path tmp = Files.createTempFile(null, null);
		tmp.toFile().deleteOnExit();
		try (final Writer w = Files.newBufferedWriter(tmp, StandardCharsets.US_ASCII, StandardOpenOption.TRUNCATE_EXISTING)) {
			while (currSize < size.toBytes()) {
				final String s = lorem.getWords(1000);
				currSize += s.length();
				w.append(s).append(System.lineSeparator());
			}
		}
		try (final RandomAccessFile raf = new RandomAccessFile(tmp.toFile(), "rw"); final FileChannel fc = raf.getChannel()) {
			fc.truncate(size.toBytes());
		}
		if (tmp.toFile().length() != size.toBytes()) {
			throw new IllegalStateException();
		}
		log.log(Level.FINE, "Dummy file created: \"{0}\".", tmp);
		return tmp;
	}

	public static void deleteIfExists(final Path... files) {
		if (files != null) {
			for (final Path file : files) {
				if (file != null) {
					try {
						Files.deleteIfExists(file);
					}
					catch (final IOException e) {
						log.log(Level.WARNING, e, () -> "Cannot delete file \"" + file + "\":");
						file.toFile().deleteOnExit();
					}
				}
			}
		}
	}

	public static String sha256Hex(final Path file) throws IOException {
		try (final InputStream data = Files.newInputStream(file)) {
			return DigestUtils.sha256Hex(data);
		}
	}

}
