package io.github.albertus82.filestore.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.unit.DataSize;

import io.github.albertus82.filestore.TestConfig;
import io.github.albertus82.filestore.TestUtils;
import io.github.albertus82.filestore.io.Compression;
import io.github.albertus82.filestore.jdbc.SimpleJdbcFileStore.DatabaseResource;
import io.github.albertus82.filestore.jdbc.read.FileBufferedBlobExtractor;
import io.github.albertus82.filestore.jdbc.read.MemoryBufferedBlobExtractor;
import io.github.albertus82.filestore.jdbc.write.MemoryBufferedBinaryStreamProvider;

@SpringJUnitConfig(TestConfig.class)
class PerformanceTest {

	private static final String TABLE_NAME = "STORAGE";
	private static final short FILE_SIZE = 256;
	private static final byte ITERATION_COUNT = 5;

	private static final Logger log = Logger.getLogger(PerformanceTest.class.getName());

	private final AtomicLong totalWritingTime = new AtomicLong();
	private final AtomicLong totalReadingTime = new AtomicLong();

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@BeforeAll
	static void beforeAll() {
		System.out.printf("File size: %d MB.%n", FILE_SIZE);
		System.out.printf("Iteration count: %d.%n", ITERATION_COUNT);
	}

	@BeforeEach
	void beforeEach() {
		new ResourceDatabasePopulator(new ClassPathResource(getClass().getPackageName().replace('.', '/') + "/table.sql")).execute(jdbcTemplate.getDataSource());
		totalWritingTime.set(0);
		totalReadingTime.set(0);
	}

	@AfterEach
	void afterEach() {
		final long averageWritingTimeMillis = TimeUnit.NANOSECONDS.toMillis(totalWritingTime.get() / ITERATION_COUNT);
		final long averageReadingTimeMillis = TimeUnit.NANOSECONDS.toMillis(totalReadingTime.get() / ITERATION_COUNT);
		System.out.printf("Avg write: %d ms (%.1f MB/s).%n", averageWritingTimeMillis, FILE_SIZE * 1000d / averageWritingTimeMillis);
		System.out.printf("Avg read: %d ms (%.1f MB/s).%n", averageReadingTimeMillis, FILE_SIZE * 1000d / averageReadingTimeMillis);
		jdbcTemplate.execute("TRUNCATE TABLE storage");
		jdbcTemplate.execute("DROP TABLE storage");
		jdbcTemplate.execute("SHUTDOWN COMPACT");
	}

	@Test
	void inMemory() {
		logStart(Thread.currentThread().getStackTrace()[1].getMethodName());
		test(new SimpleJdbcFileStore(jdbcTemplate, TABLE_NAME, new MemoryBufferedBlobExtractor()).withBinaryStreamProvider(new MemoryBufferedBinaryStreamProvider()));
	}

	@Test
	void inMemoryWithEncryption() {
		logStart(Thread.currentThread().getStackTrace()[1].getMethodName());
		test(new SimpleJdbcFileStore(jdbcTemplate, TABLE_NAME, new MemoryBufferedBlobExtractor()).withBinaryStreamProvider(new MemoryBufferedBinaryStreamProvider()).withEncryption("testpass".toCharArray()));
	}

	@Test
	void inMemoryWithCompression() {
		logStart(Thread.currentThread().getStackTrace()[1].getMethodName());
		test(new SimpleJdbcFileStore(jdbcTemplate, TABLE_NAME, new MemoryBufferedBlobExtractor()).withBinaryStreamProvider(new MemoryBufferedBinaryStreamProvider()).withCompression(Compression.LOW));
	}

	@Test
	void inMemoryWithCompressionAndEncryption() {
		logStart(Thread.currentThread().getStackTrace()[1].getMethodName());
		test(new SimpleJdbcFileStore(jdbcTemplate, TABLE_NAME, new MemoryBufferedBlobExtractor()).withBinaryStreamProvider(new MemoryBufferedBinaryStreamProvider()).withCompression(Compression.LOW).withEncryption("testpass".toCharArray()));
	}

	@Test
	void onDisk() {
		logStart(Thread.currentThread().getStackTrace()[1].getMethodName());
		test(new SimpleJdbcFileStore(jdbcTemplate, TABLE_NAME, new FileBufferedBlobExtractor()));
	}

	@Test
	void onDiskWithEncryption() {
		logStart(Thread.currentThread().getStackTrace()[1].getMethodName());
		test(new SimpleJdbcFileStore(jdbcTemplate, TABLE_NAME, new FileBufferedBlobExtractor()).withEncryption("testpass".toCharArray()));
	}

	@Test
	void onDiskWithCompression() {
		logStart(Thread.currentThread().getStackTrace()[1].getMethodName());
		test(new SimpleJdbcFileStore(jdbcTemplate, TABLE_NAME, new FileBufferedBlobExtractor()).withCompression(Compression.LOW));
	}

	@Test
	void onDiskWithCompressionAndEncryption() {
		logStart(Thread.currentThread().getStackTrace()[1].getMethodName());
		test(new SimpleJdbcFileStore(jdbcTemplate, TABLE_NAME, new FileBufferedBlobExtractor()).withCompression(Compression.LOW).withEncryption("testpass".toCharArray()));
	}

	private static void logStart(final String methodName) {
		System.out.printf("**** %s **", methodName);
	}

	private void test(final SimpleJdbcFileStore store) {
		for (byte i = 1; i <= ITERATION_COUNT; i++) {
			Path tempFile = null;
			try {
				tempFile = TestUtils.createDummyFile(DataSize.ofMegabytes(FILE_SIZE));
				final Path f = tempFile;
				try {
					final String fileName = UUID.randomUUID().toString();
					try (final InputStream is = Files.newInputStream(f)) {
						final byte currentIteration = i;
						Assertions.assertDoesNotThrow(() -> {
							final long t0 = System.nanoTime();
							store.store(new InputStreamResource(is), fileName);
							final long t = System.nanoTime() - t0;
							log.log(Level.FINE, "Written in {0} ms.", TimeUnit.NANOSECONDS.toMillis(t));
							System.out.printf(" %d%% **", 100 / (ITERATION_COUNT * 2) * (currentIteration * 2 - 1));
							totalWritingTime.addAndGet(t);
						});
					}

					final byte[] buffer = new byte[8192];
					final String hashAlgorithm = "SHA-256";
					final MessageDigest digestSource = MessageDigest.getInstance(hashAlgorithm);
					try (final InputStream is = Files.newInputStream(f)) {
						int bytesCount = 0;
						while ((bytesCount = is.read(buffer)) != -1) {
							digestSource.update(buffer, 0, bytesCount);
						}
					}
					final MessageDigest digestStored = MessageDigest.getInstance(hashAlgorithm);
					final long t0 = System.nanoTime();
					final DatabaseResource dr = store.get(fileName);
					try (final InputStream stored = dr.getInputStream()) {
						int bytesCount = 0;
						while ((bytesCount = stored.read(buffer)) != -1) {
							digestStored.update(buffer, 0, bytesCount);
						}
					}
					final long t = System.nanoTime() - t0;
					log.log(Level.FINE, "Read in {0} ms.", TimeUnit.NANOSECONDS.toMillis(t));
					System.out.printf(" %d%% **", 100 / ITERATION_COUNT * i);
					totalReadingTime.addAndGet(t);
					final byte[] sha256Source = digestSource.digest();
					final byte[] sha256Stored = digestStored.digest();
					Assertions.assertArrayEquals(sha256Source, sha256Stored);
				}
				catch (final IOException e) {
					throw new UncheckedIOException(e);
				}
				catch (final NoSuchAlgorithmException e) {
					throw new RuntimeException(e);
				}
			}
			catch (final IOException e) {
				throw new UncheckedIOException(e);
			}
			finally {
				TestUtils.deleteIfExists(tempFile);
			}
		}
		System.out.printf("**%n");
	}

}
