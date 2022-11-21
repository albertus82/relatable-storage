package io.github.albertus82.filestore.io;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * An extension of {@link GZIPOutputStream} with customizable compression level.
 */
public class CustomLevelGzipOutputStream extends GZIPOutputStream {

	/**
	 * Creates a new output stream with a default buffer size and the specified
	 * flush mode.
	 *
	 * @param out the output stream
	 * @param syncFlush if {@code true} invocation of the inherited
	 *        {@link DeflaterOutputStream#flush() flush()} method of this instance
	 *        flushes the compressor with flush mode {@link Deflater#SYNC_FLUSH}
	 *        before flushing the output stream, otherwise only flushes the output
	 *        stream
	 * @param compressionLevel the new compression level (0-9)
	 *
	 * @throws IOException If an I/O error has occurred.
	 * @throws IllegalArgumentException if the compression level is invalid
	 */
	public CustomLevelGzipOutputStream(final OutputStream out, final boolean syncFlush, final int compressionLevel) throws IOException {
		super(out, syncFlush);
		def.setLevel(compressionLevel);
	}

	/**
	 * Creates a new output stream with the specified buffer size and flush mode.
	 *
	 * @param out the output stream
	 * @param size the output buffer size
	 * @param syncFlush if {@code true} invocation of the inherited
	 *        {@link DeflaterOutputStream#flush() flush()} method of this instance
	 *        flushes the compressor with flush mode {@link Deflater#SYNC_FLUSH}
	 *        before flushing the output stream, otherwise only flushes the output
	 *        stream
	 * @param compressionLevel the new compression level (0-9)
	 * 
	 * @throws IOException If an I/O error has occurred.
	 * @throws IllegalArgumentException if {@code size <= 0} or if the compression
	 *         level is invalid
	 */
	public CustomLevelGzipOutputStream(final OutputStream out, final int size, final boolean syncFlush, final int compressionLevel) throws IOException {
		super(out, size, syncFlush);
		def.setLevel(compressionLevel);
	}

	/**
	 * Creates a new output stream with the specified buffer size.
	 *
	 * <p>
	 * The new output stream instance is created as if by invoking the 3-argument
	 * constructor GZIPOutputStream(out, size, false).
	 *
	 * @param out the output stream
	 * @param size the output buffer size
	 * @param compressionLevel the new compression level (0-9)
	 *
	 * @throws IOException If an I/O error has occurred.
	 * @throws IllegalArgumentException if {@code size <= 0} or if the compression
	 *         level is invalid
	 */
	public CustomLevelGzipOutputStream(final OutputStream out, final int size, final int compressionLevel) throws IOException {
		super(out, size);
		def.setLevel(compressionLevel);
	}

	/**
	 * Creates a new output stream with a default buffer size.
	 *
	 * <p>
	 * The new output stream instance is created as if by invoking the 2-argument
	 * constructor GZIPOutputStream(out, false).
	 *
	 * @param out the output stream
	 * @param compressionLevel the new compression level (0-9)
	 *
	 * @throws IOException If an I/O error has occurred.
	 * @throws IllegalArgumentException if the compression level is invalid
	 */
	public CustomLevelGzipOutputStream(final OutputStream out, final int compressionLevel) throws IOException {
		super(out);
		def.setLevel(compressionLevel);
	}

}
