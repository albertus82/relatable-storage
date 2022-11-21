package io.github.albertus82.filestore.compression;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

public class CustomLevelGzipOutputStream extends GZIPOutputStream {

	public CustomLevelGzipOutputStream(final OutputStream out, final boolean syncFlush, final int compressionLevel) throws IOException {
		super(out, syncFlush);
		def.setLevel(compressionLevel);
	}

	public CustomLevelGzipOutputStream(final OutputStream out, final int size, final boolean syncFlush, final int compressionLevel) throws IOException {
		super(out, size, syncFlush);
		def.setLevel(compressionLevel);
	}

	public CustomLevelGzipOutputStream(final OutputStream out, final int size, final int compressionLevel) throws IOException {
		super(out, size);
		def.setLevel(compressionLevel);
	}

	public CustomLevelGzipOutputStream(final OutputStream out, final int compressionLevel) throws IOException {
		super(out);
		def.setLevel(compressionLevel);
	}

}
