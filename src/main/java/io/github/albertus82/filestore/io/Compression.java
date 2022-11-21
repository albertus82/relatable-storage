package io.github.albertus82.filestore.io;

import java.util.zip.Deflater;

/** Compression level. */
public enum Compression {

	/** No compression. */
	NONE(Deflater.NO_COMPRESSION),

	/** Fastest compression. */
	LOW(Deflater.BEST_SPEED),

	/** Default compression level. */
	MEDIUM(Deflater.DEFAULT_COMPRESSION),

	/** Best compression. */
	HIGH(Deflater.BEST_COMPRESSION);

	private final int deflaterLevel;

	Compression(final int deflaterLevel) {
		this.deflaterLevel = deflaterLevel;
	}

	public int getDeflaterLevel() {
		return deflaterLevel;
	}

}
