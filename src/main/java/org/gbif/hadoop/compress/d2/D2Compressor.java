package org.gbif.hadoop.compress.d2;

import java.io.IOException;
import java.util.zip.Deflater;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;

/**
 * A deflater that provides hooks to run as a compressor with Hadoop.
 */
public class D2Compressor extends Deflater implements Compressor {

  /**
   * Uses best compression, and instructs the no wrap mode.
   */
  public D2Compressor() {
    super(BEST_COMPRESSION, true);
  }

  @Override
  public int compress(byte[] b, int off, int len) throws IOException {
    return deflate(b, off, len, SYNC_FLUSH);
  }

  @Override
  public void reinit(Configuration conf) {
    reset();
  }
}
