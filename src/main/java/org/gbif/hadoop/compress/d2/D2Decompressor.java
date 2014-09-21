package org.gbif.hadoop.compress.d2;

import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import org.apache.hadoop.io.compress.Decompressor;

/**
 * A deflater that provides hooks to run as a decompressor with Hadoop and initializes itself in no wrap mode to
 * disable default headers and footers.
 */
public class D2Decompressor extends Inflater implements Decompressor {

  /**
   * Uses no wrap mode.
   */
  public D2Decompressor() {
    super(true);  // forces use of nowrap mode
  }

  @Override
  public int decompress(byte[] b, int off, int len) throws IOException {
    try {
      return inflate(b, off, len);
    } catch (DataFormatException e) {
      throw new IOException(e.getMessage(), e);
    }
  }
}
