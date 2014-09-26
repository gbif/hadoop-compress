package org.gbif.hadoop.compress.d2;

import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import java.util.zip.Deflater;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;

/**
 * A deflater that provides hooks to run as a compressor with Hadoop and keeps track of a CRC-32 checksum for the
 * uncompressed data.
 */
public class D2Compressor extends Deflater implements Compressor {
  private final Checksum checksum = new CRC32(); // tracks CRC of uncompressed data
  private long bytesWritten;
  private long bytesRead;

  /**
   * Uses best compression, and instructs the no wrap mode.
   */
  public D2Compressor() {
    super(BEST_COMPRESSION, true);
  }

  @Override
  public void setInput(byte[] b, int off, int len) {
    super.setInput(b, off, len);
    checksum.update(b, off, len);
  }

  @Override
  public int compress(byte[] b, int off, int len) throws IOException {
    int compressedSize = deflate(b, off, len, SYNC_FLUSH);

    // copied out, so they are still available even after closing
    bytesWritten = super.getBytesWritten();
    bytesRead = super.getBytesRead();
    return compressedSize;
  }

  @Override
  public void reinit(Configuration conf) {
    reset();
    bytesWritten = bytesRead = 0;
    checksum.reset();
  }

  /**
   * Available after closing.
   * @return The CRC of the uncompressed data that was written
   */
  public long getCRC32() {
    return checksum.getValue();
  }

  /**
   * Unlike parent, available even after closing.
   * @return actual number of bytes written
   */
  @Override
  public long getBytesWritten() {
    return bytesWritten;
  }

  /**
   * Unlike parent, available even after closing.
   * @return actual number of bytes read
   */
  @Override
  public long getBytesRead() {
    return bytesRead;
  }
}
