package org.gbif.hadoop.compress.d2;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import java.util.zip.DeflaterOutputStream;

/**
 * A {@link java.util.zip.DeflaterOutputStream} which will deflate data in a manner that can either be read in
 * isolation, or merged without inflating to produce e.g. a Zip file.
 * <p/>
 * This uses a deflator operating in unwrapped (e.g. strip header and footer), SYNC_FLUSH (e.g. always flushes to the
 * byte boundary) mode but adds a custom fixed length footer.
 * <p/>
 * Compressed data can therefore be read in 2 ways:
 * <ul>
 * <li>Individually, by using a {@link java.util.zip.Inflater} in no wrap mode, and skipping the last 24 bytes</li>
 * <li>Merged with other data by skipping the last 26 bytes of all streams except the last which you skip 24 bytes</li>
 * </ul>
 * </p>
 * When merging, if one accumulates the uncompressed length, compressed length and combines the CRC-32s one can
 * construct an appropriate Zip entry data descriptor, thus combining data compressed in parallel into a Zip without
 * inflating the data.
 * <p/>
 * For examples of this in action, please see the unit tests which illustrate expected developer behaviour.
 *
 * @see org.gbif.hadoop.compress.d2.CRCCombine
 * @see java.util.zip.Inflater
 * @see <a href="https://www.ietf.org/rfc/rfc1951.txt">DEFLATE Compressed Data Format Specification version 1.3</a>
 */
@SuppressWarnings("MagicNumber")
public class D2OutputStream extends DeflaterOutputStream {

  private final Object lock = new Object();
  private final Checksum checksum = new CRC32(); // tracks CRC of uncompressed data

  public D2OutputStream(OutputStream out) {
    this(out, 1024);
  }

  public D2OutputStream(OutputStream out, int bufferSize) {
    super(out, new D2Compressor(), bufferSize, true); // run in SYNC_FLUSH mode
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    // defensive coding, to safeguard against CRC-32 inaccuracies
    synchronized (lock) {
      super.write(b, off, len);
      checksum.update(b, off, len);
    }
  }

  /**
   * Ensures deflated bytes are flushed so counts are accurate and then writes the custom fixed length footer to the
   * underlying stream.
   */
  @Override
  public void finish() throws IOException {
    flush(); // make sure deflater flushes so that lengths and CRC are accurate

    // Push the custom footer to the output stream, not deflation stream (important or lengths and CRC are adjusted)
    out.write(D2Footer.serialize(def.getBytesRead(), def.getBytesWritten(), getCRC32()));
    out.flush(); // and flush it
  }

  /**
   * @return The CRC of the uncompressed data that was written
   */
  public long getCRC32() {
    return checksum.getValue();
  }
}
