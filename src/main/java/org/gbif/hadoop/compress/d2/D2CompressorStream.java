package org.gbif.hadoop.compress.d2;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.compress.CompressorStream;

public class D2CompressorStream extends CompressorStream {

  private static final int DEFAULT_BUFFER_SIZE = 8024;
  private final D2Compressor compressor;

  public D2CompressorStream(OutputStream out, D2Compressor compressor, int bufferSize) {
    super(out, compressor, bufferSize);
    this.compressor = compressor;
  }

  public D2CompressorStream(OutputStream out, D2Compressor compressor) {
    this(out, compressor, DEFAULT_BUFFER_SIZE);
  }

  public D2CompressorStream(OutputStream out) {
    this(out, new D2Compressor(), DEFAULT_BUFFER_SIZE);
  }

  /**
   * Flushes the compressor and then the compressed output stream.
   * Note that because the {@link org.gbif.hadoop.compress.d2.D2Compressor} always runs in SYNC_MODE this exhibits the
   * same behavior as {@link java.util.zip.DeflaterOutputStream} in SYNC_MODE.
   */
  public void flush() throws IOException {
    if (!compressor.finished()) {
      int len = 0;
      while ((len = compressor.compress(buffer, 0, buffer.length)) > 0) {
        out.write(buffer, 0, len);
        if (len < buffer.length) {
          break;
        }
      }
    }
    out.flush();
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    super.write(b, off, len);
  }

  /**
   * Ensures deflated bytes are flushed so counts are accurate and then writes the custom fixed length footer to the
   * underlying stream.
   */
  @Override
  public void finish() throws IOException {
    flush(); // make sure compressor flushes so that lengths and CRC are accurate

    // Push the custom footer to the output stream, not deflation stream (important or lengths and CRC are adjusted)
    out.write(D2Footer.serialize(compressor.getBytesRead(), compressor.getBytesWritten(), compressor.getCRC32()));
    out.flush(); // and flush it
  }
}
