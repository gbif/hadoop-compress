package org.gbif.hadoop.compress.d2;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.compress.CompressorStream;

public class D2CompressorStream extends CompressorStream {

  private static final int DEFAULT_BUFFER_SIZE = 8192; // same default as BufferedInputStream
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
   * Ensure all bytes of the compressor buffer are flushed, and write the custom fixed length footer to the
   * underlying stream.
   *
   * <p>This avoids the behavior of the parent class where the deflater is instructed to finish which would
   * result in a 2 byte trailing footer (actually a header saying "last block" followed by no data). We avoid
   * that and append our own footer.
   */
  @Override
  public void finish() throws IOException {
    // defensive coding: ensure compressor input buffer is empty
    while (!compressor.needsInput()) {
      compress();
    }

    // Push the custom footer to the output stream, not deflation stream (important or lengths and CRC are adjusted)
    out.write(D2Footer.serialize(compressor.getBytesRead(), compressor.getBytesWritten(), compressor.getCRC32()));
    out.flush(); // and flush it
    compressor.reset(); // defensive coding
  }
}
