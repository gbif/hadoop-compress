/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.hadoop.compress.d2;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.compress.CompressorStream;

public class D2CompressorStream extends CompressorStream {

  private static final int DEFAULT_BUFFER_SIZE = 102768;
  private static final String BUFFER_SIZE_ENV_VAR = "D2_BUFFER_SIZE"; // Name of the environment variable

  private final D2Compressor compressor;

  private static int getBufferSizeFromEnv() {
    String bufferSizeStr = System.getenv(BUFFER_SIZE_ENV_VAR);
    if (bufferSizeStr != null) {
      try {
        return Integer.parseInt(bufferSizeStr);
      } catch (NumberFormatException e) {
        System.err.println("Invalid buffer size provided in environment variable. Using default buffer size.");
      }
    }
    return DEFAULT_BUFFER_SIZE;
  }
  public D2CompressorStream(OutputStream out, D2Compressor compressor, int bufferSize) {
    super(out, compressor, bufferSize);
    System.out.println("Using buffer size: " + bufferSize);
    this.compressor = compressor;
  }

  public D2CompressorStream(OutputStream out, D2Compressor compressor) {
    this(out, compressor, getBufferSizeFromEnv());
  }

  public D2CompressorStream(OutputStream out) {
    this(out, new D2Compressor(), getBufferSizeFromEnv());
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
