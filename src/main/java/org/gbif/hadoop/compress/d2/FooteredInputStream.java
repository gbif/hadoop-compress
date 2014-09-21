package org.gbif.hadoop.compress.d2;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 * An {@link java.io.BufferedInputStream} that is aware of a fixed length footer at the end of the stream. Clients
 * reading from this stream will not be delivered the footer while reading, but can ask for it after closing.
 * <p/>
 * This extends BufferedInputStream for performance reasons.  Each read call requires that we "peek" ahead in the
 * underlying stream and by using a buffer we are reading a local byte cache.
 */
public class FooteredInputStream extends BufferedInputStream {

  private static final int EOF = -1;
  private static final int DEFAULT_BUFFER_SIZE = 8192; // same as BufferedInputStream
  private final int footerLengthBytes;
  private final byte[] footer;
  private boolean footerPopulated;
  private boolean open = true;

  public FooteredInputStream(InputStream in, int footerLengthBytes) {
    this(in, DEFAULT_BUFFER_SIZE, footerLengthBytes);
  }

  public FooteredInputStream(InputStream in, int bufferSize, int footerLengthBytes) {
    super(in, bufferSize);
    if (footerLengthBytes <= 0) {
      throw new IllegalArgumentException("Footer length must be greater than 0");
    }
    footer = new byte[footerLengthBytes];
    this.footerLengthBytes = footerLengthBytes;
  }

  /**
   * Simply performs {@link java.io.BufferedInputStream#read()} provided it will not return content in the footer,
   * otherwise returns {@link org.gbif.hadoop.compress.d2.FooteredInputStream#EOF};
   */
  @Override
  public synchronized int read() throws IOException {
    return availableBeforeFooter(1) <= 0 ? EOF : super.read();
  }

  @Override
  public synchronized int read(byte[] b, int off, int len) throws IOException {
    int available = availableBeforeFooter(len);
    return available <= 0 ? EOF : super.read(b, off, available);
  }

  @Override
  public synchronized int available() throws IOException {
    return Math.max(super.available() - footerLengthBytes, 0);
  }

  /**
   * Sets the footer, if an only if we have reached the end of the stream, and then calls the parent close().
   */
  @Override
  public synchronized void close() throws IOException {
    if (open) {
      // Only set the footer if the stream was read to completion
      if (availableBeforeFooter(1) == 0) {
        // important that we use parent here, to skip the test for available bytes
        int read = super.read(footer, 0, footerLengthBytes);
        if (read == footerLengthBytes) {
          footerPopulated = true;
        } else {
          throw new IllegalStateException("Expected to read "
                                          + footerLengthBytes
                                          + " into the footer, but could only read "
                                          + read);
        }
      }
      super.close();
      open = false; // defensive against multiple calls to close() which easily happens with try with resources
    }
  }

  /**
   * Provides access to the fixed length footer, but <b>must</b> only be accessed once reading the stream to
   * completion.
   *
   * @return the fixed length footer
   *
   * @throws IllegalStateException If called but the stream was not read to completion
   */
  public synchronized byte[] getFooter() {
    if (footerPopulated) {
      return Arrays.copyOf(footer, footer.length);
    } else {
      throw new IllegalStateException(
        "Unable to provide footer when the stream has not be read to completion - perhaps you forgot to read fully and close()?");
    }
  }

  /**
   * Returns the number of bytes available without returning footer content up to a maximum number of requested bytes.
   * If the caller requests 10 bytes, and there are 1000 before the footer, this will return 10.  If the caller
   * requests
   * 10 bytes, and there are 7 before the footer, this will return 7.
   *
   * @param requestedBytes The number of bytes requested by the caller
   */
  private int availableBeforeFooter(int requestedBytes) throws IOException {
    int totalLength = requestedBytes + footerLengthBytes;
    mark(totalLength);
    try {
      int read = super.read(new byte[totalLength], 0, totalLength); // importantly calls the parent!
      return read - footerLengthBytes;  // how many bytes can actually be read
    } finally {
      reset();
    }
  }
}
