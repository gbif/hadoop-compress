package org.gbif.hadoop.compress.d2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.InflaterInputStream;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;

/**
 * Utilities that help use D2 correctly, and in particular the setting up of streams.
 * <p/>
 * For examples of use see the unit tests (D2CompressionTest in particular).
 */
@SuppressWarnings("CyclicClassDependency")
public final class D2Utils {

  public static final String FILE_EXTENSION = ".def2";

  /**
   * Utility to construct an input stream suitable for reading a d2 file in isolation.  Note that this will return a
   * stream which includes closing bytes.  It is not suitable for combining deflated streams.
   *
   * @param in An input stream which should provide raw d2 bytes
   *
   * @return An input stream prepared to handle footers correctly
   */
  public static FooteredInputStream prepareD2Stream(InputStream in) {
    return new FooteredInputStream(in, D2Footer.FOOTER_LENGTH_ISOLATED_READ);
  }

  /**
   * Utility to provide a decompressing input stream that wraps the source, which should be a footer-less stream of
   * raw compressed bytes.
   */
  public static InputStream decompressInputSteam(InputStream in) {
    return new InflaterInputStream(in, new D2Decompressor());
  }

  /**
   * Merges the content of the incoming streams of compressed content onto the output stream which are all then closed.
   *
   * @param compressed streams of compressed content
   * @param target     to write to
   */
  public static void decompress(Iterable<InputStream> compressed, OutputStream target) throws IOException {
    Closer closer = Closer.create();
    closer.register(target);
    try (
      InputStream in = decompressInputSteam(new D2CombineInputStream(compressed))
    ) {
      ByteStreams.copy(in, target);
      target.flush(); // probably unnecessary but not guaranteed by close()
    } finally {
      closer.close();
    }
  }

  /**
   * Decompresses the content of the incoming stream of compressed content onto the output stream which are both then
   * closed.
   *
   * @param compressed stream of compressed content
   * @param target     to write to
   */
  public static void decompress(InputStream compressed, OutputStream target) throws IOException {
    Closer closer = Closer.create();
    closer.register(target);
    try (
      InputStream in = decompressInputSteam(compressed)
    ) {
      ByteStreams.copy(in, target);
      target.flush();  // probably unnecessary but not guaranteed by close()
    } finally {
      closer.close();
    }
  }

  /**
   * Compresses the incoming stream of uncompressed content onto the target stream, which are both then closed.
   *
   * @param uncompressed incoming stream of uncompressed bytes
   * @param target       target of the compression
   */
  public static void compress(InputStream uncompressed, OutputStream target) throws IOException {
    Closer closer = Closer.create();
    closer.register(target);
    try (
      D2CompressorStream compressed = new D2CompressorStream(target);
    ) {
      ByteStreams.copy(uncompressed, compressed);
      target.flush(); // probably unnecessary but not guaranteed by close()
    } finally {
      closer.close();
    }
  }

  private D2Utils() {}
}
