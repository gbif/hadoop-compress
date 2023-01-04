package org.gbif.hadoop.compress.d2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * A stream that allows multiple raw D2 input streams to be merged, allowing access to the combined CRC-32 and the
 * lengths without actually inflated the compressed data.  This allows (e.g.) the ability to construct a Zip file
 * of pieces without decompressing individual streams.
 * <p/>
 * This may be wrapped by an InflaterInputStream, with the inflater constructed in no wrap mode to decompress and read
 * the combined stream.
 */
public class D2CombineInputStream extends InputStream {

  private final SequenceInputStream combinedStream;
  private final List<FooteredInputStream> deflatedStreams;

  private Long crc32;
  private Long compressedLength;
  private Long uncompressedLength;

  /**
   * Builds a combining stream from the input streams which must provide a raw byte stream which includes the D2Footer.
   * @param streams to raw D2 byte streams, such as file streams to .def2 files
   */
  public D2CombineInputStream(Iterable<InputStream> streams) {
    List<FooteredInputStream> raw = Lists.newArrayList();
    for (InputStream in : streams) {
      // strip the complete footer (important!)
      raw.add(new FooteredInputStream(in, D2Footer.FOOTER_LENGTH));
    }
    List<InputStream> combined = Lists.newArrayList(raw);
    // add a new stream which simply provides a closing byte sequence
    combined.add(new ByteArrayInputStream(D2Footer.FOOTER_CLOSE_DEFLATE));

    combinedStream = new SequenceInputStream(Collections.enumeration(combined));
    deflatedStreams = raw;
  }

  @Override
  public int read() throws IOException {
    return combinedStream.read();
  }

  @Override
  public int available() throws IOException {
    return combinedStream.available();
  }

  @Override
  public long skip(long n) throws IOException {
    return combinedStream.skip(n);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return combinedStream.read(b, off, len);
  }

  @Override
  public void close() throws IOException {
    combinedStream.close();

    Long localCrc32 = null;
    long localCompressedLength = 0;
    long localUncompressedLength = 0;
    for (FooteredInputStream stream : deflatedStreams) {
      try {
        D2Footer footer = D2Footer.buildFooter(stream.getFooter());

        // set or combine the CRC-32
        localCrc32 =
          localCrc32 == null ? footer.getCrc() : CRCCombine.combine(localCrc32, footer.getCrc(), footer.getUncompressedLength());
        localCompressedLength += footer.getCompressedLength();
        localUncompressedLength += footer.getUncompressedLength();

      } catch (IllegalStateException ignored) {
        // happens when the stream was not fully read
        return; // does not set the values
      }
    }

    // The final stream reported a compressed length without the closing bytes, which is correct for isolated deflation
    // but here we actually returned the closing bytes, so we adjust accordingly.
    localCompressedLength += D2Footer.FOOTER_CLOSE_DEFLATE.length;

    // values only if we managed to set them all
    crc32 = localCrc32;
    uncompressedLength = localUncompressedLength;
    compressedLength = localCompressedLength;
  }

  public Long getCrc32() {
    Preconditions.checkState(crc32 != null, "Can only retrieve CRC-32 if all streams were read to completion");
    return crc32;
  }

  public Long getCompressedLength() {
    Preconditions.checkState(compressedLength != null, "Can only retrieve compressed length if all streams were read to completion");
    return compressedLength;
  }

  public Long getUncompressedLength() {
    Preconditions.checkState(uncompressedLength != null, "Can only retrieve uncompressed length if all streams were read to completion");
    return uncompressedLength;
  }
}
