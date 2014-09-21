package org.gbif.hadoop.compress.d2;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

/**
 * A stream that allows multiple raw D2 input streams to be merged, allowing access to the combined CRC-32 and the
 * lengths without actually inflated the compressed data.  This allows (e.g.) the ability to construct a Zip file
 * of pieces without decompressing individual streams.
 * <p/>
 * This may be wrapped by an InflaterInputStream, with the inflater constructed in no wrap mode to decompress and read
 * the combined stream.
 */
@SuppressWarnings("CyclicClassDependency")
public class D2CombineInputStream extends SequenceInputStream {

  private final List<? extends FooteredInputStream> streams;
  private Long crc32;
  private Long compressedLength;
  private Long uncompressedLength;

  public static D2CombineInputStream build(Iterable<InputStream> incoming) {
    return new D2CombineInputStream(D2Utils.prepareD2Streams(incoming));
  }

  public static D2CombineInputStream buildFromFiles(Iterable<File> files) throws FileNotFoundException {
    List<InputStream> streams = Lists.newArrayList();
    for (File f : files) {
      streams.add(new FileInputStream(f));
    }
    return new D2CombineInputStream(D2Utils.prepareD2Streams(streams));
  }

  private D2CombineInputStream(List<? extends FooteredInputStream> e) {
    super(Collections.enumeration(e));
    streams = e;
  }

  @Override
  public void close() throws IOException {
    super.close();

    Long localCrc32 = null;
    long localCompressedLength = 0;
    long localUncompressedLength = 0;
    for (FooteredInputStream stream : streams) {
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

    // set values only if we managed to set them all
    crc32 = localCrc32;
    uncompressedLength = localUncompressedLength;
    compressedLength = localCompressedLength;
  }

  public Long getCrc32() {
    if (crc32 == null) {
      throw new IllegalStateException("Can only retrieve CRC-32 if all streams were read to completion");
    }
    return crc32;
  }

  public Long getCompressedLength() {
    if (compressedLength == null) {
      throw new IllegalStateException("Can only retrieve compressed length if all streams were read to completion");
    }
    return compressedLength;
  }

  public Long getUncompressedLength() {
    if (uncompressedLength == null) {
      throw new IllegalStateException("Can only retrieve uncompressed length if all streams were read to completion");
    }
    return uncompressedLength;
  }
}
