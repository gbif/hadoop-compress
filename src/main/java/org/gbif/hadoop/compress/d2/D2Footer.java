package org.gbif.hadoop.compress.d2;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * Defines the structure and serialization of the custom D2 fixed length footer.
 * <p/>
 * The format of the footer is:
 * <ol>
 * <li>2 bytes: A fixed sequence that indicates a closing of the Deflate stream</li>
 * <li>8 bytes: The length of the uncompressed data</li>
 * <li>8 bytes: The length of the compressed data</li>
 * <li>8 bytes: The CRC-32 of the uncompressed data</li>
 * </ol>
 */
public class D2Footer {

  // A fixed sequence that informs the deflate stream is closing
  // See https://www.ietf.org/rfc/rfc1951.txt for some light reading...
  static final byte[] FOOTER_CLOSE_DEFLATE = {(byte) 3, (byte) 0};
  static final long FOOTER_CLOSE_CRC;
  static {
    CRC32 crc = new CRC32();
    crc.update(FOOTER_CLOSE_DEFLATE);
    FOOTER_CLOSE_CRC = crc.getValue();
  }

  // The total length of the footer
  public static final int FOOTER_LENGTH = 26;

  // The expected length of the footer if deflated streams are read in isolation (they will need the close marker)
  public static final int FOOTER_LENGTH_ISOLATED_READ = FOOTER_LENGTH - FOOTER_CLOSE_DEFLATE.length;
  private final long uncompressedLength;
  private final long compressedLength;
  private final long crc;

  /**
   * Utility to serialize as a byte array.
   */
  public static byte[] serialize(long uncompressed, long compressed, long crc32) {
    ByteBuffer footer = ByteBuffer.allocate(FOOTER_LENGTH);
    footer.put(FOOTER_CLOSE_DEFLATE);           // 2 bytes: which means the deflate stream can be read in isolation
    footer.putLong(uncompressed);               // 8 bytes: uncompressed length
    footer.putLong(compressed);                 // 8 bytes: compressed length
    footer.putLong(crc32);                      // 8 bytes: CRC 32
    if (footer.hasArray()) {
      return footer.array();
    } else {
      // for safety, but should never happen
      footer.reset();
      byte[] data = new byte[FOOTER_LENGTH];
      footer.get(data);
      return data;
    }
  }

  public static D2Footer buildFooter(byte[] footer) {
    ByteBuffer bb = ByteBuffer.wrap(footer);
    if (footer.length == FOOTER_LENGTH) {
      bb.get(new byte[FOOTER_CLOSE_DEFLATE.length]); // skip header if present
    }
    return new D2Footer(bb.getLong(), bb.getLong(), bb.getLong());
  }

  private D2Footer(long uncompressedLength, long compressedLength, long crc) {
    this.uncompressedLength = uncompressedLength;
    this.compressedLength = compressedLength;
    this.crc = crc;
  }

  public long getUncompressedLength() {
    return uncompressedLength;
  }

  public long getCompressedLength() {
    return compressedLength;
  }

  public long getCrc() {
    return crc;
  }

}
