package org.gbif.hadoop.compress.d2;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.security.SecureRandom;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests that demonstrate compression and decompression round tripping.
 */
@SuppressWarnings({"MagicNumber", "SameParameterValue"})
public class D2CompressionTest {

  private static final SecureRandom RANDOM = new SecureRandom();
  private static final int NUMBER_PARTS = 3;
  private static final int PART_SIZE_IN_BYTES = 1024 << 10; // 1MB

  /**
   * Concatenates the contents of the parts into the target File.
   */
  private static void merge(Iterable<File> parts, File target) throws IOException {
    try (
      OutputStream os = new BufferedOutputStream(java.nio.file.Files.newOutputStream(target.toPath()))
    ) {
      for (File f : parts) {
        try (InputStream is = java.nio.file.Files.newInputStream(f.toPath())) {
          D2Utils.copy(is, os);
        }
      }
    }
  }

  /**
   * Note: has potential to blow memory if numBytes is huge.
   */
  private static File randomFileOfSize(int sizeInBytes) throws IOException {
    File tempFile = File.createTempFile("part-", ".txt");
    // fill a byte array with random bytes
    byte[] data = new byte[sizeInBytes];
    RANDOM.nextBytes(data);
    try (OutputStream os = new BufferedOutputStream(java.nio.file.Files.newOutputStream(tempFile.toPath()))) {
      os.write(data);
      return tempFile;
    }
  }

  /**
   * Returns a list of InputStreams to the files.
   */
  private static Iterable<InputStream> asInputStreams(Iterable<File> files) throws IOException {
    List<InputStream> streams = new ArrayList<>();
    for (File f : files) {
      streams.add(java.nio.file.Files.newInputStream(f.toPath()));
    }
    return streams;
  }

  /**
   * An end to end test that writes some random files and ensures that when deflated separately, merged and inflated
   * they represent the same byte sequence as a concatenation of the original files.
   */
  @Test
  public void testParallelCompress() throws IOException {

    // generate the files of random uncompressed content
    List<File> parts = Lists.newArrayList();
    for (int i = 0; i < NUMBER_PARTS; i++) {
      parts.add(randomFileOfSize(PART_SIZE_IN_BYTES));
    }

    // concatenate uncompressed files
    File original = File.createTempFile("original-", ".txt");
    merge(parts, original);

    // compress each separately
    List<File> deflated = Lists.newArrayList();
    for (File f : parts) {
      File compressedPart = File.createTempFile("comp-", D2Utils.FILE_EXTENSION);
      D2Utils.compress(java.nio.file.Files.newInputStream(f.toPath()), java.nio.file.Files.newOutputStream(compressedPart.toPath()));
      deflated.add(compressedPart);
    }

    // merge and decompress the compressed parts
    File decompressed = File.createTempFile("decomp-", ".txt");
    D2Utils.decompress(asInputStreams(deflated), java.nio.file.Files.newOutputStream(decompressed.toPath()));

    Assert.assertTrue("Content of files should be identical", Files.equal(original, decompressed));
  }

  /**
   * A test that writes some random bytes to a files and ensures that when compressed and decompressed the content is
   * the same.
   */
  @Test
  public void testCompress() throws IOException {
    File original = randomFileOfSize(1024 * 1024 * 10); // 10MB
    File compressed = File.createTempFile("comp-", D2Utils.FILE_EXTENSION);
    D2Utils.compress(java.nio.file.Files.newInputStream(original.toPath()),
                     java.nio.file.Files.newOutputStream(compressed.toPath()));

    File decompressed = File.createTempFile("decomp-", ".txt");
    D2Utils.decompress(java.nio.file.Files.newInputStream(compressed.toPath()),
                       java.nio.file.Files.newOutputStream(decompressed.toPath()));

    Assert.assertTrue("Content of files should be identical", Files.equal(original, decompressed));
  }
}
