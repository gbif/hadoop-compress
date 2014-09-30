package org.gbif.hadoop.compress.d2.zip;

import org.gbif.hadoop.compress.d2.D2CombineInputStream;
import org.gbif.hadoop.compress.d2.D2Utils;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.zip.ZipInputStream;

import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Test;

import static org.gbif.hadoop.compress.d2.zip.ModalZipOutputStream.MODE;

/**
 * Shows how to write and read a zip.
 */
public class ModalZipTest {

  /**
   * Test that we can write compressed content to a Zip and read it back.
   */
  @Test
  public void testPredeflated() throws IOException {
    byte[] original = "Ghosts crowd the young child's fragile eggshell mind".getBytes();
    byte[] compressed = compress(original);
    byte[] zipFile = createZip(original, compressed);

    try (
      ZipInputStream zin = new ZipInputStream(new ByteArrayInputStream(zipFile));
    ) {
      // read the entry that was pre-compressed
      zin.getNextEntry();
      byte[] read = new byte[original.length];
      ByteStreams.read(zin, read, 0, read.length);
      Assert.assertTrue("Uncompressed does not equal the original", Arrays.equals(read, original));

      // read the entry that was compressed as it was added to the zip
      zin.getNextEntry();
      read = new byte[original.length];
      ByteStreams.read(zin, read, 0, read.length);
      Assert.assertTrue("Uncompressed does not equal the original", Arrays.equals(read, original));
    }
  }

  /**
   * Illustrates how to add both pre-compressed and uncompressed content to a Zip.
   */
  private static byte[] createZip(byte[] original, byte[] compressed) throws IOException {
    try (
      ByteArrayOutputStream zipped = new ByteArrayOutputStream();
      ModalZipOutputStream zos = new ModalZipOutputStream(new BufferedOutputStream(zipped));
    ) {

      // add a pre-compressed entry
      ZipEntry ze = new ZipEntry("pre-deflated.txt");
      zos.putNextEntry(ze, MODE.PRE_DEFLATED);
      try (D2CombineInputStream in = new D2CombineInputStream(Lists.<InputStream>newArrayList(new ByteArrayInputStream(
        compressed)))) {
        ByteStreams.copy(in, zos);
        in.close(); // required to get the sizes
        ze.setSize(in.getUncompressedLength()); // important to set the sizes and CRC
        ze.setCompressedSize(in.getCompressedLength());
        ze.setCrc(in.getCrc32());
      }
      zos.closeEntry();

      // add an entry to compress
      ze = new ZipEntry("original.txt");
      zos.putNextEntry(ze, MODE.DEFAULT);
      try (InputStream in = new ByteArrayInputStream(original)) {
        ByteStreams.copy(in, zos);
      }
      zos.closeEntry();
      zos.close();
      return zipped.toByteArray();
    }
  }

  private static byte[] compress(byte[] original) throws IOException {
    try (
      InputStream in = new ByteArrayInputStream(original);
      ByteArrayOutputStream out = new ByteArrayOutputStream()
    ) {
      D2Utils.compress(in, out);
      return out.toByteArray();
    }
  }
}
