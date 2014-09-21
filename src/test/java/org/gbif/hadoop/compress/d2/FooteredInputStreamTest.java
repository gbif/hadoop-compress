package org.gbif.hadoop.compress.d2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.common.io.ByteStreams;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests behaviour of the FooteredInputStream.
 */
@SuppressWarnings("MagicNumber")
public class FooteredInputStreamTest {

  // sample data to test against
  private static final byte[] DATA; // includes the footer
  private static final byte[] FOOTER;
  static {
    FOOTER = ByteBuffer.allocate(4).putInt(2).array();
    DATA = ByteBuffer.allocate(12).putInt(1).putInt(2).put(FOOTER).array();
  }

  @Test
  public void testRead() throws IOException {
    try (FooteredInputStream is = new FooteredInputStream(new ByteArrayInputStream(DATA), FOOTER.length)) {
      byte[] content = new byte[DATA.length - FOOTER.length];
      //noinspection ResultOfMethodCallIgnored
      is.read(content, 0, content.length);
      is.close();
      assertArrayEquals("Content is not equal", Arrays.copyOf(DATA, DATA.length - FOOTER.length), content);
      assertNotNull("Unable to get the FOOTER, when we have read the stream fully", is.getFooter());
      assertArrayEquals("Footer is not equal", FOOTER, is.getFooter());
    }
  }

  @Test
  public void testReadFully() throws IOException {
    try (FooteredInputStream is = new FooteredInputStream(new ByteArrayInputStream(DATA), FOOTER.length)) {
      ByteStreams.copy(is, ByteStreams.nullOutputStream());
      is.close();
      assertNotNull("Unable to get the FOOTER, when we have read the stream fully", is.getFooter());
      assertArrayEquals("Footer is not equal", FOOTER, is.getFooter());
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testReadFooter() throws IOException {
    try (FooteredInputStream is = new FooteredInputStream(new ByteArrayInputStream(DATA), FOOTER.length)) {
      is.close();
      is.getFooter(); // can't read FOOTER if you don't read the stream
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoFooter() throws IOException {
    try (InputStream is = new FooteredInputStream(new ByteArrayInputStream(DATA), 0)) {
    }
  }

  @Test
  public void testReadMultiple() throws IOException {
    try (FooteredInputStream is = new FooteredInputStream(new ByteArrayInputStream(DATA), FOOTER.length)) {
      byte[] content = new byte[DATA.length - FOOTER.length];
      int read = 0;
      int populated = 0;
      // check it reads in batches ok
      while (read != -1) {
        read = is.read(content, populated, 5); // 5 bytes at a time
        populated += read;
      }
      is.close();
      assertArrayEquals("Content is not equal", Arrays.copyOf(DATA, DATA.length - FOOTER.length), content);
      assertNotNull("Unable to get the FOOTER, when we have read the stream fully", is.getFooter());
      assertArrayEquals("Footer is not equal", FOOTER, is.getFooter());
    }
  }
}
