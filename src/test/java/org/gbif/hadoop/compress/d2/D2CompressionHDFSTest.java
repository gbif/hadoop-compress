package org.gbif.hadoop.compress.d2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.junit.Assert;

public class D2CompressionHDFSTest extends ClusterMapReduceTestCase {

  // control the input test file
  private static final int LINES_IN_FILE = 500; // keep below 1000 or else modify to3Chars()
  private static final int CHARS_PER_LINE = 1024;

  /**
   * Writes random chars to a file, runs it through a Map only job which compresses it, and then verifies that the
   * compressed output provides the same content as the original when read and decompressed.
   */
  public void testCompressDecompress() throws Exception {
    JobConf conf = createJobConf();

    Path inDir = new Path("test/input");
    Path outDir = new Path("test/output");

    byte[] data = generateInput();

    try (
      InputStream in = new ByteArrayInputStream(data);
      OutputStream out = getFileSystem().create(new Path(inDir, "original.txt"));
    ) {
      ByteStreams.copy(in, out);
    }

    conf.setNumMapTasks(3); // we'll have 3 to merge
    conf.setNumReduceTasks(0); // no reduction, we'll have compressed map output

    conf.setOutputFormat(ValueOnlyTextOutputFormat.class);  // ignore the keys
    TextInputFormat.setInputPaths(conf, inDir);
    ValueOnlyTextOutputFormat.setOutputPath(conf, outDir);

    // setup compression to use the Deflate2 format
    conf.setBoolean("mapred.output.compress", true);
    conf.set("mapred.output.compression.type", SequenceFile.CompressionType.BLOCK.toString());
    conf.setClass("mapred.output.compression.codec", D2Codec.class, CompressionCodec.class);

    assertTrue(JobClient.runJob(conf).isSuccessful());

    RemoteIterator<LocatedFileStatus> files = getFileSystem().listFiles(outDir, false);
    List<InputStream> parts = Lists.newArrayList();
    while (files.hasNext()) {
      LocatedFileStatus fs = files.next();
      Path path = fs.getPath();
      // Ignore the SUCCESS file which MR writes
      if (!path.toString().contains("SUCCESS")) {
        parts.add(getFileSystem().open(path));
      }
    }

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    D2Utils.decompress(parts, out);
    Assert.assertTrue("Uncompressed content does not equal the original", Arrays.equals(data, out.toByteArray()));
  }

  /**
   * Generates random input with a sequential start so that ordering is consistent.  The start will be
   * 001,002,003 etc
   */
  private byte[] generateInput() throws UnsupportedEncodingException {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < LINES_IN_FILE; i++) {
      // we write a line number so we get consistent sorting
      sb.append(to3Char(i));
      sb.append("\t");
      sb.append(RandomStringUtils.randomAlphabetic(CHARS_PER_LINE));
      sb.append("\n");
    }
    return sb.toString().getBytes();
  }

  private String to3Char(int i) {
    if (i<10) {
      return "00" + i;
    } else if (i <100) {
      return "0" + i;
    }
    return String.valueOf(i);
  }

  @Override
  protected void setUp() throws Exception {
    // required or the cluster will not start
    System.setProperty("hadoop.log.dir", "target/logs");
    startCluster(true, null);
  }

}
