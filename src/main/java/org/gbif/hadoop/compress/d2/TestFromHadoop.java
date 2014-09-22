package org.gbif.hadoop.compress.d2;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import com.google.common.collect.Lists;

public class TestFromHadoop {
  public static void main2(String[] args) throws IOException {
    List<InputStream> streams = Lists.newArrayList();
    for (int i=0; i<10; i++) {
      streams.add(new BufferedInputStream(new FileInputStream(new File("/Users/tim/dev/git/gbif/hadoop-compress/"
                                                                       + "passerdom/occurrence_tab_def2/00000" +
                                                                       i + "_0.def2"))));
    }
    for (int i=10; i<23; i++) {
      streams.add(new BufferedInputStream(new FileInputStream(new File("/Users/tim/dev/git/gbif/hadoop-compress/"
                                                                       + "passerdom/occurrence_tab_def2/0000" +
                                                                       i + "_0.def2"))));
    }

    OutputStream out = new BufferedOutputStream(new FileOutputStream(new File("/tmp/pd.txt")));
    D2Utils.decompress(streams, out);

    out = new BufferedOutputStream(new FileOutputStream(new File("/tmp/pd-0.txt")));
    D2Utils.decompress(new BufferedInputStream(new FileInputStream(new File("/Users/tim/dev/git/gbif/hadoop-compress/"
                                                    + "passerdom/occurrence_tab_def2/000001"
                                                    + "_0.def2"))), out);
  }

  public static void main(String[] args) throws IOException {
    OutputStream out = new BufferedOutputStream(new FileOutputStream(new File("/tmp/narrow.txt")));
    D2Utils.decompress(new BufferedInputStream(new FileInputStream(new File("/Users/tim/dev/git/gbif/hadoop-compress/"
                                                                            + "narrow/narrow_tab_def2/000000_0.def2"))), out);
  }
}
