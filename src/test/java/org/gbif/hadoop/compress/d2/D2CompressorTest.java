/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.hadoop.compress.d2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Verifies compressor behavior.
 */
public class D2CompressorTest {

  /**
   * Test the CRC is correctly calculated.  Without a CRC, we can't merge anything.
   */
  @Test
  public void testCRC() throws IOException {
    byte[] data = "'Jam me, jack me, push me, pull me, talk hard.' (Nora Diniro, 1990)".getBytes(StandardCharsets.UTF_8);

    CRC32 rawCRC = new CRC32();
    rawCRC.update(data);

    // compress some data, and see if the CRC matches
    D2Compressor compressor = new D2Compressor();
    byte[] compressedBuffer = new byte[1024];
    compressor.setInput(data, 0, data.length); // data to compress
    compressor.compress(compressedBuffer, 0, compressedBuffer.length);

    Assert.assertEquals("CRC-32 is not as expected", rawCRC.getValue(), compressor.getCRC32());
  }
}
