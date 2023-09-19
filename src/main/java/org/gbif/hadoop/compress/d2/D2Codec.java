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
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DecompressorStream;

import com.google.common.base.Preconditions;

/**
 * This class creates D2 compressors and decompressors providing the hooks for this to be a registered Hadoop codec.
 */
public final class D2Codec implements Configurable, CompressionCodec {
  private Configuration conf;

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
    return new D2CompressorStream(out, createCompressor());
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor) throws IOException {
    Preconditions.checkArgument(compressor instanceof D2Compressor, "Requires a %s", D2Compressor.class);
    return new D2CompressorStream(out, (D2Compressor)compressor);
  }

  @Override
  public Class<D2Compressor> getCompressorType() {
    return D2Compressor.class;
  }

  @Override
  public D2Compressor createCompressor() {
    return new D2Compressor();
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in) throws IOException {
    return createInputStream(in, createDecompressor());
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor) throws IOException {
    Preconditions.checkArgument(decompressor instanceof D2Decompressor, "Requires a %s", D2Decompressor.class);
    // prepare the stream to strip the footer
    return new DecompressorStream(D2Utils.prepareD2Stream(in), decompressor);
  }

  @Override
  public Class<D2Decompressor> getDecompressorType() {
    return D2Decompressor.class;
  }

  @Override
  public D2Decompressor createDecompressor() {
    return new D2Decompressor();
  }

  @Override
  public String getDefaultExtension() {
    return D2Utils.FILE_EXTENSION;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
