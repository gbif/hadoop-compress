/**
 * This package contains classes to help deal with a custom deflation format D2.
 *
 * <p/>
 *
 * The D2 format (deflate v2) is effectively a standard deflate format, but done in a manner that allows the ability to
 * both compress and decompress a single file, or compressed data in parallel and then merge those compressed parts into
 * a Zip file without inflating the compressed data.
 *
 * <p/>
 *
 * In order to merge compressed streams into a Zip without inflating them the following are required:
 * <ol>
 * <li>The deflated streams have no headers or footers (no wrap mode), except for the final stream</li>
 * <li>The deflated streams are deflated using SYNC_FLUSH in order to respect byte boundaries</li>
 * <li>The total length of the uncompressed data is available</li>
 * <li>The total length of the compressed data is available</li>
 * <li>The CRC-32 of the uncompressed data is available.  This can be computed from multiple CRC-32 provided that the
 * length of uncompressed data for each CRC-32 is known</li>
 * </ol>
 *
 * <p/>
 *
 * The structure of a D2 file is [data][footer].  The footer is structured as:
 * <ol>
 * <li>2 bytes: A fixed sequence that indicates a closing of the Deflate stream</li>
 * <li>8 bytes: The length of the uncompressed data</li>
 * <li>8 bytes: The length of the compressed data</li>
 * <li>8 bytes: The CRC-32 of the uncompressed data</li>
 * </ol>
 *
 * {@link org.gbif.hadoop.compress.d2.D2Utils} provides convenience methods to read the [data] portion in isolation from
 * the footer.
 *
 * <p/>
 *
 * This package provides all the classes necessary to deflate and inflate data, and merge streams, but does not provide
 * means to create a Zip which is found in the org.gbif.hadoop.compress.zip package.

 */
package org.gbif.hadoop.compress.d2;
