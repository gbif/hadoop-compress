package org.gbif.hadoop.compress.d2;

/**
 * A utility that enables CRC-32 values to be combined from separate parts of the original.
 * <p/>
 * A port of the zlib.1.2 implementation.  All credit to authors Jean-loup Gailly(jloup@gzip.org) and Mark
 * Adler(madler@alumni.caltech.edu) and contributors of zlib.
 */
@SuppressWarnings("MagicNumber")
final class CRCCombine {

  private static final int GF2_DIM = 32;

  static long combine(long crc1, long crc2, long len2) {

    // degenerate case (also disallow negative lengths)
    if (len2 <= 0) {
      return crc1;
    }

    // put operator for one zero bit in odd
    long[] odd = new long[GF2_DIM];
    odd[0] = 0xedb88320L;          // CRC-32 polynomial
    long row = 1;
    for (int n = 1; n < GF2_DIM; n++) {
      odd[n] = row;
      row <<= 1;
    }

    // put operator for two zero bits in even
    long[] even = new long[GF2_DIM];
    gf2_matrix_square(even, odd);

    // put operator for four zero bits in odd
    gf2_matrix_square(odd, even);

    // apply len2 zeros to crc1 (first square will put the operator for one
    // zero byte, eight zero bits, in even)
    do {
      // apply zeros operator for this bit of len2
      gf2_matrix_square(even, odd);
      if ((len2 & 1) != 0) {
        crc1 = gf2_matrix_times(even, crc1);
      }
      len2 >>= 1;

      // if no more bits set, then done
      if (len2 == 0) {
        break;
      }

      // another iteration of the loop with odd and even swapped
      gf2_matrix_square(odd, even);
      if ((len2 & 1) != 0) {
        crc1 = gf2_matrix_times(odd, crc1);
      }
      len2 >>= 1;

      // if no more bits set, then done
    } while (len2 != 0);

    /* return combined crc */
    crc1 ^= crc2;
    return crc1;
  }

  private static void gf2_matrix_square(long[] square, long... mat) {
    for (int n = 0; n < GF2_DIM; n++) {
      square[n] = gf2_matrix_times(mat, mat[n]);
    }
  }

  private static long gf2_matrix_times(long[] mat, long vec) {
    long sum = 0;
    int index = 0;
    while (vec != 0) {
      if ((vec & 1) != 0) {
        sum ^= mat[index];
      }
      vec >>= 1;
      index++;
    }
    return sum;
  }

  private CRCCombine() {}
}
