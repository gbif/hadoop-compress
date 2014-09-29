hadoop-compress
===============

This project provides the ability to compressed content in parallel using Hadoop and then merge the compressed content into a Zip file without decompressing.

Specifically, this project provides a deflate format ```version 2``` which adds a custom fixed-length footer to a deflated file that provides the lengths and CRC-32 checksum needed to allow merging without inflating.  

This project provides the ability to build Zip files from compressed content ```def2 format``` without the need to decompress it.  

When used in Hadoop, this minimizes bandwidth, CPU and allows the ability to compress content in parallel thus making the building of Zip files as efficient as possible.

Contributions and improvements to this project are very welcome.
