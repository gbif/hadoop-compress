hadoop-compress
===============

Utilities for custom compression when using Hadoop.

Most notably, this project provides a deflate format named "d2" which allows provides a custom footer on the deflated file that allows the separate deflated files to be merged without inflating.  

This project (will!) provides the ability to build Zip files from compressed content (d2 format) stored in Hadoop without the need to decompress it.  This minimizes bandwidth, CPU and allows the ability to compress content in parallel thus making the building of Zip files when taking content out of Hadoop as efficient as possible.
