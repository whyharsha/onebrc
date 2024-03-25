A Rust based attempt at the One Billion Rows challenge. Teaching myself a little Rust along the way.

System Spec: Macbook Pro M3 with 8 GB RAM.
The measurements file is 13.2 GB.

First naive implementation:
Try going line by line and compute the metrics using a buffered reader.
Time to complete: 694.01 secs

Second attempt:
Remove the new string allocation every time you read a line with the buffered reader.
Time to complete: 538.05 secs
