A Rust based attempt at the One Billion Rows challenge. Teaching myself a little Rust along the way.

System Spec: Macbook Pro M3 with 8 GB RAM. The measurements file is 13.2 GB.

First naive implementation: Try going line by line and compute the metrics using a buffered reader. Time to complete: 694.01 secs

Second attempt: Remove the new string allocation every time you read a line with the buffered reader. Time to complete: 538.05 secs

Third attempt: Use threads to parallelize the work. Time to complete: 502.37 secs

Adding threads has not done much to improve the speed.
This is because the inefficiencies are in how we're reading the file and large number of string allocations and cloning etc.
Next attempt will be to tackle this.
