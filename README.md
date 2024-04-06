A Rust based attempt at the One Billion Rows challenge. Teaching myself a little Rust along the way.

System Spec: Macbook Pro M3 with 8 GB RAM. The measurements file is 13.2 GB.

First naive implementation: Try going line by line and compute the metrics using a buffered reader. Time to complete: 694.01 secs

Second attempt: Remove the new string allocation every time you read a line with the buffered reader. Time to complete: 538.05 secs

Third attempt: Use threads to parallelize the work. Time to complete: 502.37 secs

Adding threads has not done much to improve the speed.
This is because the inefficiencies are in how we're reading the file and large number of string allocations and cloning etc.
Next attempt will be to tackle this.

Made two major changes.

1. Introduced a threadpool
2. Read in fixed size chunks of 256 kiB

Reading in fixed size chunks required some additional code to handle the end of line and separators.

I also learned to add the --release option while running the code. Feeling more than a little stupid. Should go back and rerun the earlier attempts also.

Fourth attempt: Using a threadpool and fixed size chunks with the release mode. Time to complete: 124.63 secs

Still working with the mpsc channel. Will crossbeam make a difference? Let's see.