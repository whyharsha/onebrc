A Rust based attempt at the One Billion Rows challenge. Teaching myself a little Rust along the way.

System Spec: Macbook Pro M3 with 8 GB RAM. The measurements file is 13.2 GB.
My measurements are very unscientific. I'm just looking to improve my understanding and learn some Rust along the way.
But they offer some directional validation of the improvements.

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

First it turns out using the fast_float to parse is good.

Fifth attempt: Time to complete: 111.85 secs

Changed a string to a u8 slice in the Hashmap to reduce some conversions. Also deleted the print statements.

Sixth attempt: Time to complete: 106.96 secs

Seventh attempt: Fixed some parsing issues. Time to complete: 53.23 secs