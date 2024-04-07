A Rust based attempt at the One Billion Rows challenge. Teaching myself a little Rust along the way.

System Spec: Macbook Pro M3 with 8 GB RAM. The measurements file is 13.2 GB.
My measurements are very unscientific. I'm just looking to improve my understanding and learn some Rust along the way.
But they offer some directional validation of the improvements.

First naive implementation: Try going line by line and compute the metrics using a buffered reader. Time to complete: 73.81 secs

Second attempt: Remove the new string alloc on every read line. Time to complete: 60.73 secs

Fourth attempt: Fixed some parsing issues. Time to complete: 53.23 secs

Fifth attempt: Cleaned up another unnecessary parse. Time to complete: 25.67 secs