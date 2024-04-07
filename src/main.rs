use std::fs::File;
use std::io::{Read, ErrorKind};
use threadpool;
use std::{collections::HashMap, time::Instant};

const CHUNK_CHANNEL_BUFFER_CAP: usize = 15;
const CHUNK_SIZE: usize = 128 * 1024; //128kiB
const VALUE_SEPARATOR: u8 = b';';
const NEW_LINE: u8 = b'\n';

struct Metrics {
    min: f32,
    max: f32,
    sum: f32,
    count: u64
}

impl Metrics {
    fn new(initial: f32) -> Metrics {
        Metrics {
            min: initial,
            max: initial,
            sum: initial,
            count: 1,
        }
    }

    fn update(&mut self, next: f32) {
        self.min = self.min.min(next);
        self.max = self.max.max(next);
        self.sum += next;
        self.count += 1;    
    }

    // fn compare_and_update(&mut self, metric: &Metrics) {
    //     self.min = self.min.min(metric.min);
    //     self.max = self.max.max(metric.max);
    //     self.sum += metric.sum;
    //     self.count += metric.count;
    // }
}

fn mean(sum: f32, count: u64) -> f32 {
    sum / count as f32
}

fn main() {
    println!("Start reading the file.");
    let filename = "./measurements.txt";
    read_the_file(filename);
}

fn read_the_file(filename: &str) {
    let now = Instant::now();

    let num_of_threads: usize = std::thread::available_parallelism().unwrap().into();
    let thread_pool = threadpool::ThreadPool::new(num_of_threads);
    
    let mut file = File::open(filename).unwrap();

    let (chunk_sender, chunk_receiver) = crossbeam_channel::bounded::<Box::<[u8]>>(CHUNK_CHANNEL_BUFFER_CAP);

    let mut map = HashMap::<String, Metrics>::new();

    let mut buffer = vec![0; CHUNK_SIZE];
    let mut unprocessed_part  = 0;

    loop {
        let bytes_read = match file.read(&mut buffer[unprocessed_part..]) {
            Ok(n) => n,
            Err(err) => {
                if err.kind() == ErrorKind::Interrupted {
                    continue; // Retry
                } else {
                    panic!("I/O error: {err:?}");
                }
            }
        };

        if bytes_read == 0 {
            break; // we've reached the end of the file
        }

        let processed_buffer = &mut buffer[..(bytes_read + unprocessed_part)];

        let new_line_index = match processed_buffer.iter().rposition(|&b| b == b'\n') {
            Some(pos) => pos,
            None => {
                unprocessed_part += bytes_read;
                assert!(unprocessed_part <= buffer.len());
                if unprocessed_part == buffer.len() {
                    panic!("Found no new line in the whole read buffer");
                }
                continue; // Read again, maybe next read contains a new line
            }
        };

        let boxed_buffer = Box::<[u8]>::from(&processed_buffer[..(new_line_index + 1)]);
        let local_sender = chunk_sender.clone();

        thread_pool.execute(move || {
            local_sender.send(boxed_buffer).unwrap();
        });

        processed_buffer.copy_within((new_line_index + 1).., 0);
        unprocessed_part = processed_buffer.len() - new_line_index - 1;
    }

    // Handle the case when the file doesn't end with '\n'
    if unprocessed_part != 0 {
        // Send the last batch
        let boxed_buffer = Box::<[u8]>::from(&buffer[..unprocessed_part]);
        chunk_sender.send(boxed_buffer).unwrap();
        unprocessed_part = 0;
    } else {
        drop(chunk_sender); //drop the dangling sender since you've cloned one for each chunk
    }

    for chunk in chunk_receiver {
        let mut start_idx = 0;
        let mut end_idx = 0;
    
        for (idx, elem) in (*chunk).iter().enumerate() {
            match elem {
                &VALUE_SEPARATOR => {
                    end_idx = idx;
                },
                &NEW_LINE => {
                    let city = std::str::from_utf8(&chunk[start_idx..end_idx]).unwrap().to_string();
                    start_idx = end_idx + 1;

                    if (idx - end_idx) > 1 && city.len() > 0 {
                        let temperature: f32 = fast_float::parse::<f32, _>(&chunk[start_idx..idx]).unwrap();
                        start_idx = idx + 1;

                        map.entry(city)
                            .and_modify(|metric| metric.update(temperature))
                            .or_insert_with(|| Metrics::new(temperature));
                    }
                },
                _ => {
                    continue;
                },
            };
        }
    }

    for city in map.keys() {
        let mean = mean(map[city].sum, map[city].count);

        println!("{}:
                \n\t min: {}
                \n\t max: {} 
                \n\t sum: {}
                \n\t mean: {}", city, map[city].min, map[city].max, map[city].sum, mean);
    }

    let elapsed = now.elapsed();
    println!("Finished printing the metrics in: {:.2?}", elapsed);
}