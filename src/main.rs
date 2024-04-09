use std::{
    collections::BTreeMap,
    fs::File,
    io::{ErrorKind, Read},
};

use ahash::RandomState;
use bstr::ByteSlice;
use hashbrown::HashMap;

const READ_BUF_SIZE: usize = 128 * 1024; // 128 KiB
const VALUE_SEPARATOR: u8 = b';';
const NEW_LINE: u8 = b'\n';
const CHANNEL_CAPACITY: usize = 1_000;

#[derive(Debug, Clone, Copy)]
struct Metrics {
    count: u64,
    sum: f32,
    min: f32,
    max: f32,
}

impl Metrics {
    fn new(first_value: f32) -> Metrics {
        Metrics {
            count: 1,
            sum: first_value,
            min: first_value,
            max: first_value,
        }
    }

    fn update(&mut self, next_value: f32) {
        self.count += 1;
        self.sum += next_value;
        self.min = self.min.min(next_value);
        self.max = self.max.max(next_value);
    }
}

fn mean(sum: f32, count: u64) -> f32 {
    sum / count as f32
}

fn main() {
    let (sender, receiver) = crossbeam_channel::bounded::<Box<[u8]>>(CHANNEL_CAPACITY);

    let num_of_threads = std::thread::available_parallelism().unwrap().into();
    let mut thread_handles = Vec::with_capacity(num_of_threads);
    for _ in 0..num_of_threads {
        let receiver = receiver.clone();
        let handle = std::thread::spawn(move || {
            let mut map = HashMap::<Box<[u8]>, Metrics, RandomState>::default();

            for buf in receiver {
                let mut start_idx = 0;
                let mut end_idx = 0;

                for (idx, elem) in (*buf).iter().enumerate() {
                    match elem {
                        &VALUE_SEPARATOR => {
                            end_idx = idx;
                        },
                        &NEW_LINE => {
                            let city = &buf[start_idx..end_idx];
                            start_idx = end_idx + 1;
            
                            if (idx - end_idx) > 1 && city.len() > 0 {
                                let temperature: f32 = fast_float::parse::<f32, _>(&buf[start_idx..idx]).unwrap();
                                start_idx = idx + 1;
            
                                map.entry_ref(city)
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
            map
        });
        thread_handles.push(handle);
    }
    
    drop(receiver);

    let input_filename = std::env::args().nth(1).expect("No input filename");
    let mut input_file = File::open(input_filename).unwrap();

    let mut buf = vec![0; READ_BUF_SIZE];
    let mut bytes_not_processed = 0;
    loop {
        let bytes_read = match input_file.read(&mut buf[bytes_not_processed..]) {
            Ok(n) => n,
            Err(err) => {
                if err.kind() == ErrorKind::Interrupted {
                    continue; 
                } else {
                    panic!("I/O error: {err:?}");
                }
            }
        };
        if bytes_read == 0 {
            break; 
        }

        let valid_buf = &mut buf[..(bytes_read + bytes_not_processed)];
        let last_new_line_idx = match valid_buf.iter().rposition(|&b| b == b'\n') {
            Some(pos) => pos,
            None => {
                bytes_not_processed += bytes_read;
                assert!(bytes_not_processed <= buf.len());
                if bytes_not_processed == buf.len() {
                    panic!("Found no new line in the whole read buffer");
                }
                continue;
            }
        };
        let buf_boxed = Box::<[u8]>::from(&valid_buf[..(last_new_line_idx + 1)]);
        sender.send(buf_boxed).unwrap();

        valid_buf.copy_within((last_new_line_idx + 1).., 0);
        bytes_not_processed = valid_buf.len() - last_new_line_idx - 1;
    }

    // Handle the case when the file doesn't end with '\n'
    if bytes_not_processed != 0 {
        // Send the last batch
        let buf_boxed = Box::<[u8]>::from(&buf[..bytes_not_processed]);
        sender.send(buf_boxed).unwrap();
        bytes_not_processed = 0;
    }

    drop(sender);
    let mut ordered_map = BTreeMap::new();
    for (idx, handle) in thread_handles.into_iter().enumerate() {
        let map = handle.join().unwrap();
        if idx == 0 {
            ordered_map.extend(map);
        } else {
            for (city, stats) in map.into_iter() {
                ordered_map
                    .entry(city)
                    .and_modify(|s| {
                        s.count += stats.count;
                        s.sum += stats.sum;
                        s.min = s.min.min(stats.min);
                        s.max = s.max.max(stats.max);
                    })
                    .or_insert(stats);
            }
        }
    }

    for city in ordered_map.keys() {
        let mean = mean(ordered_map[city].sum, ordered_map[city].count);

        println!("{}:
                \t min: {}
                \t max: {} 
                \t sum: {}
                \t mean: {}", city.as_bstr(), ordered_map[city].min, ordered_map[city].max, ordered_map[city].sum, mean);
    }
}