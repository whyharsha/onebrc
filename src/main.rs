use std::fs::File;
use std::io::{Read, ErrorKind};
use threadpool;
use hashbrown::HashMap;
use bstr::ByteSlice;

const CHUNK_CHANNEL_BUFFER_CAP: usize = 1000;
const CHUNK_SIZE: usize = 128 * 1024; //128kiB
const VALUE_SEPARATOR: u8 = b';';

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
    let num_of_threads: usize = std::thread::available_parallelism().unwrap().into();
    let thread_pool = threadpool::ThreadPool::new(num_of_threads);

    let (chunk_sender, chunk_receiver) = crossbeam_channel::bounded::<Box::<[u8]>>(CHUNK_CHANNEL_BUFFER_CAP);

    let mut map = HashMap::<Box<[u8]>, Metrics>::new();

    let mut buffer = vec![0; CHUNK_SIZE];
    let mut unprocessed_part  = 0;

    let mut file = File::open(filename).unwrap();

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

    for buf in chunk_receiver {
        for raw_line in buf.lines_with_terminator() {
            let line = trim_new_line(raw_line);
            let (city, temp) =
                split_once_byte(line, VALUE_SEPARATOR).expect("Separator not found");

            let temperature = fast_float::parse::<f32, _>(temp).unwrap();
            map.entry_ref(city)
                .and_modify(|metrics| metrics.update(temperature))
                .or_insert_with(|| Metrics::new(temperature));
        }
    }

    let mut ordered_map = std::collections::BTreeMap::new();
    ordered_map.extend(map);

    for city in ordered_map.keys() {
        let mean = mean(ordered_map[city].sum, ordered_map[city].count);

        println!("{}:
                \n\t min: {}
                \n\t max: {} 
                \n\t sum: {}
                \n\t mean: {}", city.as_bstr(), ordered_map[city].min, ordered_map[city].max, ordered_map[city].sum, mean);
    }
}

fn split_once_byte(haystack: &[u8], needle: u8) -> Option<(&[u8], &[u8])> {
    let Some(pos) = haystack.iter().position(|&b| b == needle) else {
        return None;
    };

    Some((&haystack[..pos], &haystack[pos + 1..]))
}

fn trim_new_line(s: &[u8]) -> &[u8] {
    let mut trimmed = s;
    if trimmed.last_byte() == Some(b'\n') {
        trimmed = &trimmed[..trimmed.len() - 1];
        if trimmed.last_byte() == Some(b'\r') {
            trimmed = &trimmed[..trimmed.len() - 1];
        }
    }
    trimmed
}