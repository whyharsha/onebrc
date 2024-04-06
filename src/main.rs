use std::{
    collections::HashMap, fs::File, io::{ErrorKind, Read}, path::Path, sync::mpsc, time::Instant};
use bstr::ByteSlice;
use threadpool::ThreadPool;

const READ_BUFFER_SIZE: usize = 256 * 1024; // 256 KiB
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

    fn compare_and_update(&mut self, metric: &Metrics) {
        self.min = self.min.min(metric.min);
        self.max = self.max.max(metric.max);
        self.sum += metric.sum;
        self.count += metric.count;
    }
}

fn main() {
    //std::env::set_var("RUST_BACKTRACE", "full");
    read_the_file("./measurements.txt");
}

fn mean(sum: f32, count: u64) -> f32 {
    sum / count as f32
}

fn read_the_file<P>(filename: P) where P: AsRef<Path> {
    println!("Start reading the file");
    let now = Instant::now();

    let (sender, receiver) = std::sync::mpsc::channel::<Box<[u8]>>();

    let mut file = File::open(filename).unwrap();

    let mut buffer = vec![0; READ_BUFFER_SIZE];
    let mut unprocessed_part = 0;

    let num_of_threads: usize = std::thread::available_parallelism().unwrap().into();
    let thread_pool = ThreadPool::new(num_of_threads);

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
        let local_sender = sender.clone();

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
        sender.send(boxed_buffer).unwrap();
        unprocessed_part = 0;
    } else {
        drop(sender); //drop the dangling sender since you've cloned one for each chunk
    }

    let (final_sender, final_receiver) = mpsc::channel::<HashMap<Box<[u8]>, Metrics>>();

    for received in receiver {
        let mut map = HashMap::<Box<[u8]>, Metrics>::default();
        
        for raw_line in received.lines_with_terminator() {
            let line = trim_new_line(raw_line);
            let (city, temp) = split_once_byte(line, VALUE_SEPARATOR).expect("Separator not found");

            let city = Box::<[u8]>::from(city);
            let temperature = fast_float::parse::<f32, _>(temp).unwrap();

            map.entry(city)
                .and_modify(|metric| metric.update(temperature))
                .or_insert_with(|| Metrics::new(temperature));
        }

        let local_final_sender = final_sender.clone();

        thread_pool.execute(move || {
            local_final_sender.send(map).unwrap();
        });
    }

    drop(final_sender);

    let mut final_map = HashMap::<Box<[u8]>, Metrics>::default();

    for final_received in final_receiver {
        for (city, temp_metric) in final_received {
            final_map.entry(city)
                .and_modify(|metric| metric.compare_and_update(&temp_metric))
                .or_insert(temp_metric);
        }
    }

    for city in final_map.keys() {
        let mean = mean(final_map[city].sum, final_map[city].count);

        println!("{}:
                \n\t min: {}
                \n\t max: {} 
                \n\t sum: {}
                \n\t mean: {}", city.as_bstr(), final_map[city].min, final_map[city].max, final_map[city].sum, mean);
    }

    let elapsed = now.elapsed();
    println!("Finished printing the metrics in: {:.2?}", elapsed);

}

//Borrowed these parts of the code as I read up on better file reading options online
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

//Borrowed these parts of the code as I read up on better file reading options online
fn split_once_byte(haystack: &[u8], needle: u8) -> Option<(&[u8], &[u8])> {
    let Some(pos) = haystack.iter().position(|&b| b == needle) else {
        return None;
    };

    Some((&haystack[..pos], &haystack[pos + 1..]))
}