use std::{
    collections::HashMap,
    fs::File, io::{BufRead, BufReader}, path::Path, time::Instant};

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

//Read the file, calculate and print the metrics
fn read_the_file<P>(filename: P) where P: AsRef<Path> {
    
    println!("Start reading the file");
    let now = Instant::now();

    let (sender, receiver) = std::sync::mpsc::channel::<HashMap<String, Metrics>>();

    let file = File::open(filename).unwrap();
    let mut reader = BufReader::new(file);

    let n_threads: usize = std::thread::available_parallelism().unwrap().into();
    println!("Number of available threads is: {}", n_threads);

    let mut line_count: i32 = 0;
    let mut lines = Vec::<String>::new();

    //Let's use 8 threads and read the file in 8 chunks of 125 mn lines each, see if we can process one chunk before the other is done
    loop {
        let mut line = String::new(); // losing the benefit of reusing the string due to the introduction of threads
        let result = reader.read_line(& mut line);

        match result {
            Ok(value) => {
                if value == 0 {
                    break;
                }

                if line.ends_with("\n") || line.ends_with("\r") {
                    line.pop();
                }

                lines.push(line);

                
            }
            Err(error) => {
                println!("Error: {}", error)
            },
        }

        line_count += 1;

        if line_count%125000000 == 0 {
            println!("We have now read {} lines", line_count);
            let elapsed = now.elapsed();
            println!("Finished reading in: {:.2?}", elapsed);

            let local_sender = sender.clone();
            let completed_lines = lines.clone(); //huge costs I'm paying by cloning, not sure what the trade off is

            std::thread::spawn(move || {
                let mut map = HashMap::<String, Metrics>::new();

                for line in completed_lines {
                    let (city, temp) = line.split_once(';').unwrap();

                    let temp_result = temp.parse::<f32>();
                    let mut temperature = 0.0;

                    match temp_result {
                        Ok(value) => {
                            temperature = value;
                        },
                        Err(error) => {
                            println!("Error: {}", error)
                        }
                    }

                    map.entry(city.to_string())
                        .and_modify(|metric| metric.update(temperature))
                        .or_insert_with(|| Metrics::new(temperature));
                }

                local_sender.send(map).unwrap();
                println!("Sent a chunk of data for processing");
                let elapsed = now.elapsed();
                println!("Finished processing in: {:.2?}", elapsed);
            });

            lines.clear();
        }
    }

    drop(sender); //it's cloned for all senders, so need to drop it.

    let mut final_map = HashMap::<String, Metrics>::new();

    for received in receiver {
        for (city, temp_metric) in received {
            final_map.entry(city)
                .and_modify(|metric| metric.compare_and_update(&temp_metric))
                .or_insert(temp_metric);
        }
        println!("Just checking if this is being processed");
    }

    println!("Just checking if final map is ready for printing");

    for city in final_map.keys() {
        let mean = mean(final_map[city].sum, final_map[city].count);

        println!("{}:
                \n\t min: {}
                \n\t max: {} 
                \n\t sum: {}
                \n\t mean: {}", city, final_map[city].min, final_map[city].max, final_map[city].sum, mean);
    }

    let elapsed = now.elapsed();
    println!("Finished printing the metrics in: {:.2?}", elapsed);
}