use std::{
    collections::HashMap,
    fs::File, io::{self, BufRead}, path::Path, time::Instant};

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

    let file = File::open(filename).unwrap();
    let lines = io::BufReader::new(file).lines();
    let mut map = HashMap::<String, Metrics>::new();
    
    for line in lines.flatten() {
        let (city, temp) = line.split_once(';').unwrap();
        let temperature: f32 = temp.parse().unwrap();

        map.entry(city.to_string())
        .and_modify(|metric| metric.update(temperature))
        .or_insert_with(|| Metrics::new(temperature));
    }

    let mut elapsed = now.elapsed();
    println!("Finished reading the file in: {:.2?}", elapsed);

    for city in map.keys() {
        let mean = mean(map[city].sum, map[city].count);

        println!("{}:
                \n\t min: {}
                \n\t max: {} 
                \n\t sum: {}
                \n\t mean: {}", city, map[city].min, map[city].max, map[city].sum, mean);
    }

    elapsed = now.elapsed();
    println!("Finished printing the metrics in: {:.2?}", elapsed);
}