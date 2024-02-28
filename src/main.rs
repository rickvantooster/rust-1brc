use std::{fs::File, io::{BufReader, BufRead, Read}, collections::HashMap, sync::mpsc::{SyncSender, channel, sync_channel}, thread::{Thread, self, JoinHandle}, u8, usize, f64};

use indicatif::{ProgressBar, ProgressStyle};

enum MessageType {
    Line(String),
    Exit
}

struct StationData {
    min: f64,
    max: f64,
    sum: f64,
    count: usize
}

fn process_naive_less_alloc(file: File) {
    let mut reader = BufReader::with_capacity(1024*1024*16, file);
    let bar = ProgressBar::new(10_u64.pow(9));
    bar.set_style(ProgressStyle::with_template("[{elapsed} {eta_precise} {percent}{bar:40.cyan/blue}]").unwrap().progress_chars("##-"));
    let mut resultset: HashMap<String, StationData> = HashMap::new();
    let mut line = String::new();

    while reader.read_line(&mut line).unwrap() != 0 {
        line.pop();
        let split = line.split(';').collect::<Vec<_>>();
        
        let station = split.get(0).unwrap();
        //println!("value as hex: {:?}", split.get(1).unwrap().as_bytes());
        let value: f64 = split.get(1).unwrap().parse().unwrap();

        if let Some(data) = resultset.get_mut(&station.to_string()) {
            if data.min > value {
                data.min = value;
            }
            if data.max < value {
                data.max = value;
            }

            data.sum += value;
            data.count += 1;

        }else{
            resultset.insert(station.to_string().clone(), StationData { min: value, max: value, sum: value, count: 1 });

        }
        bar.inc(1);
        line.clear();


    }

    print!("{{");
    for (name, data) in resultset {
        let min = data.min;
        let max = data.max;
        let avg = data.sum / data.count as f64;
        println!("{name}={min}/{avg}/{max},");

    }
    print!("}}");
}

fn process_naive(file: File) {
    let reader = BufReader::with_capacity(1024*1024*16, file);
    let bar = ProgressBar::new(10_u64.pow(9));
    bar.set_style(ProgressStyle::with_template("[{elapsed} {eta_precise} {percent}{bar:40.cyan/blue}]").unwrap().progress_chars("##-"));
    let mut resultset: HashMap<String, StationData> = HashMap::new();

    reader.lines().for_each(|line| {
        let line = line.unwrap();

        let split = line.split(';').collect::<Vec<_>>();
        
        let station = split.get(0).unwrap();
        //println!("value as hex: {:?}", split.get(1).unwrap().as_bytes());
        let value: f64 = split.get(1).unwrap().parse().unwrap();

        if let Some(data) = resultset.get_mut(&station.to_string()) {
            if data.min > value {
                data.min = value;
            }
            if data.max < value {
                data.max = value;
            }

            data.sum += value;
            data.count += 1;

        }else{
            resultset.insert(station.to_string().clone(), StationData { min: value, max: value, sum: value, count: 1 });

        }
        bar.inc(1);
    });

    print!("{{");
    for (name, data) in resultset {
        let min = data.min;
        let max = data.max;
        let avg = data.sum / data.count as f64;
        println!("{name}={min}/{avg}/{max},");

    }
    print!("}}");



}

fn main() {
    let file = File::open("./measurements.txt").unwrap();
    //process_readline(file);
    process_naive(file);
    //process_naive_less_alloc(file);
    //process_chunked(file);
}
