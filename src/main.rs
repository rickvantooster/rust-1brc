use core::panic;
use std::{fs::{self, File}, hint::black_box, io::{self, BufRead, BufReader, Read}, sync::{mpsc, Arc}, thread, time::Duration, u8};
mod threadpool;
mod multithreaded;


use indicatif::{ProgressBar, ProgressStyle};
use rustc_hash::FxHashMap;

use crate::threadpool::ThreadPool;


/*
*TODO: 
* 1. implement chunked version
* 2. implement multithreaded version. (possibly with actor model like pattern so we don´t have to
*    rely on mutex/rwlock's
*/
fn main() {
    let file = File::open("./measurements.txt").unwrap();
    //let file = File::open("./small_set.txt").unwrap();
    //test_chunk_reader(file);
    multithreaded::process_multithreaded_chunked(file);
    //process_chunked(file);
    //read_chunks(file);
    //test_chunk_reader(file);
}
struct ChunkReader<R> {
    reader: R,
    buffer: Vec<u8>,
    remaining: Vec<u8>
}

impl<R: Read> ChunkReader<R> {
    fn new(reader: R, chunk_size: usize) -> Self {
        Self {
            reader,
            buffer: vec![0; chunk_size],
            remaining: Vec::new()
        }
    }

    fn next_chunk(&mut self, dest: &mut Vec<u8>) -> Result<usize, io::Error> {
        let n_read = self.reader.read(&mut self.buffer)?;

        if n_read == 0 {
            return Ok(0);
        }

        let opt_idx = self.buffer.iter().rposition(|&p| p == b'\n');
        dest.extend_from_slice(&self.remaining);
        self.remaining.clear();
        match opt_idx {
            Some(idx) => { 
                dest.extend_from_slice(&self.buffer[..idx]);
                self.remaining.extend_from_slice(&self.buffer[idx..]);
            },
            None => dest.extend_from_slice(&self.buffer),
        };

        Ok(n_read)

    }
}

fn test_chunk_reader(file: File) {
    let buf_size = 1024*1024*16;
    let mut reader = BufReader::with_capacity(buf_size, file);
    let mut chunk_reader = ChunkReader::new(reader, buf_size);
    let mut buffer = vec![0; buf_size];
    let mut bytes_read = 0;

    loop {
        let read = chunk_reader.next_chunk(&mut buffer);

        if let Ok(0) = read {
            break;
        }

        if let Err(e) = read {
            panic!("Error reading chunks: {e}");
        }


        if let Ok(n) = read {
            bytes_read += n;
        }

        buffer.clear();

    }
    println!("\n\ntotal bytes read: {bytes_read}");

}

fn read_chunks(file: File) {
    let buf_size = 1024*1024*16;
    //let bar = ProgressBar::new(1024*1024*1024*16);
    //bar.set_style(ProgressStyle::with_template("[{elapsed} {eta_precise} {percent}{bar:40.cyan/blue}]").unwrap().progress_chars("##-"));

    let mut reader = BufReader::with_capacity(buf_size, file);
    let mut buffer = vec![0; buf_size];
    let mut bytes_read = 0;

    let mut combined = Vec::new();
    let mut remaining = Vec::new();

    /*
    use std::io::Write;
    let mut stdout = std::io::stdout();
    let mut lock = stdout.lock();
    */

    loop { 
        let read = reader.read(&mut buffer);

        if let Ok(0) = read {
            /*
            if !remaining.is_empty() {
                bytes_read += remaining.len();

            }*/
            break;
        }

        if let Err(e) = read {
            panic!("Error e: {e}");

        }
 
        if let Ok(n) = read {
            bytes_read += n;
            let opt_idx = buffer.iter().rposition(|&p| p == b'\n');
            combined.extend_from_slice(&remaining);
            remaining.clear();
            match opt_idx {
                Some(idx) => { 
                    combined.extend_from_slice(&buffer[..idx]);
                    remaining.extend_from_slice(&buffer[idx..]);
                },
                None => combined.extend_from_slice(&buffer),
            };
            //let string = String::from_utf8_lossy(&buffer);
            //println!("chunk as string: {string}");
        }
        //bytes_read += n;

        //let string = String::from_utf8_lossy(&combined);
        //writeln!(lock, "chunk as str = {string}").unwrap();
        buffer.clear();
        combined.clear();
        //bar.inc(1);

    }
    println!("\n\ntotal bytes read: {bytes_read}");
    //println!("\n\ntotal bytes read: {bytes_read}");
    

}

/*
fn test_chunk_iter(file: File) {
    let buf_size = 256;
    let mut reader = BufReader::with_capacity(buf_size, file);
    let chunk_reader = Lin::new(reader, buf_size);
    let mut string = String::new();

    for chunk_result in chunk_reader {
        match chunk_result {
            Ok(chunk) => {
                let string = String::from_utf8_lossy(&chunk);
                //black_box(string);
                println!("{string}\n\n\n");
            },
            Err(e) => {
                eprintln!("error: {e}");
                break;
            }
        }
    }

}
*/


enum ChunkMessageType {
    Chunk(Vec<u8>),
    Exit
}
#[derive(Debug)]
enum CollectionMessageType {
    Collection(FxHashMap<String, StationData>),
    Exit
}
enum LineMessageType {
    Line(String),
    Exit
}
enum ValueMessageType {
    Value(String, Value),
    Exit
}


type Value = i64;

#[derive(Debug)]
struct StationData {
    min: Value,
    max: Value,
    sum: Value,
    count: usize
}

fn round(value: f64) -> f64 {
    let rounded = (value * 10.0).round();
    if rounded == -0.0 {
        return 0.0;
    }

    rounded / 10.0


}

fn parse_num(input: &str) -> Value {
    let (is_negative, temp) = if input.chars().nth(0).unwrap() == '-'{
        (true, &input[1..input.len()])
    }else{
        (false, input)
    };

    let num = match temp.len() {
        1 => (temp.chars().nth(0).unwrap() as u8 - 48_u8) as Value,
        2 =>{
            let first = (temp.chars().nth(0).unwrap() as u8 - 48_u8) as Value;
            let second = (temp.chars().nth(1).unwrap() as u8 - 48_u8) as Value;
            first * 10 + second
        },
        3 => {
            let first = (temp.chars().nth(0).unwrap() as u8 - 48_u8) as Value;
            let fraction = (temp.chars().nth(2).unwrap() as u8 - 48_u8) as Value;
            first * 10 + fraction

        },
        4 => {
            let first = (temp.chars().nth(0).unwrap() as u8 - 48_u8) as Value;
            let second = (temp.chars().nth(1).unwrap() as u8 - 48_u8) as Value;
            let fraction = (temp.chars().nth(3).unwrap() as u8 - 48_u8) as Value;
            first * 100 + second * 10 + fraction

        }
        _ => unreachable!("unreachable case for input {input}")

    };

    if is_negative {
       return -num
    }

    num
}

fn test_parse_num(){
    let inputs = vec!["9", "-9", "-9.9", "-99.9", "9.9", "99", "99.9"];
    let results: Vec<Value> = inputs.iter().map(|val| parse_num(val)).collect();
    for i in 0..inputs.len() {
        println!("input: {}, output: {}", inputs[i], results[i]);

    }

}




fn process_naive_less_alloc(file: File) {
    let mut reader = BufReader::with_capacity(1024*1024*16, file);
    let bar = ProgressBar::new(10_u64.pow(9));
    bar.set_style(ProgressStyle::with_template("[{elapsed} {eta_precise} {percent}{bar:40.cyan/blue}]").unwrap().progress_chars("##-"));
    let mut resultset: FxHashMap<String, StationData> = FxHashMap::default();
    let mut line = String::new();

    while reader.read_line(&mut line).unwrap() != 0 {
        line.pop();
        let split = line.split(';').collect::<Vec<_>>();
        
        let station = split.get(0).unwrap();
        //println!("value as hex: {:?}", split.get(1).unwrap().as_bytes());
        //let value: f64 = split.get(1).unwrap().parse().unwrap();
        let value: Value = parse_num(split.get(1).unwrap());

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
        let min = round(data.min as f64 / 10.0);
        let max = round(data.max as f64 / 10.0);
        let avg = round((data.sum as f64) / 10.0 / data.count as f64);
        println!("{name}={min}/{avg}/{max},");

    }
    print!("}}");
}



fn process_multithreaded(file: File) {
    let mut reader = BufReader::with_capacity(1024*1024*16, file);
    let bar = ProgressBar::new(10_u64.pow(9));
    bar.set_style(ProgressStyle::with_template("[{elapsed} {eta_precise} {percent}{bar:40.cyan/blue}]").unwrap().progress_chars("##-"));

    let mut line = String::new();
    let (ln_tx, ln_rx) = mpsc::channel::<LineMessageType>();

    let parser_handle = thread::spawn(move ||{
        let mut resultset: FxHashMap<String, StationData> = FxHashMap::default();
        while let LineMessageType::Line(mut line) = ln_rx.recv().unwrap(){
            line.pop();
            let split = line.split(';').collect::<Vec<_>>();
            
            let station = split.get(0).unwrap();
            let value: Value = parse_num(split.get(1).unwrap());

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
            //let _ = parser_val_tx.send(ValueMessageType::Value(station.to_string(), value)).unwrap();
        }

        print!("{{");
        for (name, data) in resultset {
            let min = round(data.min as f64 / 10.0);
            let max = round(data.max as f64 / 10.0);
            let avg = round((data.sum as f64) / 10.0 / data.count as f64);
            println!("{name}={min}/{avg}/{max},");

        }
        print!("}}");
    });



    while reader.read_line(&mut line).unwrap() != 0 {
        let _ = ln_tx.send(LineMessageType::Line(line.clone())).unwrap();

        bar.inc(1);
        line.clear();

    }

    ln_tx.send(LineMessageType::Exit).unwrap();
    let _ = parser_handle.join().unwrap();
    eprintln!("end of parser join");
    //val_tx.send(ValueMessageType::Exit).unwrap();
    //let _ = set_handle.join().unwrap();
}


fn process_chunked(file: File) {

    let reader = BufReader::with_capacity(1024*1024*16, file);
    let bar = ProgressBar::new(10_u64.pow(9));
    bar.set_style(ProgressStyle::with_template("[{elapsed} {eta_precise} {percent}{bar:40.cyan/blue}]").unwrap().progress_chars("##-"));

    let mut resultset: FxHashMap<String, StationData> = FxHashMap::default();

    
    //let chunk_reader = ChunkedLines::new(reader, 512);
    let mut buffer = vec![0; 1024*1024*16];
    let mut chunk_reader = ChunkReader::new(reader, 1024*1024*16);
    let mut bytes_read = 0;
    loop {
        let read = chunk_reader.next_chunk(&mut buffer);

        if let Ok(0) = read {
            break;
        }

        if let Err(e) = read {
            panic!("An error occured when trying to read a chunk: {e}");
        }

        if let Ok(n) = read {
            bytes_read += n;

            let string = unsafe {String::from_utf8_unchecked(buffer.clone())}; 

            let mut split: Vec<&str> = Vec::new();
            string.split('\n').for_each(|line|{
                line.split(';').for_each(|part| split.push(part));
                let station = split.get(0).unwrap();
                let value: Value = parse_num(split.get(1).unwrap());

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
                split.clear();

            });

        }

    }
    println!("processed {bytes_read} bytes");
    

    print!("{{");
    for (name, data) in resultset {
        let min = round(data.min as f64 / 10.0);
        let max = round(data.max as f64 / 10.0);
        let avg = round((data.sum as f64) / 10.0 / data.count as f64);
        println!("{name}={min}/{avg}/{max},");

    }
    print!("}}");

    //col_tx.send(CollectionMessageType::Exit).unwrap();

    //let _ = set_handle.join().unwrap();
}

