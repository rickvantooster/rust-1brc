use std::{fs::File, io::BufReader, sync::{mpsc, Arc, Condvar, Mutex}, thread};

use indicatif::{ProgressBar, ProgressStyle};
use rustc_hash::FxHashMap;

use crate::{parse_num, round, threadpool::ThreadPool, ChunkReader, CollectionMessageType, StationData, Value};

pub fn process_multithreaded_chunked(file: File) {

    let reader = BufReader::with_capacity(1024*1024*16, file);
    let bar = ProgressBar::new(10_u64.pow(9));
    bar.set_style(ProgressStyle::with_template("[{elapsed} {eta_precise} {percent}{bar:40.cyan/blue}]").unwrap().progress_chars("##-"));

    let (col_tx, col_rx) = mpsc::sync_channel::<CollectionMessageType>(1000);

    let collection_handle = thread::spawn(move || {
        let mut resultset: FxHashMap<String, StationData> = FxHashMap::default();
        loop {
            let incoming = col_rx.recv();
            match incoming {
                Ok(col_msg) => {
                    if let CollectionMessageType::Collection(col) = col_msg {
                        
                        for (station, data) in col {

                            if let Some(entry) = resultset.get_mut(&station) {
                                entry.sum += data.sum;
                                entry.count += data.count;
                                if data.min < entry.min {
                                    entry.min = data.min;
                                }
                                if data.max > entry.max {
                                    entry.max = data.max;
                                }

                            } else {
                                resultset.insert(station, data);

                            }

                        }
                    } else if let CollectionMessageType::Exit = col_msg {
                        break;

                    }

                },
                Err(e) => {
                    eprintln!("collector thread panicked with error: {e}");
                    break;

                },
            }

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
    
    let wg = Arc::new(WaitGroup::new());

    let parser_group = ThreadPool::new(8);
    let mut n_chunks = 0;

    
    let mut buffer = vec![0; 1024*1024*16];
    let mut chunk_reader = ChunkReader::new(reader, 1024*1024*16);
    let mut bytes_read = 0;

    loop {
        n_chunks += 1;
        let read = chunk_reader.next_chunk(&mut buffer);

        if let Ok(0) = read {
            break;
        }

        if let Err(e) = read {
            panic!("An error occured when trying to read a chunk: {e}");
        }

        if let Ok(n) = read {
            bytes_read += n;
            let col_tx_clone = col_tx.clone();
            let chunk = buffer.clone();
            buffer.clear();


            let wg_clone = Arc::clone(&wg);

            parser_group.execute(move || {

                wg_clone.add(1);
                let mut resultset: FxHashMap<String, StationData> = FxHashMap::default();
                let string = unsafe {String::from_utf8_unchecked(chunk)}; 

                string.split('\n').for_each(|line| {
                    if line.trim().is_empty() {
                        return;
                    }

                    let (station, temp) = line.split_once(';').unwrap();
                    
                    let value: Value = parse_num(temp);

                    if let Some(data) = resultset.get_mut(&station.to_string()) {
                        if data.min > value {
                            data.min = value;
                        }
                        if data.max < value {
                            data.max = value;
                        }

                        data.sum += value;
                        data.count += 1;

                    } else {
                        resultset.insert(station.to_string().clone(), StationData { min: value, max: value, sum: value, count: 1 });

                    }

                });

                let send_res = col_tx_clone.send(CollectionMessageType::Collection(resultset));
                

                match send_res {
                    Ok(_) => (),
                    Err(e) => eprintln!("An error occured when trying to send collection ({n} bytes read): {e}"),
                };

                wg_clone.done();
            });
        }
    }

    wg.wait();

    println!("processed {bytes_read} bytes");
    println!("Finished processing {n_chunks} chunks");
    
    println!("end of parser join");
    let _ = col_tx.send(CollectionMessageType::Exit).unwrap();
    collection_handle.join().unwrap();

}


pub struct WaitGroup {
    counter: Mutex<usize>,
    condvar: Condvar
}

impl WaitGroup {
    pub fn new() -> Self {
        WaitGroup { 
            counter: Mutex::new(0), 
            condvar: Condvar::new() 
        }
    }

    pub fn add(&self, delta: usize) {
        let mut counter = self.counter.lock().unwrap();
        *counter += delta;
    }

    pub fn done(&self) {

        let mut counter = self.counter.lock().unwrap();
        if *counter == 0 {
            return;
        }

        *counter -= 1;
        self.condvar.notify_one();
    }

    pub fn wait(&self) {
        let mut counter = self.counter.lock().unwrap();
        while *counter > 0 {
            counter = self.condvar.wait(counter).unwrap();
        }
    }

}
