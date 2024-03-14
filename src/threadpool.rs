use core::panic;
use std::{sync::{mpsc, Arc, Mutex}, thread};

pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<mpsc::Sender<TaskMessage>>


}

type Job = Box<dyn FnOnce() + Send + 'static>;

enum TaskMessage {
    Job(Job),
    Exit
}

impl ThreadPool {
    pub fn new(n: usize) -> ThreadPool {
        assert!(n > 0);


        let mut workers = Vec::with_capacity(n);
        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));

        for id in 0..n {
            workers.push(Worker::new(id, Arc::clone(&receiver)));

        }

        ThreadPool {
            workers,
            sender: Some(sender)
        }

    }

    pub fn execute<F>(&self, f: F)
        where
            F: FnOnce() + Send + 'static
    {
        let job = Box::new(f);

        match self.sender.as_ref() {
            Some(sender) => {
                match sender.send(TaskMessage::Job(job)) {
                    Ok(_) => (),
                    Err(e) => panic!("An error occured when trying to execute job. Error {e}"),
                }
            },
            None => (),
        }
    }

        

}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        drop(self.sender.take());
        for worker in &mut self.workers {
            if let Some(thread) = worker.thread.take() {

                thread.join().unwrap();
            }

        }
    }
}

pub struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>

}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<TaskMessage>>>) -> Worker {
        let thread = thread::spawn(move || {
            loop {
                let recv = receiver.lock().unwrap().recv();
                match recv {
                    Ok(message) => {
                        match message {
                            TaskMessage::Job(job) => job(),
                            TaskMessage::Exit => break,
                        }
                        //println!("Worker {id} got a job; executing.");
                    },
                    Err(e) => {
                        break;

                    },
                }
            }

        });
        Worker { id, thread: Some(thread) }

    }

}
