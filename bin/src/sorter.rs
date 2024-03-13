use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;

use genawaiter::rc::Gen;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;

const MAX_CACHE_SIZE: usize = 4_000_000;

struct SortWorker<T: Clone + Ord + Serialize + DeserializeOwned> {
    tempdir: PathBuf,
    name: String,
    cache: Vec<T>,
    segments: Vec<PathBuf>,
    count: u64,
}

impl<T: Clone + Ord + Serialize + DeserializeOwned> SortWorker<T> {
    fn new(tempdir: PathBuf, name: String) -> Self {
        let mut cache = vec![];
        cache.reserve_exact(MAX_CACHE_SIZE);

        Self {
            tempdir,
            name,
            cache,
            segments: vec![],
            count: 0,
        }
    }

    fn push(&mut self, val: T) {
        self.cache.push(val);
        self.count += 1;

        if self.cache.len() >= MAX_CACHE_SIZE {
            self.flush().unwrap();
        }
    }

    fn flush(&mut self) -> Result<(), Box<dyn Error>> {
        let file_path = self.tempdir.join(format!(
            "sort_{}_segment.{}.bin",
            self.name,
            self.segments.len()
        ));

        // eprintln!(
        //     "flushing sorter cache to file: {}",
        //     file_path.to_str().unwrap()
        // );
        let mut writer = BufWriter::new(File::create(&file_path)?);
        self.segments.push(file_path);
        self.cache.sort_unstable();

        // eprintln!("sort complete; writing to file...");

        for elem in self.cache.iter() {
            bincode::serialize_into(&mut writer, &elem)?;
        }

        writer.flush()?;
        self.cache.clear();

        // eprintln!("flush complete");

        Ok(())
    }
}

struct SortReader<T: Clone + Ord + DeserializeOwned> {
    segments: Vec<PathBuf>,
    phantom: PhantomData<T>,
}

impl<T: Clone + Ord + DeserializeOwned> SortReader<T> {
    fn new(segments: Vec<PathBuf>) -> Self {
        Self {
            segments,
            phantom: PhantomData {},
        }
    }

    fn sorted(self) -> impl Iterator<Item = T> {
        Gen::new(|co| async move {
            let mut readers: Vec<BufReader<File>> = vec![];
            let mut pqueue: BinaryHeap<Reverse<(T, usize)>> = BinaryHeap::new();

            for filename in self.segments {
                readers.push(BufReader::new(File::open(filename).unwrap()));
            }

            for ridx in 0..readers.len() {
                let val = bincode::deserialize_from(&mut readers[ridx]).unwrap();
                pqueue.push(Reverse((val, ridx)));
            }

            let mut prev: Option<T> = None;

            while !pqueue.is_empty() {
                let Reverse((curr, ridx)) = pqueue.pop().unwrap();
                if prev.is_none() || curr != prev.unwrap() {
                    co.yield_(curr.clone()).await;
                }
                if let Ok(next) = bincode::deserialize_from(&mut readers[ridx]) {
                    pqueue.push(Reverse((next, ridx)));
                }
                prev = Some(curr);
            }
        })
        .into_iter()
    }
}

pub struct Sorter<T: Clone + Ord + Send + Serialize + DeserializeOwned + 'static> {
    name: String,
    handle: thread::JoinHandle<Vec<PathBuf>>,
    tx: mpsc::Sender<T>,
    count: u64,
}

impl<T: Clone + Ord + Send + Serialize + DeserializeOwned + 'static> Sorter<T> {
    pub fn new(tempdir: &Path, name: &str) -> Self {
        let (tx, rx) = mpsc::channel::<T>();

        let tempdir = tempdir.to_owned(); // HACK
        let name_string = name.to_string(); // HACK

        let handle = thread::spawn(move || {
            let mut sorter = SortWorker::<T>::new(tempdir, name_string);

            let rx = rx;

            for val in rx.into_iter() {
                sorter.push(val.clone());
            }

            sorter.flush().unwrap();

            sorter.segments
        });

        Self {
            name: name.to_string(), // HACK
            handle,
            tx,
            count: 0,
        }
    }

    pub fn push(&mut self, val: T) {
        self.tx.send(val.clone()).unwrap();
        self.count += 1;
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn sorted(self) -> impl Iterator<Item = T> {
        drop(self.tx);
        let segments = self.handle.join().unwrap();
        let reader = SortReader::new(segments);
        reader.sorted()
    }
}
