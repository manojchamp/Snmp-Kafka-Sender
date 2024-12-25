// use clap::Parser;
// use rdkafka::config::ClientConfig;
// use rdkafka::producer::{BaseProducer, BaseRecord};
// use serde_json::Value;
// use std::fs::File;
// use std::time::Duration;
// use tokio::{task, time::sleep};

// /// Command-line arguments for the application
// #[derive(Parser, Debug)]
// #[clap(author, version, about, long_about = None)]
// struct Args {
//     /// Path to the input JSON file
//     #[clap(short, long)]
//     file: String,

//     /// Kafka broker address
//     #[clap(short = 'k', long, default_value = "localhost:9092")]
//     broker: String,

//     /// Kafka topic to publish to
//     #[clap(short = 'd', long, default_value = "ifadmin")]
//     topic: String,

//     /// Publish interval in seconds
//     #[clap(short = 'i', long, default_value_t = 1)]
//     interval: u64,

//     /// Number of tasks to spawn
//     #[clap(short = 't', long, default_value_t = 4)]
//     tasks: usize,

//     /// Size of each batch
//     #[clap(short = 's', long, default_value_t = 100)]
//     batch_size: usize,
// }

// #[tokio::main]
// async fn main() {
//     // Parse command-line arguments
//     let args = Args::parse();

//     // Load and parse JSON file dynamically
//     let json_data = load_json_data(&args.file);

//     // Divide JSON data into chunks for multitasking
//     let chunks: Vec<Vec<Value>> = json_data
//         .chunks(json_data.len() / args.tasks + 1)
//         .map(|chunk| chunk.to_vec())
//         .collect();

//     // Initialize Kafka producer
//     let producer = create_kafka_producer(&args.broker);

//     println!(
//         "Starting to publish messages to topic: {} using {} tasks",
//         args.topic, args.tasks
//     );

//     // Spawn tasks for each chunk
//     let mut handles = Vec::new();
//     for chunk in chunks {
//         let producer = producer.clone();
//         let topic = args.topic.clone();
//         let interval = args.interval;
//         let batch_size = args.batch_size;

//         handles.push(task::spawn(async move {
//             loop {
//                 publish_messages(&producer, &topic, chunk.clone(), interval, batch_size).await;
//             }
//         }));
//     }

//     // Wait for all tasks to run indefinitely
//     for handle in handles {
//         if let Err(e) = handle.await {
//             eprintln!("Task failed: {:?}", e);
//         }
//     }
// }

// /// Load JSON data from a file
// fn load_json_data(file_path: &str) -> Vec<Value> {
//     let file = File::open(file_path).expect("Failed to open input JSON file");
//     serde_json::from_reader(file).expect("Failed to parse JSON")
// }

// /// Create a Kafka producer
// fn create_kafka_producer(broker: &str) -> BaseProducer {
//     ClientConfig::new()
//         .set("bootstrap.servers", broker)
//         .set("message.timeout.ms", "5000")
//         .create::<BaseProducer>()
//         .expect("Failed to create Kafka producer")
// }

// /// Publish JSON data to Kafka topic in intervals
// async fn publish_messages(
//     producer: &BaseProducer,
//     topic: &str,
//     json_data: Vec<Value>,
//     interval: u64,
//     batch_size: usize,
// ) {
//     for batch in json_data.chunks(batch_size) {
//         let mut batch_messages = Vec::new();

//         // Serialize the batch of messages
//         for data in batch {
//             if let Ok(message) = serde_json::to_string(data) {
//                 let key = data.get("neKey")
//                     .and_then(|k| k.as_str())
//                     .unwrap_or("default_key")
//                     .to_string();

//                 batch_messages.push((key, message));
//             }
//         }

//         // Send batch messages to Kafka
//         for (key, message) in batch_messages {
//             if let Err(e) = producer.send(BaseRecord::to(topic).payload(&message).key(&key)) {
//                 eprintln!("Failed to send message to Kafka: {:?}", e);
//             } else {
//                 println!("Published: {}", message);
//             }
//         }

//         // Wait for the specified interval before processing the next batch
//         //sleep(Duration::from_secs(interval)).await;
//     }
    
// }


use clap::Parser;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord};
use serde_json::{Value, json};
use std::fs::File;
use std::time::Duration;
use tokio::{task, time::sleep};
use chrono::Utc;
use chrono::SecondsFormat;

/// Command-line arguments for the application
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Path to the input JSON file
    #[clap(short, long)]
    file: String,

    /// Kafka broker address
    #[clap(short = 'k', long, default_value = "localhost:9092")]
    broker: String,

    /// Kafka topic to publish to
    #[clap(short = 'd', long, default_value = "ifadmin")]
    topic: String,

    /// Publish interval in seconds
    #[clap(short = 'i', long, default_value_t = 1)]
    interval: u64,

    /// Number of tasks to spawn
    #[clap(short = 't', long, default_value_t = 4)]
    tasks: usize,

    /// Size of each batch
    #[clap(short = 's', long, default_value_t = 100)]
    batch_size: usize,
}

#[tokio::main]
async fn main() {
    // Parse command-line arguments
    let args = Args::parse();

    // Load and parse JSON file dynamically
    let json_data = load_json_data(&args.file);

    // Divide JSON data into chunks for multitasking
    let chunks: Vec<Vec<Value>> = json_data
        .chunks(json_data.len() / args.tasks + 1)
        .map(|chunk| chunk.to_vec())
        .collect();

    // Initialize Kafka producer
    let producer = create_kafka_producer(&args.broker);

    println!(
        "Starting to publish messages to topic: {} using {} tasks",
        args.topic, args.tasks
    );

    // Spawn tasks for each chunk
    let mut handles = Vec::new();
    for chunk in chunks {
        let producer = producer.clone();
        let topic = args.topic.clone();
        let interval = args.interval;
        let batch_size = args.batch_size;

        handles.push(task::spawn(async move {
            loop {
                publish_messages(&producer, &topic, chunk.clone(), interval, batch_size).await;
            }
        }));
    }

    // Wait for all tasks to run indefinitely
    for handle in handles {
        if let Err(e) = handle.await {
            eprintln!("Task failed: {:?}", e);
        }
    }
}

/// Load JSON data from a file
fn load_json_data(file_path: &str) -> Vec<Value> {
    let file = File::open(file_path).expect("Failed to open input JSON file");
    serde_json::from_reader(file).expect("Failed to parse JSON")
}

/// Create a Kafka producer
fn create_kafka_producer(broker: &str) -> BaseProducer {
    ClientConfig::new()
        .set("bootstrap.servers", broker)
        .set("message.timeout.ms", "5000")
        .create::<BaseProducer>()
        .expect("Failed to create Kafka producer")
}

/// Publish JSON data to Kafka topic in intervals
async fn publish_messages(
    producer: &BaseProducer,
    topic: &str,
    mut json_data: Vec<Value>,
    interval: u64,
    batch_size: usize,
) {
    loop {
        for batch in json_data.chunks_mut(batch_size) {
            let mut batch_messages = Vec::new();

            // Serialize the batch of messages with updated datetime
            for data in batch {
                // Update the `updatedOn` field to the current datetime
                if let Some(if_admin_status_pk) = data.get_mut("ifAdminStatusPK") {
                    if let Some(updated_on) = if_admin_status_pk.get_mut("updatedOn") {
                        *updated_on = json!(Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true));
                    }
                }

                // Convert updated data to a JSON string
                if let Ok(message) = serde_json::to_string(data) {
                    let key = data
                        .get("ifAdminStatusPK")
                        .and_then(|pk| pk.get("neKey"))
                        .and_then(|ne_key| ne_key.as_str())
                        .unwrap_or("default_key")
                        .to_string();

                    batch_messages.push((key, message));
                }
            }

            // Send batch messages to Kafka
            for (key, message) in batch_messages {
                if let Err(e) = producer.send(BaseRecord::to(topic).payload(&message).key(&key).partition(0)) {
                    eprintln!("Failed to send message to Kafka: {:?}", e);
                } else {
                    println!("Published: {}", message);
                }
            }
        }

        // Wait for the specified interval before processing the next batch
        sleep(Duration::from_millis(interval)).await;
    }
}
