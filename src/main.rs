use std::{str::FromStr, sync::Arc};

use reqwest::{Error, Method, Request, Url};
use tokio::{fs, sync::Semaphore};

#[tokio::main]
async fn main() {
    let client = reqwest::Client::new();
    with_rename(client, 7503126, 7503197, 3, 5).await;

    let _ = tokio::signal::ctrl_c().await;
}

async fn with_rename(
    client: reqwest::Client,
    srx_from: usize,
    srx_to: usize,
    number_of_threads: usize,
    max_retry: usize,
) {
    let semaphore = Arc::new(Semaphore::new(number_of_threads));
    for i in srx_from..(srx_to + 1) {
        let semaphore = semaphore.clone();
        let client = client.clone();
        tokio::spawn(async move {
            let mut number_of_retry = 0;
            let mut permit = None;
            let (id, srr) = loop {
                if permit.is_none() {
                    let _ = permit.insert(semaphore.acquire().await.unwrap());
                }
                match read_id_and_srr(&client, i).await {
                    Ok(v) => {
                        break v;
                    }
                    Err(_) => {
                        if number_of_retry == max_retry {
                            println!("maximum retry time reached for srx number {}", i);
                            return;
                        }
                        number_of_retry += 1;
                        println!(
                            "SRR request failed for SRX{}, {} retry remains",
                            i,
                            max_retry - number_of_retry
                        );

                        permit.take();
                        continue;
                    }
                }
            };
            number_of_retry = 0;
            let bytes = loop {
                if permit.is_none() {
                    let _ = permit.insert(semaphore.acquire().await.unwrap());
                }
                match read_file(&client, &srr).await {
                    Ok(v) => break v,
                    Err(_) => {
                        if number_of_retry == max_retry {
                            println!("maximum retry time reached for SRX{}", i);
                            return;
                        }
                        number_of_retry += 1;
                        println!(
                            "File request failed for SRX{}, {} retry remains",
                            i,
                            max_retry - number_of_retry
                        );

                        permit.take();
                        continue;
                    }
                }
            };

            fs::write(
                format!("C:/Users/Administrator/Downloads/fastq/{}.fastq.gz", id),
                bytes,
            )
            .await
            .expect("write to file to succeed");
            drop(permit);
            println!("finished {}", srr)
        });
    }
    println!("all tasks have been distributed")
}

async fn read_id_and_srr(
    client: &reqwest::Client,
    srx_number: usize,
) -> Result<(String, String), Error> {
    let req = Request::new(
        Method::GET,
        Url::from_str(&format!(
            "https://www.ncbi.nlm.nih.gov/sra/SRX{}[accn]",
            srx_number
        ))
        .unwrap(),
    );
    let bytes = client.execute(req).await?.bytes().await?;
    let pos = String::from_utf8(bytes.to_vec())
        .unwrap()
        .find("trace.ncbi.nlm.nih.gov/Traces?run=SRR")
        .unwrap();
    let srr_number = String::from_utf8(bytes[(pos + 34)..(pos + 45)].to_vec()).unwrap();
    let pos = String::from_utf8(bytes.to_vec())
        .unwrap()
        .find("Sample: <span>")
        .unwrap();
    let id = String::from_utf8(bytes[(pos + 14)..(pos + 19)].to_vec()).unwrap();
    println!("found id {}, srr {}", id, srr_number);
    Ok((id, srr_number))
}

async fn read_file(client: &reqwest::Client, srr: &String) -> Result<bytes::Bytes, Error> {
    let req = Request::new(
        Method::GET,
        Url::from_str(&format!(
            "https://www.be-md.ncbi.nlm.nih.gov/Traces/sra-reads-be/fastq?acc={}",
            srr
        ))
        .unwrap(),
    );
    client.execute(req).await?.bytes().await
}
