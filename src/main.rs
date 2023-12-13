use std::{str::FromStr, sync::Arc};

use reqwest::{Method, Request, Url};
use tokio::{fs, sync::Semaphore, task::JoinSet};

#[tokio::main]
async fn main() {
    let client = reqwest::Client::new();
    let config = fs::read_to_string("./config.txt").await.unwrap();
    let mut lines = config.lines();
    let mut file_path = "".to_string();
    let max_concurrent_network_req = 5;
    let network_semaphore = Arc::new(Semaphore::new(max_concurrent_network_req));
    // Start reading lines
    while let Some(item) = lines.next() {
        print!("Line feed: {}", item);
        if item.is_empty() || item[0..1] == "#".to_owned() {
            print!("...Skipped\n");
            continue; // Skip the empty lines or commented out lines
        }
        print!("\n");
        // When the line is started with SRR
        if item[..3] == "SRR".to_string() {
            let mut item_line = item.split_whitespace(); // read the line with space as separator
            let srr_number = item_line.next().unwrap()[3..].parse().unwrap(); // get the number
            let file_name = format!("{}/{}", file_path, item_line.next().unwrap());
            let arguments = item_line
                .map(|str| str.to_string())
                .collect::<Vec<String>>();
            if arguments.contains(&"preflight".into()) {
                println!("Filename constructed: {}", file_name);
                continue;
            }
            tokio::spawn(download(
                client.clone(),
                network_semaphore.clone(),
                srr_number,
                file_name,
                5,
            ));
            continue;
        }
        // When the line is started with SRX
        if item[..3] == "SRX".to_string() {
            let mut item_line = item.split(" ");
            let srx = item_line.next().unwrap();
            let (from, to) = read_srx(srx);
            let file_path = file_path.clone();
            // arguments are supplied after the SRX number
            let arguments = Arc::new(
                item_line
                    .map(|str| str.to_string())
                    .collect::<Vec<String>>(),
            );
            // Set up preflight
            if arguments.contains(&"preflight".into()) {
                println!("Running prelight for SRX{}-{}", from, to);
                let file_path = Arc::new(file_path.clone());
                let mut join_set = JoinSet::new();
                // Spawn workers for preflight
                for srx in from..(to + 1) {
                    let network_semaphore = network_semaphore.clone();
                    let client = client.clone();
                    let arguments = arguments.clone();
                    let file_path = file_path.clone();
                    join_set.spawn(async move {
                        match read_name_and_srr(
                            client,
                            network_semaphore,
                            srx,
                            5,
                            arguments.as_ref(),
                        )
                        .await
                        {
                            Ok((name, _srr)) => {
                                println!("File path constructed: {}/{}.fastq.gz", file_path, name);
                                Ok(())
                            }
                            Err(_) => {
                                println!("max retry times reached for SRX {}", srx);
                                Err(())
                            }
                        }
                    });
                }
                // Spawn a task to track the completion
                tokio::spawn(async move {
                    let mut success = 0;
                    while let Some(v) = join_set.join_next().await {
                        if let Ok(Ok(())) = v {
                            success += 1;
                        }
                    }
                    println!(
                        "Preflight for SRX{}-{} completed, {} out of {} success",
                        from,
                        to,
                        success,
                        to - from + 1
                    )
                });
                continue;
            };
            // Run actual download
            let semaphore = network_semaphore.clone();
            let client = client.clone();
            let mut join_set = JoinSet::new();
            for srx in from..(to + 1) {
                let (id, srr) =
                    match read_name_and_srr(client.clone(), semaphore.clone(), srx, 5, &arguments)
                        .await
                    {
                        Ok(v) => v,
                        Err(_) => {
                            println!("max retry times reached for SRX {}", srx);
                            return;
                        }
                    };
                let file_path = format!("{}/{}.fastq.gz", file_path, id);
                join_set.spawn(download(
                    client.clone(),
                    semaphore.clone(),
                    Url::from_str(&format!(
                        "https://www.be-md.ncbi.nlm.nih.gov/Traces/sra-reads-be/fastq?acc=SRR{}",
                        srr
                    ))
                    .unwrap(),
                    file_path,
                    5,
                ));
            }
            continue;
        }
        if item[..3] == "CRA".to_string() {
            let mut item_line = item.split_whitespace();
            let cra_number: usize = item_line.next().unwrap()[3..].parse().unwrap();
            let (from, to) = read_srx(item_line.next().unwrap());
            let arguments = Arc::new(
                item_line
                    .map(|str| str.to_string())
                    .collect::<Vec<String>>(),
            );
            let mut join_set = JoinSet::new();
            for crx in from..(to + 1) {
                let client = client.clone();
                let network_semaphore = network_semaphore.clone();
                let file_path = file_path.clone();
                if arguments.contains(&"preflight".into()) {
                    join_set.spawn(async move {
                        let (alias, files, _crr) = read_alias_and_crr_with_crx(
                            client.clone(),
                            network_semaphore.clone(),
                            cra_number,
                            crx,
                            5,
                        )
                        .await?;
                        for file in files {
                            println!("File path constructed: {}/{}_{}", file_path, alias, file);
                        }
                        Ok(())
                    });
                    continue;
                }
                join_set.spawn(async move {
                    let result = read_alias_and_crr_with_crx(
                        client.clone(),
                        network_semaphore.clone(),
                        cra_number,
                        crx,
                        5,
                    )
                    .await;
                    if let Ok((alias, names, crr)) = result {
                        for file in names {
                            let file_path = format!("{}/{}_{}", file_path, alias, file);
                            download(
                                client.clone(),
                                network_semaphore.clone(),
                                Url::from_str(&format!(
                                    "https://download.cncb.ac.cn/gsa/CRA{:0>6}/CRR{}/{}",
                                    cra_number, crr, file
                                ))
                                .unwrap(),
                                file_path,
                                5,
                            )
                            .await
                            .expect("download to succeed");
                        }
                        Ok(())
                    } else {
                        Err(())
                    }
                });
            }
            // Spawn a task to track the completion
            tokio::spawn(async move {
                let mut success = 0;
                while let Some(v) = join_set.join_next().await {
                    if let Ok(Ok(())) = v {
                        success += 1;
                    }
                }
                println!(
                    "Task for CRX{}-{} completed, {} out of {} success",
                    from,
                    to,
                    success,
                    to - from + 1
                )
            });
        }
        // When the line is only for configuration
        let mut config_line = item.split_whitespace();
        file_path = config_line.next().unwrap().to_owned();
        let _ = fs::create_dir_all(file_path.clone()).await;
    }
    println!("All lines have been read");
    let _ = tokio::signal::ctrl_c().await;
}

/// To initate a download, the file path to write to and the SRR number is needed.
async fn download(
    client: reqwest::Client,
    network_semaphore: Arc<Semaphore>,
    url: Url,
    file_path: String,
    max_retry: usize,
) -> Result<(), ()> {
    let bytes = get_req(
        client.clone(),
        network_semaphore.clone(),
        url.clone(),
        max_retry,
    )
    .await?;
    if let Err(e) = fs::write(&file_path, bytes).await {
        println!("Failed to write to {}: {:?}", file_path, e);
        Err(())
    } else {
        println!("{} finished and written to {}", url.to_string(), file_path);
        Ok(())
    }
}

/// This function does not spawn additional task.
async fn read_name_and_srr(
    client: reqwest::Client,
    semaphore: Arc<Semaphore>,
    srx_number: usize,
    max_retry: usize,
    arguments: &Vec<String>,
) -> Result<(String, String), ()> {
    let mut retry_times = 0;
    let mut permit = None;
    loop {
        if retry_times >= max_retry {
            break Err(());
        }
        permit.get_or_insert(semaphore.acquire().await.expect("semaphore to be open"));
        let req = Request::new(
            Method::GET,
            Url::from_str(&format!(
                "https://www.ncbi.nlm.nih.gov/sra/SRX{}[accn]",
                srx_number
            ))
            .unwrap(),
        );
        let bytes = match match client.execute(req).await {
            Ok(res) => res,
            Err(_) => {
                retry_times += 1;
                continue;
            }
        }
        .bytes()
        .await
        {
            Ok(v) => v,
            Err(_) => {
                retry_times += 1;
                continue;
            }
        };
        let document = String::from_utf8(bytes.to_vec()).unwrap();
        let srr_pos = document
            .find("trace.ncbi.nlm.nih.gov/Traces?run=SRR")
            .unwrap();
        let mut srr_number =
            String::from_utf8(bytes[(srr_pos + 34)..(srr_pos + 45)].to_vec()).unwrap();
        srr_number.retain(|char| char.is_numeric());
        let filename = if arguments.contains(&"srr_override".into()) {
            srr_number.clone()
        } else if arguments.contains(&"library_name_override".into()) {
            let pos = document
                .find("Library: <div class=\"expand-body\"><div>Name: <span>")
                .unwrap();
            let name_regex = regex::Regex::new(">[^<>/]*<").unwrap();
            let name_search_result = name_regex.find(&document[pos + 48..pos + 75]);
            if let Some(matched) = name_search_result {
                let matched_str = matched.as_str();
                matched_str[1..matched_str.len() - 1].to_owned()
            } else {
                panic!(
                    "No match for name under library section found on SRR{} with document {}",
                    srr_number,
                    &document[pos + 48..pos + 75]
                )
            }
        } else {
            let pos = document.find("Sample: <span>").unwrap();
            String::from_utf8(bytes[(pos + 14)..(pos + 19)].to_vec()).unwrap()
        };
        break Ok((filename, srr_number));
    }
}

async fn read_alias_and_crr_with_crx(
    client: reqwest::Client,
    network_semaphore: Arc<Semaphore>,
    cra: usize,
    crx: usize,
    max_retry: usize,
) -> Result<(String, Vec<String>, String), ()> {
    let document = String::from_utf8(
        get_req(
            client.clone(),
            network_semaphore.clone(),
            Url::from_str(&format!(
                "https://ngdc.cncb.ac.cn/gsa/browse/CRA{:0>6}/CRX{:0>6}",
                cra, crx
            ))
            .unwrap(),
            max_retry,
        )
        .await
        .unwrap()
        .to_vec(),
    )
    .unwrap();
    let crr_number = {
        let crr_pos = document
            .find(&format!(r#"<td><a href="browse/CRA{:0>6}/"#, cra))
            .unwrap();
        let mut crr_number = document[(crr_pos + 30)..(crr_pos + 40)].to_owned();
        crr_number.retain(|char| char.is_numeric());
        crr_number
    };
    let (alias, filename) = {
        let document = String::from_utf8(
            get_req(
                client,
                network_semaphore,
                Url::from_str(&format!(
                    "https://ngdc.cncb.ac.cn/gsa/browse/CRA{:0>6}/CRR{:0>6}",
                    cra, crr_number
                ))
                .unwrap(),
                max_retry,
            )
            .await
            .unwrap()[1000..]
                .to_vec(),
        )
        .unwrap();
        let alias_regex = regex::Regex::new(&format!(
            "<tr>[^a-z]*?<td>CRR{}</td>[^a-z]*?<td>.*</td>",
            crr_number
        ))
        .unwrap();
        let alias = &alias_regex.find(&document).unwrap().as_str()[25..];
        let alias_regex = regex::Regex::new("td>[^<>/]*</").unwrap();
        let alias = alias_regex.find(alias).unwrap();
        let alias = alias.as_str()[3..alias.as_str().len() - 2].to_owned();
        let name_regex = regex::Regex::new(&format!(
            r#"download.cncb.ac.cn/gsa/CRA{:0>6}/CRR{:0>6}/[^<>"]*""#,
            cra, crr_number
        ))
        .unwrap();
        let name_search_result = name_regex.find_iter(&document);
        (
            alias,
            name_search_result
                .map(|matched| {
                    let matched_str = matched.as_str();
                    matched_str[44..matched_str.len() - 1].to_owned()
                })
                .collect::<Vec<String>>(),
        )
    };
    Ok((alias, filename, crr_number))
}

/// Read SRX from syntax `SRX123456-789000`.
/// The number can be supplied with decending order.
fn read_srx(srx: &str) -> (usize, usize) {
    if srx.contains("-") {
        // When SRX is supplied in a range, inclusive
        let mut split = srx[3..].split("-");
        let num1 = split.next().unwrap().parse().unwrap();
        let num2 = split.next().unwrap().parse().unwrap();
        // The order of the SRX number is automatically determined
        if num1 > num2 {
            (num2, num1)
        } else {
            (num1, num2)
        }
    } else {
        // Single srx
        let num = srx.parse().unwrap();
        (num, num + 1)
    }
}

async fn get_req(
    client: reqwest::Client,
    network_semaphore: Arc<Semaphore>,
    url: Url,
    max_retry: usize,
) -> Result<bytes::Bytes, ()> {
    let mut retry_times = 0;
    loop {
        if retry_times >= max_retry {
            break Err(());
        }
        let permit = network_semaphore
            .acquire()
            .await
            .expect("semaphore to be open");
        let req = Request::new(Method::GET, url.clone());
        let bytes = match match client.execute(req).await {
            Ok(res) => res,
            Err(_) => {
                retry_times += 1;
                drop(permit);
                continue;
            }
        }
        .bytes()
        .await
        {
            Ok(v) => v,
            Err(_) => {
                retry_times += 1;
                drop(permit);
                continue;
            }
        };
        drop(permit);
        break Ok(bytes);
    }
}
