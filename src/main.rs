use std::{str::FromStr, sync::Arc};

use bytes::Bytes;
use reqwest::{Client, Method, Request, Url};
use serde::{Deserialize, Serialize};
use tokio::{fs, sync::Semaphore};

use crate::{cnbi::read_alias_and_crr_with_crx, ncbi::read_name_and_srr};

mod cnbi;
mod ncbi;
#[macro_use]
mod macros;

#[tokio::main]
async fn main() {
    let global_config = match fs::read("./config.json").await {
        Ok(v) => {
            println!("Loading config from config.json.");
            Arc::new(serde_json::from_slice::<Config>(&v).unwrap())
        }
        Err(e) => {
            println!("Cannot find config.json: {:?}", e);
            println!("Using default config values.");
            Arc::new(Config::default())
        }
    };
    let client = NetworkInstance::new(global_config.max_concurrent_requests);
    let list = match fs::read_to_string("./download_list.txt").await {
        Ok(v) => {
            println!("Loading download list from download_list.txt");
            v
        }
        Err(e) => {
            println!("Cannot read download.txt: {:?}", e);
            println!("Exiting...");
            return;
        }
    };
    let mut lines = list.lines();
    let mut file_path = "".to_string();
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
            let mut download_line = item.split_whitespace(); // read the line with space as separator
            let maybe_srr = &download_line.next().expect("Found first element")[3..];
            let (from, to) = match read_number(maybe_srr) {
                Ok(v) => v,
                Err(e) => {
                    println!("Failed to parse SRR number {}: {}", maybe_srr, e);
                    continue;
                }
            }; // get the number
            let arguments = download_line
                .map(|str| str.to_string())
                .collect::<Vec<String>>();
            if arguments.contains(&"preflight".into()) {
                println!("preflight not available for SRR numbers");
                continue;
            }
            let mut srr_list = Vec::from_iter(from..(to + 1));
            join_task!(srr in srr_list=>{
                let client = client.clone();
                let file_path = file_path.clone();
                let config = global_config.clone();
                async move {download(
                client,
                Url::from_str(&format!(
                    "https://www.be-md.ncbi.nlm.nih.gov/Traces/sra-reads-be/fastq?acc=SRR{}",
                    srr
                ))
                .unwrap(),
                config.as_ref(),
                file_path,
            ).await.map(|_|(srr,()))}
            });
            continue;
        }
        // When the line is started with SRX
        if item[..3] == "SRX".to_string() {
            let mut item_line = item.split(" ");
            let srx = item_line.next().unwrap();
            let (from, to) = match read_number(srx) {
                Ok(v) => v,
                Err(e) => {
                    println!("Error when reading SRX number {} : {:?}", srx, e);
                    continue;
                }
            };
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
                let mut srx_list = Vec::from_iter(from..(to + 1));
                join_task!(srx in srx_list=>{
                    let client = client.clone();
                    let global_config = global_config.clone();
                    let arguments = arguments.clone();
                    async move{
                        crate::ncbi::preflight_srx(client,srx,global_config,arguments).await.map(|_|(srx,()))
                    }
                });
                continue;
            };
            let mut srx_list = Vec::from_iter(from..(to + 1));
            join_task!(srx in srx_list=>{
                let client = client.clone();
                let file_path = Arc::new(file_path.clone());
                let arguments = arguments.clone();
                let config = global_config.clone();
                async move{
                    let (id, srr) = match read_name_and_srr(
                    client.clone(),
                    srx,
                    config.as_ref(),
                    arguments.as_ref(),
                )
                .await
                {
                    Ok(v) => v,
                    Err(_) => {
                        println!("max retry times reached for SRX {}", srx);
                        return Err(());
                    }
                };
                let file_path = format!("{}/{}.fastq.gz", file_path, id);
               download(
                    client.clone(),
                    Url::from_str(&format!(
                        "https://www.be-md.ncbi.nlm.nih.gov/Traces/sra-reads-be/fastq?acc={}",
                        srr
                    ))
                    .unwrap(),
                    config.as_ref(),
                    file_path,
                ).await.map(|_|(srx,()))
                }
            });
            continue;
        }
        if item[..3] == "CRA".to_string() {
            let mut item_line = item.split_whitespace();
            let cra_number: usize = item_line.next().unwrap()[3..].parse().unwrap();
            let (from, to) = match read_number(item_line.next().unwrap()) {
                Ok(v) => v,
                Err(e) => {
                    println!("{}", e);
                    continue;
                }
            };
            let arguments = Arc::new(
                item_line
                    .map(|str| str.to_string())
                    .collect::<Vec<String>>(),
            );
            if arguments.contains(&"preflight".into()) {
                let mut crx_list = Vec::from_iter(from..(to + 1));
                join_task!(crx in crx_list=>{
                        let client = client.clone();
                        let config = global_config.clone();
                        crate::cnbi::preflight(client,cra_number,crx,config)
                });
                continue;
            }
            let mut crx_list = Vec::from_iter(from..(to + 1));
            join_task!(crx in crx_list=>{
                let client = client.clone();
                let file_path = file_path.clone();
                let config = global_config.clone();
                async move {
                    let (alias, mut filenames, crr) =
                        read_alias_and_crr_with_crx(
                            client.clone(), cra_number, crx, config.clone()
                        ).await.map_err(|_|())?;
                    join_task!(filename in filenames => {
                        let client = client.clone();
                        let config = config.clone();
                        let file_path = format!("{}/{}_{}", file_path, alias, filename);
                        async move{
                            download(
                                client.clone(),
                                Url::from_str(&format!(
                                    "https://download.cncb.ac.cn/gsa/CRA{:0>6}/CRR{}/{}",
                                    cra_number, crr, filename
                                )).unwrap(),
                                config.as_ref(),
                                file_path
                            ).await.map(|_|(filename.clone(),()))
                        }
                    });
                    Ok::<(usize,()),()>((crx,()))
                }
            });
        }
        // When the line is only for configuration
        let mut config_line = item.split_whitespace();
        let maybe_file_path = config_line.next().unwrap().to_owned().replace("\\", "/");
        match fs::create_dir_all(maybe_file_path.clone()).await {
            Ok(_) => file_path = maybe_file_path,
            Err(e) => {
                println!(
                    "Cannot use {} as working directory: {:?}",
                    maybe_file_path, e
                );
                println!("Exiting...");
                return;
            }
        };
    }
    println!("All lines have been read");
}

/// To initate a download, the file path to write to and the SRR number is needed.
async fn download(
    client: NetworkInstance,
    url: Url,
    config: &Config,
    file_path: String,
) -> Result<(), ()> {
    let bytes = crate::with_retry!(
        config.retry_times =>
        client.get(url.clone(),config.download_timeout)
    )?;
    if let Err(e) = fs::write(&file_path, bytes).await {
        println!("Failed to write to {}: {:?}", file_path, e);
        Err(())
    } else {
        println!("{} finished and written to {}", url.to_string(), file_path);
        Ok(())
    }
}

/// Read SRX from syntax `SRX123456-789000`.
/// The number can be supplied with decending order.
fn read_number(srx: &str) -> Result<(usize, usize), ParseNumberError> {
    if srx.contains("-") {
        // When SRX is supplied in a range, inclusive
        let mut split = srx[3..].split("-");
        let maybe_num1 = split.next();
        if let None = maybe_num1 {
            return Err(ParseNumberError::IncompleteRange);
        };
        let num1 = maybe_num1
            .unwrap()
            .parse()
            .map_err(|_| ParseNumberError::IllegalCharacter)?;
        let maybe_num2 = split.next();
        if let None = maybe_num2 {
            return Err(ParseNumberError::IncompleteRange);
        };
        let num2 = maybe_num2
            .unwrap()
            .parse()
            .map_err(|_| ParseNumberError::IllegalCharacter)?;
        // The order of the SRX number is automatically determined
        if num1 > num2 {
            Ok((num2, num1))
        } else {
            Ok((num1, num2))
        }
    } else {
        // Single srx
        let num = srx[3..].parse().unwrap();
        Ok((num, num))
    }
}

#[derive(Debug)]
enum ParseNumberError {
    IllegalCharacter,
    IncompleteRange,
}
impl std::fmt::Display for ParseNumberError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IllegalCharacter => f.write_str("Illegal character found"),
            Self::IncompleteRange => f.write_str("Incomplete range"),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Config {
    pub retry_times: usize,
    pub read_meta_timeout: usize,
    pub max_concurrent_requests: usize,
    pub download_timeout:usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            retry_times: 5,
            read_meta_timeout: 60,
            max_concurrent_requests: 3,
            download_timeout: 600,
        }
    }
}
pub enum Parameters {
    SRRNameOverride,
    LibraryNameOverride,
}
impl TryFrom<&str> for Parameters {
    type Error = ();
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "srr_name_override" => Ok(Self::SRRNameOverride),
            "library_name_override" => Ok(Self::LibraryNameOverride),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NetworkInstance {
    semaphore: Arc<Semaphore>,
    client: Client,
}
impl NetworkInstance {
    pub fn new(max_concurrent_requests: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent_requests)),
            client: reqwest::Client::new(),
        }
    }
    /// Perform a get request to the given url.
    /// Panic when the url is malformed.
    pub async fn get_by_str(&self, url: String, timeout: usize) -> Result<Bytes, GetReqError> {
        let url = Url::from_str(&url).unwrap();
        self.get(url, timeout).await
    }
    /// Perform a get request to the given url.
    pub async fn get(&self, url: Url, timeout: usize) -> Result<Bytes, GetReqError> {
        let permit = self
            .semaphore
            .acquire()
            .await
            .expect("Semaphore to be open");
        let request = Request::new(Method::GET, url.clone());
        use futures_timer::Delay;
        use std::time::Duration;
        use tokio::select;
        let timeout =
            Duration::from_secs(timeout.try_into().expect("on machine with 64-bit or less"));
        let mut timer = Delay::new(timeout);
        let response = loop {
            select! {
                maybe_response = self.client.execute(request) =>{
                    break maybe_response
                }
                _ = &mut timer =>{
                    return Err(GetReqError::Timeout);
                }
            }
        }?;
        timer.reset(timeout);
        let bytes = loop {
            select! {
                maybe_bytes = response.bytes() =>{
                    break maybe_bytes
                }
                _ = &mut timer =>{
                    return Err(GetReqError::Timeout);
                }
            }
        }?;
        drop(permit);
        Ok(bytes)
    }
}

pub enum GetReqError {
    Timeout,
    Other(reqwest::Error),
}
impl From<reqwest::Error> for GetReqError {
    fn from(value: reqwest::Error) -> Self {
        Self::Other(value)
    }
}
