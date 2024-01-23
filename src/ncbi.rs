use std::sync::Arc;
use regex::Regex;

use crate::NetworkInstance;

/// This function does not spawn additional task.
/// This function will retry network requests.
pub async fn read_name_and_srr(
    client: NetworkInstance,
    srx_number: usize,
    config: &crate::Config,
    arguments: &Vec<String>,
) -> Result<(String, String), ()> {
    let result = crate::with_retry!(
        config.retry_times =>
            client.get_by_str(format!("https://www.ncbi.nlm.nih.gov/sra/SRX{}[accn]",srx_number),config.read_meta_timeout)
    );
    if let Err(_) = result {
        return Err(());
    }
    let bytes = result.unwrap();
    let document = String::from_utf8_lossy(&bytes[31220..(bytes.len()-18300)]).to_string();
    drop(bytes);
    let srr_pos = document
        .find("//trace.ncbi.nlm.nih.gov/Traces?run=SRR")
        .unwrap();
    let mut srr_number = document[(srr_pos+35)..(srr_pos+48)].to_owned();
    srr_number.retain(|char| char.is_alphanumeric());
    let search_regex = Regex::new(">[^<>]*<").unwrap();
    let filename = if arguments.contains(&"srr_override".into()) {
        srr_number.clone()
    } else if arguments.contains(&"library_name_override".into()) {
        let pos = document
            .find("Library: <div class=\"expand-body\"><div>Name: <span>")
            .unwrap();
        let name_search_result = search_regex.find(&document[pos + 48..pos + 75]);
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
        let matched = search_regex.find(&document[(pos+10)..(pos + 100)]).unwrap().as_str();
        matched[1..(matched.len()-1)].to_owned()
    };
    Ok((filename, srr_number))
}

enum ReadSrrError {
    RetryTimesReached,
    NotFound,
}

pub async fn preflight_srx(
    client: NetworkInstance,
    srx: usize,
    config: Arc<crate::Config>,
    arguments: Arc<Vec<String>>,
) -> Result<(String, String), ()> {
    let result = crate::with_retry!(
        config.retry_times =>
        read_name_and_srr(client.clone(), srx, config.as_ref(), arguments.as_ref())
    );
    if let Ok(v) = &result{
        println!("Found {} with {}",v.0, v.1);
    }
    result
}
