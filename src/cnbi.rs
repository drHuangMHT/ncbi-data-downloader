use reqwest::Url;
use std::{str::FromStr, sync::Arc};

use crate::{Config, NetworkInstance};

pub async fn read_alias_and_crr_with_crx(
    client: NetworkInstance,
    cra: usize,
    crx: usize,
    config: Arc<Config>,
) -> Result<(String, Vec<String>, usize), ()> {
    let result = crate::with_retry!(
        config.retry_times =>
            client.get(Url::from_str(&format!(
                "https://ngdc.cncb.ac.cn/gsa/browse/CRA{:0>6}/CRX{:0>6}",
                cra, crx
            )).unwrap(),config.read_meta_timeout
    ));
    if let Err(_) = result {
        return Err(());
    }
    let bytes = result.unwrap();
    let document = String::from_utf8_lossy(&bytes[2000..]).to_string();
    drop(bytes);
    let document = document[..document.find(r#"<div class="col-md-3">"#).unwrap()].to_string();
    let crr_number = {
        let crr_pos = document
            .find(&format!(r#"<td><a href="browse/CRA{:0>6}/"#, cra))
            .unwrap();
        let mut crr_number = document[(crr_pos + 30)..(crr_pos + 40)].to_owned();
        crr_number.retain(|char| char.is_numeric());
        crr_number.parse().expect("crr to be number")
    }; // CRR read, proceed to get alias

    let result = crate::with_retry!(
        config.retry_times =>
            client.get(Url::from_str(&format!(
                "https://ngdc.cncb.ac.cn/gsa/browse/CRA{:0>6}/CRR{:0>6}",
                cra, crr_number
            )).unwrap(),config.read_meta_timeout
    ));
    if let Err(_) = result {
        return Err(());
    }
    let bytes = result.unwrap();
    let document = String::from_utf8_lossy(&bytes[6600..(bytes.len()-100)]).to_string();
    drop(bytes);
    let (alias, filename) = {
        let alias_regex = regex::Regex::new(&format!(
            "<td>CRR{:0>6}</td>[^a-z]+?<td>.*</td>",
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

pub async fn preflight(
    client: NetworkInstance,
    cra: usize,
    crx: usize,
    config: Arc<Config>,
) -> Result<(usize, ()), ()> {
    match read_alias_and_crr_with_crx(client.clone(), cra, crx, config).await {
        Ok((alias, filenames, crr)) => {
            println!(
                "Found CRR{} with files: {:?}; alias: {}",
                crr, filenames, alias
            );
            Ok((crx, ()))
        }
        Err(_) => {
            println!(
                "Preflight for CRA{}/CRX{} failed: Maximum retry times reached",
                cra, crx
            );
            Err(())
        }
    }
}
