use std::cmp::Ordering::Equal;
use std::net::IpAddr;
use std::process::exit;
use base64::encode;
use clap::{Arg, Command, value_parser};
use rand::prelude::*;
use serde::Deserialize;
use reqwest::{Client};
use scraper::{Html, Selector};
use prettytable::{Table, Row, Cell, row};
use rpassword::read_password;

#[derive(Deserialize)]
struct Res {
    msg: String,
    code: i8,
    data: Vec<Vec<String>>,
    count: i32,
}

struct JobInfo {
    job_type: String,
    job_label: String,
    job_limit: String,
    current_consumption_bytes: f64,
    peak_consumption_bytes: f64,
    be_ip: String,
}


async fn get_vec(url: &str, be_ip: &str) -> Vec<JobInfo> {
    // let html_content = include_str!("test.html");
    let html_content = reqwest::get(url).await.expect("error ").text().await.unwrap();
    let document = Html::parse_document(&*html_content);
    let selector = Selector::parse("table  tbody tr").unwrap();
    let mut job_info_all = Vec::new();
    // 遍历每一行
    for element in document.select(&selector) {
        let type_selector = Selector::parse("td").unwrap();
        let tds: Vec<_> = element.select(&type_selector).collect();

        if let Some(type_td) = tds.get(0) {
            let type_text = type_td.text().collect::<String>().trim().to_string();
            if !type_text.is_empty() { // 确保 Type 列有值
                // 打印其他信息，根据需要调整索引
                if tds.len() > 6 {
                    let query_id = tds[1].text().collect::<String>();
                    let query_limit = tds[3].text().collect::<String>();
                    let bytes_used = tds[4].text().collect::<String>();
                    let peak_bytes_used = tds[6].text().collect::<String>();
                    let bytes_used_f64 = bytes_used.clone().trim().parse::<f64>().unwrap();
                    let peak_bytes_f64 = peak_bytes_used.clone().trim().parse::<f64>().unwrap();
                    let job_info = JobInfo {
                        job_type: type_text,
                        job_label: query_id,
                        job_limit: query_limit,
                        current_consumption_bytes: bytes_used_f64,
                        peak_consumption_bytes: peak_bytes_f64,
                        be_ip: be_ip.to_string(),
                    };
                    job_info_all.push(job_info);
                }
            }
        }
    }
    job_info_all
}


async fn crawler(url: &str, be_ip: &str) -> Vec<JobInfo> {
    let load_url_format = format!("{}?type=load", &url);
    let query_url_format = format!("{}?type=query", &url);
    let load_url = load_url_format.trim();
    let query_url = query_url_format.trim();
    let mut job_info_all = Vec::new();
    let mut job_info_all_load = get_vec(load_url, be_ip).await;
    let mut job_info_all_query = get_vec(query_url, be_ip).await;
    job_info_all.append(&mut job_info_all_query);
    job_info_all.append(&mut job_info_all_load);
    job_info_all
}

#[tokio::main]
async fn main() {
    let mut num_top = 1000;
    let mut fe_port = 8030;
    let mut fe_hosts_input = Vec::new();
    let matches = Command::new("doris_resource_topN")
        .version("1.0")
        .author("zhangweiwhim@gmail.com")
        .about("To find doris current use mem top n")
        .arg(Arg::new("num")
            .long("num")
            .help("Enter an integer number to process.")
            .takes_value(true)
            .required(false)  // 可以不是必需的，根据你的需求调整
            .value_name("INT")
            .value_parser(value_parser!(i32))
        )
        .arg(Arg::new("fe_port")
            .long("fe_port")
            .help("Enter fe http port")
            .takes_value(true)
            .required(false)  // 可以不是必需的，根据你的需求调整
            .value_name("INT")
            .value_parser(value_parser!(i32))
        )
        .arg(Arg::new("fe_host")
            .long("fe_host")
            .help("fe hosts that a comma-separated list of values")
            .takes_value(true)
            .required(true)
            .value_name("LIST")
        )
        .get_matches();


    if let Some(values) = matches.get_one::<String>("fe_host") {
        if values.len() < 1 {
            println!("Need at least one fe host");
            exit(1);
        }
        let items: Vec<&str> = values.split(',').collect();
        fe_hosts_input = items;
    }

    if let Some(num_input) = matches.get_one::<i32>("num") {
        println!("Getting current doris mem usage top {} job", num_input);
        num_top = num_input.clone();
    } else {
        println!("No num provided. Use --help for more information.");
        exit(1);
    }

    if let Some(port) = matches.get_one::<i32>("fe_port") {
        println!("Getting fe http port is {} ", port);
        fe_port = port.clone();
    } else {
        println!("No fe http port provided. Use --help for more information.");
        exit(1);
    }

    println!("Please enter your password:");
    let password = read_password().unwrap();

    let fe_hosts = fe_hosts_input;
    let mut rng = thread_rng();
    let random_element = fe_hosts.choose(&mut rng);
    let mut fe_ip = "";
    match random_element {
        Some(&value) => fe_ip = &value,
        None => exit(0)
    }

    let url = format!("http://{}:{}/api/show_proc/?path=//backends", fe_ip, fe_port);
    let auth_header = encode(format!("{}:{}", "root", &password).as_bytes());
    let client = Client::new();
    let response = client.get(url)
        .header("User-Agent", "MERONG(0.9/;p)")
        .header("Accept", "*/*")
        .header("Authorization", format!("Basic {}", auth_header))
        .send().await;

    let res: Res = response.expect("has error").json::<Res>().await.unwrap();
    let mut job_info_all: Vec<JobInfo> = Vec::new();
    let mut col_tmp = 2;
    for be in res.data {
        for (index, item) in be.iter().enumerate() {
            if item.parse::<IpAddr>().is_ok() {
                col_tmp = index;
                break
            }
        }
        let be_ip = be.get(col_tmp).unwrap().clone();
        let be_url = format!("http://{}:8040/mem_tracker", be_ip);
        let mut data_map = crawler(&be_url, &be_ip).await;
        job_info_all.append(&mut data_map);
    }

    job_info_all.sort_by(|a, b| b.current_consumption_bytes.partial_cmp(&a.current_consumption_bytes).unwrap_or(Equal));

    let mut table = Table::new();
    table.add_row(row!["No","job_label", "current_consumption_gb","job_limit","job_type","peak_consumption_gb","be_ip"]);

    let mut num = 1;
    for key in job_info_all.iter() {
        table.add_row(Row::new(vec![
            Cell::new((num).to_string().trim()),
            Cell::new(key.job_label.trim()),
            Cell::new((key.current_consumption_bytes / 1024.0 / 1024.0 / 1024.0).to_string().trim()),
            Cell::new(key.job_limit.to_string().trim()),
            Cell::new(key.job_type.to_string().trim()),
            Cell::new((key.peak_consumption_bytes / 1024.0 / 1024.0 / 1024.0).to_string().trim()),
            Cell::new(&key.be_ip),
        ]));
        num = num + 1;
        if num == num_top + 1 {
            break;
        }
    }
    table.printstd();
}