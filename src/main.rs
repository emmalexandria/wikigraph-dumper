mod files;
mod output;

extern crate rusqlite;

use console::style;
use core::{num, panic};
use dialoguer::theme::ColorfulTheme;
use dialoguer::Input;
use files::{check_xml_files, delete_temp_db_files, delete_temp_xml_files, generate_xml_file_names};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use output::{print_error, print_info, print_success, print_warning};
use parse_wiki_text_2::{Configuration, ConfigurationSource, Node};
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use rusqlite::{Connection, DatabaseName};
use std::ffi::OsStr;
use std::fs::OpenOptions;
use std::io::{BufWriter, ErrorKind, Seek, SeekFrom, Write};
use std::os::windows::fs::MetadataExt;
use std::path::Path;
use std::time::Duration;
use std::{env, thread};
use std::{
    fs::{self, File},
    io::Read,
};

use crate::output::print_header;

const WIKIPEDIA_CONFIG: ConfigurationSource = ConfigurationSource {
    category_namespaces: &["category"],
    extension_tags: &[
        "categorytree",
        "ce",
        "charinsert",
        "chem",
        "gallery",
        "graph",
        "hiero",
        "imagemap",
        "indicator",
        "inputbox",
        "langconvert",
        "mapframe",
        "maplink",
        "math",
        "nowiki",
        "phonos",
        "poem",
        "pre",
        "ref",
        "references",
        "score",
        "section",
        "source",
        "syntaxhighlight",
        "templatedata",
        "templatestyles",
        "timeline",
    ],
    file_namespaces: &["file", "image"],
    link_trail: "abcdefghijklmnopqrstuvwxyz",
    magic_words: &[
        "archivedtalk",
        "disambig",
        "expected_unconnected_page",
        "expectunusedcategory",
        "forcetoc",
        "hiddencat",
        "index",
        "newsectionlink",
        "nocc",
        "nocontentconvert",
        "noeditsection",
        "nogallery",
        "noglobal",
        "noindex",
        "nonewsectionlink",
        "notalk",
        "notc",
        "notitleconvert",
        "notoc",
        "staticredirect",
        "toc",
    ],
    protocols: &[
        "//",
        "bitcoin:",
        "ftp://",
        "ftps://",
        "geo:",
        "git://",
        "gopher://",
        "http://",
        "https://",
        "irc://",
        "ircs://",
        "magnet:",
        "mailto:",
        "matrix:",
        "mms://",
        "news:",
        "nntp://",
        "redis://",
        "sftp://",
        "sip:",
        "sips:",
        "sms:",
        "ssh://",
        "svn://",
        "tel:",
        "telnet://",
        "urn:",
        "worldwind://",
        "xmpp:",
    ],
    redirect_magic_words: &["redirect"],
};

const DISALLOWED_PREFIXES: [&str; 5] = ["Wikipedia:", "Draft:", "Template:", "Category:", "File:"];
const TEMP_PATH: &str = "temp/";

fn main() {
    let args: Vec<String> = env::args().collect();

    let wikifile = process_args(args);

   let threads_str: String = Input::with_theme(&ColorfulTheme::default())
        .with_prompt("Number of parsing threads")
        .validate_with(|input: &String| -> Result<(), &str> {
            match input.parse::<usize>() {
                Ok(_) => Ok(()),
                Err(_) => Err("Please enter an integer"),
            }
        })
        .interact_text()
        .unwrap(); 

    let num_threads = threads_str.parse::<usize>().unwrap();

    let files = init_fs(wikifile, num_threads);
    let db_files = spawn_threads(files, num_threads);

    match db_merge(db_files) {
        Ok(_) => print_success("DB files successfully merged"),
        Err(e) => print_error(&format!("Failed to merge DB with error {}", e)),
    }

    std::process::exit(0);
}

fn init_fs(input_file: String, num_threads: usize) -> Vec<String> {
    match fs::create_dir(TEMP_PATH) {
        Ok(_) => print_info("Temp directory created"),
        Err(e) => {
            if e.kind() != std::io::ErrorKind::AlreadyExists {
                print_error(&format!("Unknown error creating temp directory: {:?}", e));
                std::process::exit(-1)
            }
        }
    }

    match files::delete_temp_db_files(TEMP_PATH.to_string()) {
        Ok(_) => {},
        Err(e) => {
            if e.kind() != std::io::ErrorKind::NotFound {
                print_error(&format!("Error deleting db file: {}", e))
            }
        }
    }

    let xml_files = generate_xml_file_names(input_file.clone(), TEMP_PATH.to_string(), num_threads);
    match check_xml_files(xml_files.clone(), num_threads, TEMP_PATH.to_string()) {
        Ok(_) => {}
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound
                || e.kind() == std::io::ErrorKind::InvalidData
            {
                print_warning("Number of split XML files does not match thread number, deleting and regenerating");
                match delete_temp_xml_files(TEMP_PATH.to_string()) {
                    Ok(_) => {},
                    Err(e) => {
                        if e.kind() != std::io::ErrorKind::NotFound {
                            print_error("Could not delete temporary XML file");
                            std::process::exit(-1);
                        }
                    }
                }
                
            }
            else {
                print_error("Failed to validate xml subfiles");
                std::process::exit(-1);
            }
        }
    }
    
    return files::divide_file_at_boundary(input_file, "</page>", xml_files, num_threads);
}

fn process_args(args: Vec<String>) -> String {
    if args.len() < 2 {
        println!("{}", style("Please pass a Wikipedia XML dump").red());
        std::process::exit(-1);
    }

    return args[1].clone();
}

fn spawn_threads(files: Vec<String>, thread_num: usize) -> Vec<String> {
    let mut handles = Vec::with_capacity(10);

    let multi_progress = MultiProgress::new();
    let progress_style =
        ProgressStyle::with_template("{prefix:>9} [{wide_bar:.green}] {percent_precise}%")
            .unwrap()
            .progress_chars("#>-");

    print_header("Parsing");

    for i in 0..thread_num {
        let file = files[i].clone();
        let progress = multi_progress
            .add(ProgressBar::new(0))
            .with_style(progress_style.clone());

        handles.push(thread::spawn(move || -> String {
            return parsing_thread(file, i + 1, progress);
        }));
    }
    let mut db_files = Vec::<String>::new();

    for handle in handles {
        match handle.join() {
            Ok(d) => db_files.push(d),
            Err(e) => println!("Handle join error: {:?}", e),
        }
    }

    return db_files;
}

fn parsing_thread(file: String, num: usize, bar: ProgressBar) -> String {
    let file_size = File::open(file.clone())
        .unwrap()
        .metadata()
        .unwrap()
        .file_size();

    //initialise thread specific database
    let db_path = TEMP_PATH.to_string() + num.to_string().as_str() + ".db";
    let _ = fs::remove_file(&db_path);
    let mut conn = Connection::open(&db_path).unwrap();
    init_db(&mut conn);

    let mut reader = Reader::from_file(file.clone()).unwrap();
    reader.expand_empty_elements(true);

    bar.set_length(file_size);
    bar.set_prefix(format!("Thread {:?}", num));

    let mut in_text = false;
    let mut in_title = false;
    let mut skip_page = false;

    let mut current_page: String = String::new();
    let mut page_title = String::new();

    let mut buf = Vec::<u8>::with_capacity(1024);
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let name = e.name();
                let name = reader.decoder().decode(name.as_ref()).unwrap();

                match name.as_ref() {
                    "page" => {
                        current_page = String::from("");
                        page_title = String::from("");
                    }
                    "redirect" => skip_page = true,
                    "text" => in_text = true,
                    "title" => in_title = true,
                    &_ => {}
                }
            }
            Ok(Event::Text(e)) => {
                if !skip_page {
                    let escaped = &e.into_inner();
                    let text = reader.decoder().decode(&escaped).unwrap();
                    if in_text {
                        current_page.push_str(&text)
                    }
                    if in_title {
                        page_title.push_str(&text);
                    }
                }
            }
            Ok(Event::End(e)) => {
                let name = e.name();
                let name = reader.decoder().decode(name.as_ref()).unwrap();

                match name.as_ref() {
                    "page" => {
                        skip_page = false;
                    }
                    "text" => {
                        in_text = false;
                        if !skip_page {
                            parse_page(&current_page, &page_title, &mut conn);
                        }
                    }
                    "title" => {
                        in_title = false;
                        for prefix in DISALLOWED_PREFIXES {
                            if page_title.contains(prefix) {
                                skip_page = true;
                            }
                        }
                    }
                    &_ => {}
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => continue,
            Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
        }
        buf.clear();
        bar.set_position(reader.buffer_position() as u64);
    }
    bar.finish();
    return db_path;
}

fn init_db(conn: &mut Connection) {
    conn.pragma_update(Some(DatabaseName::Main), "journal_mode", "OFF")
        .unwrap();
    conn.pragma_update(Some(DatabaseName::Main), "synchronous", "OFF")
        .unwrap();

    conn.execute(
        "CREATE TABLE IF NOT EXISTS articles (
        Title VARCHAR PRIMARY KEY
       )
      ",
        (),
    )
    .unwrap();

    conn.execute(
        "CREATE TABLE IF NOT EXISTS article_links (
        Linker varchar,
        Linked varchar   
    )",
        (),
    )
    .unwrap();
}


fn parse_page(content: &String, title: &String, conn: &mut Connection) {
    let result = Configuration::new(&WIKIPEDIA_CONFIG).parse(&content, 5000);
    match result {
        Err(_) => {
            print_warning("Article was skipped due to being unparseable");
            match files::add_to_unparseables(title) {
                Ok(()) => {}
                Err(_) => print_error("Failed to add article to unparseable list"),
            }
            return;
        }
        Ok(v) => {
            let mut links = Vec::<String>::new();
            for node in v.nodes {
                if let Node::Link { target, .. } = node {
                    for prefix in DISALLOWED_PREFIXES {
                        if target.contains(prefix) {
                            return;
                        }
                    }
                    links.push(String::from(target));
                }
            }

            if links.len() > 0 {
                match push_page_to_db(conn, links, title) {
                    Ok(_) => {}
                    Err(_e) => println!("{}", style("Failed to push article to database").red()),
                }
            }
        }
    }
}

fn push_page_to_db(
    conn: &mut Connection,
    links: Vec<String>,
    title: &String,
) -> Result<(), rusqlite::Error> {
    let tx = conn.transaction()?;
    tx.execute(
        "INSERT INTO articles (Title) VALUES (?1)",
        (title.as_str(),),
    )?;
    {
        let mut stmt =
            tx.prepare_cached("INSERT INTO article_links (Linker, Linked) VALUES (?1, ?2)")?;

        for link in links {
            stmt.execute([&title.as_str(), &link.as_str()])?;
        }
    }

    tx.commit()?;

    Ok(())
}

//merges the thread specific db files into one larger db file
fn db_merge(db_files: Vec<String>) -> Result<(), rusqlite::Error> {
    print_header("Merging databases");

    let progress_style = ProgressStyle::with_template("{wide_msg} [{bar:.cyan}] {pos}/{len}%")
        .unwrap()
        .progress_chars("#>-");

    let pb = ProgressBar::new(db_files.len() as u64).with_style(progress_style);

    let mut merge_conn = Connection::open("wikigraph.db").unwrap();
    init_db(&mut merge_conn);

    for db_file in db_files {
        let attach_statement = format!("ATTACH '{}' as dba;", db_file);
        println!("{}", attach_statement);
        let tx = merge_conn.transaction()?;

        tx.execute(attach_statement.as_str(), [])?;

        pb.set_message(format!("Copying articles from {} to merged DB", &db_file));
        tx.execute("INSERT INTO articles SELECT * FROM dba.articles", [])?;

        pb.set_message(format!(
            "Copying article_links from {} to merged DB",
            &db_file
        ));
        tx.execute(
            "INSERT INTO article_links SELECT * FROM dba.article_links",
            [],
        )?;

        tx.commit()?;
        merge_conn.execute("DETACH DATABASE dba", ())?;
        match fs::remove_file(&db_file) {
            Ok(_) => {}
            Err(_e) => print_error(&format!("Failed to remove {}", db_file)),
        }

        pb.tick()
    }

    Ok(())
}

