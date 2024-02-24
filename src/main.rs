mod output;

extern crate rusqlite;

use colored::Colorize;
use console::style;
use output::{print_error, print_info, print_success, print_warning};
use core::panic;
use dialoguer::theme::ColorfulTheme;
use dialoguer::Input;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use parse_wiki_text_2::{Configuration, ConfigurationSource, Node};
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use rusqlite::{Connection, DatabaseName};
use std::fs::OpenOptions;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::os::windows::fs::MetadataExt;
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

    match fs::create_dir(TEMP_PATH) {
        Ok(_) => print_info("Temp directory created"),
        Err(e) => {
            if !(e.kind() == std::io::ErrorKind::AlreadyExists) {
                panic!("Unknown error creating temp directory: {:?}", e);
            }
        }
    }

    let files = divide_file_at_boundary(
        wikifile,
        "</page>",
        num_threads.try_into().unwrap(),
        num_threads,
    );

    let db_files = spawn_threads(files, num_threads);

    match db_merge(db_files) {
        Ok(_) => print_success("DB files successfully merged"), 
        Err(e) => print_error(&format!("Failed to merge DB with error {}", e))
    }

    std::process::exit(0);
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

fn divide_file_at_boundary(
    file: String,
    boundary: &str,
    num: u64,
    num_threads: usize,
) -> Vec<String> {
    let (files_exist, out_files) = get_subfiles(file.clone(), num_threads);

    //if not all the files exist, delete any which do and regenerate them
    if files_exist {
        print_info("All subfiles already present, proceeding to parse");
        return out_files;
    } else {
        print_info("Number of subfiles is incorrect, regenerating");
        for i in 0..num_threads {
            fs::remove_file(&out_files[i]);
        }
    }

    let pb = ProgressBar::new_spinner();
    pb.set_length(num_threads as u64);
    pb.set_style(
        ProgressStyle::with_template("{spinner:.green} {msg} ({pos}/{len})")
            .unwrap()
            .tick_strings(&["-", "\\", "|", "/"]),
    );
    pb.enable_steady_tick(Duration::from_millis(120));

    let in_file = File::open(&file).unwrap();
    let file_size = in_file.metadata().unwrap().file_size();

    //each split file should ideally be this size
    let split_size = file_size / num;

    let mut last_split: u64 = 0;
    for i in 0..num {
        pb.set_message(format!("Splitting {} to {}", &file, out_files[i as usize]));
        pb.set_position(i + 1);
        let out_file = File::create(out_files[i as usize].clone()).unwrap();
        let mut writer = BufWriter::new(out_file);
        let mut read_file = in_file.try_clone().unwrap();

        if i == num - 1 {
            read_file.seek(SeekFrom::Start(last_split));

            let mut take = read_file.take(file_size - last_split);
            std::io::copy(&mut take, &mut writer);

            writer.flush();
            continue;
        }

        let pos = SeekFrom::Start(last_split + split_size);
        read_file.seek(pos);

        let iter_bytes: usize = 1024;
        let mut buf = Vec::<u8>::new();
        buf.resize(1024, 0);
        loop {
            read_file.read_exact(&mut buf);

            let buf_string = String::from_utf8(buf.clone());
            match buf_string {
                Ok(v) => {
                    if v.contains(boundary) {
                        break;
                    }
                }
                Err(_) => continue,
            }
            read_file.seek(SeekFrom::Current(iter_bytes.try_into().unwrap()));
        }

        //get offset at end of </page> tag
        let buf_string = String::from_utf8(buf).unwrap();

        let string_offset = buf_string.find(boundary).unwrap() + boundary.len();
        //add to offset within file

        //offset is calculated from the start of the buffer
        let offset =
            (read_file.stream_position().unwrap() - iter_bytes as u64) + string_offset as u64;

        //seek back to start in order to read file
        read_file.seek(SeekFrom::Start(last_split));
        let mut take = read_file.try_clone().unwrap().take(offset - last_split);
        std::io::copy(&mut take, &mut writer);
        writer.flush();

        read_file.rewind().unwrap();
        last_split = offset;
    }
    pb.finish();
    return out_files;
}

fn get_subfiles(input_file: String, num_threads: usize) -> (bool, Vec<String>) {
    let mut out_files = Vec::<String>::with_capacity(num_threads);
    let mut files_exist = true;

    for i in 0..num_threads {
        let path = TEMP_PATH.to_string()
            + input_file.clone().trim_end_matches(".xml")
            + "-split"
            + (i + 1).to_string().as_str()
            + ".xml";
        out_files.push(path.clone());
        match File::open(path) {
            Ok(_) => {}
            Err(_) => files_exist = false,
        }
    }

    if files_exist {
        return (true, out_files);
    }

    return (false, out_files);
}

fn parse_page(content: &String, title: &String, conn: &mut Connection) {
    let result = Configuration::new(&WIKIPEDIA_CONFIG).parse(&content, 5000);
    match result {
        Err(_) => {
            print_warning("Article was skipped due to being unparseable");
            match add_to_unparseables(title) {
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
                    Err(E) => println!("{}", style("Failed to push article to database").red()),
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

    let progress_style =
        ProgressStyle::with_template("{wide_msg} [{bar:.cyan}] {pos}/{len}%")
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
        tx.execute("INSERT INTO articles SELECT * FROM dba.articles", [])
            ?;

        pb.set_message(format!("Copying article_links from {} to merged DB", &db_file));
        tx.execute(
            "INSERT INTO article_links SELECT * FROM dba.article_links",
            [],
        )?;

        tx.commit()?;
        merge_conn.execute("DETACH DATABASE dba", ())?;
        match fs::remove_file(&db_file) {
            Ok(_) => {},
            Err(e) => print_error(&format!("Failed to remove {}", db_file)),
        }

        pb.tick()
    }

    Ok(())
}

//adds an article to the outputted list of 'unparseables' (articles that the parser took more than max_ms to parse)
fn add_to_unparseables(title: &String) -> Result<(), Box<dyn std::error::Error>> {
    let mut f = OpenOptions::new()
        .write(true)
        .append(true)
        .open("unparseables.txt")?;

    f.seek(SeekFrom::End(0))?;
    f.write(b"\n")?;
    f.write(title.as_bytes())?;

    Ok(())
}
