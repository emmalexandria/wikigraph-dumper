use std::{ffi::OsStr, fs::{self, File, OpenOptions}, io::{BufWriter, ErrorKind, Read, Seek, SeekFrom, Write}, os::windows::fs::MetadataExt, path::Path, time::Duration};

use indicatif::{ProgressBar, ProgressStyle};

use crate::output::print_info;

pub fn check_xml_files(
    xml_files: Vec<String>,
    num_threads: usize,
    temp_path: String,
) -> Result<(), std::io::Error> {
    let temp_dir = fs::read_dir(temp_path)?;
    let mut num_xml = 0;
    

    for entry in temp_dir {
        let path = entry?.path();
        if path.extension() == Some(OsStr::new("xml")) {
            num_xml += 1;
            let file_name = path.file_name().unwrap().to_str().unwrap();
            if !xml_files.contains(&file_name.to_string()) {
                return Err(std::io::Error::new(
                    ErrorKind::InvalidData,
                    "Additional XML file in temp directory",
                ));
            }
        }

        if num_xml != num_threads {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "Incorrect number of XML files for number of threads",
            ));
        }
    }

    Ok(())
}

pub fn delete_temp_xml_files(temp_dir: String) -> Result<(), std::io::Error> { 
    for path in fs::read_dir(temp_dir)? {
      let entry = path?;
      if entry.metadata()?.is_file() {
        if Path::new(entry.file_name().to_str().unwrap()).extension() == Some(OsStr::new("xml")) {
          fs::remove_file(entry.path())?;
        }
      }
    }
    return Ok(());
}

pub fn delete_temp_db_files(temp_dir: String) -> Result<(), std::io::Error> { 
  print_info("Deleting any temporary db files that exist");
  for path in fs::read_dir(temp_dir)? {
    let entry = path?;
    if entry.metadata()?.is_file() {
      if Path::new(entry.file_name().to_str().unwrap()).extension() == Some(OsStr::new("db")) {
        fs::remove_file(entry.path())?;
      }
    }
  }
  return Ok(());
}


pub fn generate_xml_file_names(base_file: String, temp_path: String, num_threads: usize) -> Vec<String> {
  let mut out_xml_files = Vec::<String>::new();

  for i in 0..num_threads {
      let path = temp_path.clone()
          + base_file.clone().trim_end_matches(".xml")
          + "-split"
          + (i + 1).to_string().as_str()
          + ".xml";
      out_xml_files.push(path.clone());
  }

  return out_xml_files;
}

pub fn divide_file_at_boundary(
  file: String,
  boundary: &str,
  out_files: Vec<String>,
  num_threads: usize,
) -> Vec<String> {

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
  let split_size = file_size / num_threads as u64;

  let mut last_split: u64 = 0;
  for i in 0..num_threads {
      let out_file = File::create(out_files[i as usize].clone()).unwrap();
      let mut writer = BufWriter::new(out_file);
      let mut read_file = in_file.try_clone().unwrap();

      pb.set_message(format!("Splitting {} to {}", &file, out_files[i as usize]));
      pb.set_position(i as u64 + 1);

      if i == num_threads - 1 {
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

//adds an article to the outputted list of 'unparseables' (articles that the parser took more than max_ms to parse)
pub fn add_to_unparseables(title: &String) -> Result<(), Box<dyn std::error::Error>> {
  let mut f = OpenOptions::new()
      .write(true)
      .append(true)
      .open("unparseables.txt")?;

  f.seek(SeekFrom::End(0))?;
  f.write(b"\n")?;
  f.write(title.as_bytes())?;

  Ok(())
}
