use console::style;

pub fn print_error(output: &str) {
  eprintln!("{} {}", style("[ERROR]").red().bold(), style(output).red())  
}

pub fn print_info(output: &str) {
  println!("{} {}", style("[INFO]").dim().bold(), style(output).dim())  
}

pub fn print_warning(output: &str) {
  println!("{} {}", style("[WARN]").yellow().bold(), style(output).yellow())  
}

pub fn print_header(output: &str) {
  println!("{}", style(output).bold());
  println!("{}", "─".repeat(output.len()));
}

pub fn print_success(output: &str) {
  println!("{} {}", style("✔").green().bold(), style(output).green());
}