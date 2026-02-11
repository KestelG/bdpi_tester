use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{BufRead, BufReader, Write, stdin};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::Local;
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::time;

#[derive(Debug, Deserialize, Clone)]
struct Settings {
    group_size: usize,
    start_port: u16,
    group_delay_ms: u64,
    request_timeout_sec: u64,
    log_dir: String,
    results_file: String,
    ciadpi_start_delay_ms: u64,
}

#[derive(Debug, Clone)]
struct TestResult {
    config: String,
    socks5_port: u16,
    successful_domains: Vec<String>,
    failed_domains: Vec<String>,
    success_rate: f32,
}

impl TestResult {
    fn new(config: String, socks5_port: u16, successful: Vec<String>, failed: Vec<String>) -> Self {
        let total = successful.len() + failed.len();
        let success_rate = if total > 0 {
            (successful.len() as f32 / total as f32) * 100.0
        } else {
            0.0
        };

        Self {
            config,
            socks5_port,
            successful_domains: successful,
            failed_domains: failed,
            success_rate,
        }
    }
}

struct GroupStats {
    successful: usize,
    total: usize,
}

impl GroupStats {
    fn success_rate(&self) -> f32 {
        if self.total > 0 {
            (self.successful as f32 / self.total as f32) * 100.0
        } else {
            0.0
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    show_welcome_message();
    wait_for_start();

    let settings = load_settings()?;
    let configs = read_lines("configs.txt")?;
    let domains = read_lines("domains.txt")?;

    display_startup_info(&settings, &configs, &domains);
    confirm_start()?;

    let session_dir = create_session_directory(&settings.log_dir)?;
    let results = Arc::new(Mutex::new(Vec::new()));

    let group_stats = run_all_groups(&configs, &domains, &settings, &session_dir, results.clone()).await?;

    finalize_results(results, &settings, &group_stats, &session_dir).await?;
    wait_for_quit();

    Ok(())
}

fn load_settings() -> Result<Settings, Box<dyn std::error::Error + Send + Sync>> {
    print_status("[+]", "Загружаем настройки из settings.toml...");
    let content = std::fs::read_to_string("settings.toml")
        .map_err(|e| format!("Failed to read settings.toml: {}", e))?;
    
    toml::from_str(&content)
        .map_err(|e| format!("Failed to parse settings.toml: {}", e).into())
}

fn read_lines(filename: &str) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let file = File::open(filename)
        .map_err(|e| format!("Failed to open {}: {}", filename, e))?;
    
    BufReader::new(file)
        .lines()
        .map(|line| line.map(|l| l.trim().to_string()))
        .filter(|line| line.as_ref().map(|l| !l.is_empty()).unwrap_or(false))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Failed to read lines from {}: {}", filename, e).into())
}

fn create_session_directory(log_dir: &str) -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    let timestamp = Local::now().format("%Y-%m-%d_%H-%M-%S");
    let session_dir = PathBuf::from(log_dir).join(timestamp.to_string());
    create_dir_all(&session_dir)?;
    Ok(session_dir)
}

async fn run_all_groups(
    configs: &[String],
    domains: &[String],
    settings: &Settings,
    session_dir: &Path,
    results: Arc<Mutex<Vec<TestResult>>>,
) -> Result<Vec<GroupStats>, Box<dyn std::error::Error + Send + Sync>> {
    let total_groups = (configs.len() + settings.group_size - 1) / settings.group_size;
    let mut group_stats = Vec::with_capacity(total_groups);

    for (group_idx, chunk) in configs.chunks(settings.group_size).enumerate() {
        let group_number = group_idx + 1;
        
        print_group_header(group_number, total_groups, chunk.len(), settings.start_port);
        
        let group_dir = session_dir.join(format!("group_{}", group_number));
        create_dir_all(&group_dir)?;

        let stats = process_group(
            chunk,
            domains,
            settings,
            &group_dir,
            results.clone(),
        ).await?;

        print_group_summary(group_number, &stats);
        group_stats.push(stats);

        save_intermediate_results(&results, &settings.results_file).await?;

        if group_number < total_groups {
            wait_between_groups(settings.group_delay_ms).await;
        }
    }

    Ok(group_stats)
}

async fn process_group(
    configs: &[String],
    domains: &[String],
    settings: &Settings,
    group_dir: &Path,
    results: Arc<Mutex<Vec<TestResult>>>,
) -> Result<GroupStats, Box<dyn std::error::Error + Send + Sync>> {
    let mut tasks = Vec::with_capacity(configs.len());

    for (i, config) in configs.iter().enumerate() {
        let socks5_port = settings.start_port + i as u16;
        let task = spawn_config_test(
            config.clone(),
            socks5_port,
            domains.to_vec(),
            group_dir.to_path_buf(),
            settings.clone(),
            results.clone(),
        );
        
        print_config_start(config, socks5_port);
        tasks.push(task);
    }

    let mut successful_total = 0;
    let mut tests_total = 0;

    for (i, task) in tasks.into_iter().enumerate() {
        match task.await {
            Ok(Some((config_name, successful, total))) => {
                successful_total += successful;
                tests_total += total;
                print_config_result(&config_name, successful, total);
            }
            Ok(None) => print_config_error(&configs[i]),
            Err(e) => eprintln!("   [ERROR] Task execution failed: {:?}", e),
        }
    }

    Ok(GroupStats {
        successful: successful_total,
        total: tests_total,
    })
}

fn spawn_config_test(
    config: String,
    socks5_port: u16,
    domains: Vec<String>,
    group_dir: PathBuf,
    settings: Settings,
    results: Arc<Mutex<Vec<TestResult>>>,
) -> tokio::task::JoinHandle<Option<(String, usize, usize)>> {
    tokio::spawn(async move {
        run_config_test(&config, socks5_port, &domains, &group_dir, &settings, results)
            .await
            .ok()
    })
}

async fn run_config_test(
    config: &str,
    socks5_port: u16,
    domains: &[String],
    group_dir: &Path,
    settings: &Settings,
    results: Arc<Mutex<Vec<TestResult>>>,
) -> Result<(String, usize, usize), Box<dyn std::error::Error + Send + Sync>> {
    let mut process = start_ciadpi_process(config, socks5_port, group_dir)?;
    time::sleep(Duration::from_millis(settings.ciadpi_start_delay_ms)).await;

    let (successful, failed) = test_all_domains(domains, socks5_port, settings.request_timeout_sec).await;

    stop_process(&mut process);

    let total_tests = successful.len() + failed.len();
    let result = TestResult::new(config.to_string(), socks5_port, successful.clone(), failed);
    
    results.lock().await.push(result);

    let config_name = extract_config_name(config);
    Ok((config_name, successful.len(), total_tests))
}

fn start_ciadpi_process(
    config: &str,
    socks5_port: u16,
    group_dir: &Path,
) -> Result<Child, Box<dyn std::error::Error + Send + Sync>> {
    let exe_name = if cfg!(windows) { "ciadpi.exe" } else { "./ciadpi" };
    let log_file_name = sanitize_filename(config, socks5_port);
    let log_path = group_dir.join(format!("ciadpi_{}.log", log_file_name));
    
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;

    let args: Vec<&str> = config.split_whitespace().collect();
    
    Command::new(exe_name)
        .args(&args)
        .args(&["--ip", "0.0.0.0", "--port", &socks5_port.to_string(), "-Y"])
        .stdout(Stdio::from(log_file.try_clone()?))
        .stderr(Stdio::from(log_file))
        .spawn()
        .map_err(|e| format!("Failed to spawn {} ({}): {}", exe_name, config, e).into())
}

fn stop_process(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

async fn test_all_domains(
    domains: &[String],
    port: u16,
    timeout_sec: u64,
) -> (Vec<String>, Vec<String>) {
    let tasks: Vec<_> = domains
        .iter()
        .map(|domain| test_domain(domain.clone(), port, timeout_sec))
        .collect();

    let results = futures::future::join_all(tasks).await;
    
    let (successful, failed): (Vec<_>, Vec<_>) =
        results.into_iter().partition(|(_, success)| *success);

    let successful_domains = successful.into_iter().map(|(domain, _)| domain).collect::<Vec<_>>();
    let failed_domains = failed.into_iter().map(|(domain, _)| domain).collect::<Vec<_>>();

    (successful_domains, failed_domains)
}

async fn test_domain(domain: String, port: u16, timeout_sec: u64) -> (String, bool) {
    let client = match create_http_client(port, timeout_sec) {
        Ok(c) => c,
        Err(_) => return (domain, false),
    };

    let success = if try_https(&client, &domain).await.unwrap_or(false) {
        true
    } else {
        try_http(&client, &domain).await.unwrap_or(false)
    };

    (domain, success)
}

fn create_http_client(
    port: u16,
    timeout_sec: u64,
) -> Result<reqwest::Client, Box<dyn std::error::Error + Send + Sync>> {
    let proxy = reqwest::Proxy::all(format!("socks5h://127.0.0.1:{}", port))?;
    
    reqwest::Client::builder()
        .proxy(proxy)
        .timeout(Duration::from_secs(timeout_sec))
        .build()
        .map_err(Into::into)
}

async fn try_https(client: &reqwest::Client, domain: &str) -> Option<bool> {
    client
        .get(format!("https://{}", domain))
        .send()
        .await
        .ok()
        .map(|resp| resp.status().is_success())
}

async fn try_http(client: &reqwest::Client, domain: &str) -> Option<bool> {
    client
        .get(format!("http://{}", domain))
        .send()
        .await
        .ok()
        .map(|resp| resp.status().is_success())
}

async fn save_intermediate_results(
    results: &Arc<Mutex<Vec<TestResult>>>,
    filepath: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let locked = results.lock().await;
    write_results_file(&locked, filepath)?;
    print_status("[+]", &format!("Результаты сохранены в {}", filepath));
    Ok(())
}

async fn finalize_results(
    results: Arc<Mutex<Vec<TestResult>>>,
    settings: &Settings,
    group_stats: &[GroupStats],
    session_dir: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let locked = results.lock().await;
    write_results_file(&locked, &settings.results_file)?;
    
    let total_stats = calculate_total_stats(group_stats);
    show_final_results(&total_stats, session_dir, &settings.results_file);
    
    Ok(())
}

fn calculate_total_stats(group_stats: &[GroupStats]) -> GroupStats {
    let successful: usize = group_stats.iter().map(|s| s.successful).sum();
    let total: usize = group_stats.iter().map(|s| s.total).sum();
    GroupStats { successful, total }
}

fn show_welcome_message() {
    println!();
    print_banner("BDPI TESTER", "Инструмент тестирования прокси-конфигураций");
    println!();
    println!("Перед запуском убедитесь, что:");
    println!("   ✓ Файл 'settings.toml' содержит нужные настройки");
    println!("   ✓ Файл 'configs.txt' содержит список конфигураций");
    println!("   ✓ Файл 'domains.txt' содержит домены для проверки");
    println!("   ✓ Исполняемый файл ciadpi доступен в PATH");
    println!();
}

fn display_startup_info(settings: &Settings, configs: &[String], domains: &[String]) {
    println!();
    print_section("СТАТИСТИКА ЗАГРУЗКИ");
    print_table(&[
        ("Конфигураций загружено:", &configs.len().to_string()),
        ("Доменов для проверки:", &domains.len().to_string()),
    ]);

    print_section("НАСТРОЙКИ");
    print_table(&[
        ("Размер группы:", &settings.group_size.to_string()),
        ("Стартовый порт:", &settings.start_port.to_string()),
        ("Задержка между группами:", &format!("{} мс", settings.group_delay_ms)),
        ("Таймаут запроса:", &format!("{} сек", settings.request_timeout_sec)),
        ("Папка логов:", &settings.log_dir),
        ("Файл результатов:", &settings.results_file),
    ]);
}

fn print_group_header(group_num: usize, total_groups: usize, config_count: usize, start_port: u16) {
    print_section(&format!("ГРУППА {}/{}", group_num, total_groups));
    println!("   Конфигураций в группе: {}", config_count);
    println!("   Порт диапазон: {}-{}", start_port, start_port + config_count as u16 - 1);
}

fn print_config_start(config: &str, port: u16) {
    let config_name = extract_config_name(config);
    print_status("[~]", &format!("Запускаем {} на порту {}...", config_name, port));
}

fn print_config_result(config_name: &str, successful: usize, total: usize) {
    let rate = (successful as f32 / total as f32 * 100.0) as u32;
    let status = match rate {
        90..=100 => "[OK]",
        50..=89 => "[WARN]",
        _ => "[FAIL]",
    };
    
    println!("   {} {}: {}/{} успешно ({}%)", status, config_name, successful, total, rate);
}

fn print_config_error(config: &str) {
    println!("   [FAIL] {}: завершился с ошибкой", extract_config_name(config));
}

fn print_group_summary(group_num: usize, stats: &GroupStats) {
    let rate = stats.success_rate() as u32;
    println!();
    println!("   Группа {} завершена: {}/{} успешно ({}%)", 
        group_num, stats.successful, stats.total, rate);
}

fn show_final_results(stats: &GroupStats, session_dir: &Path, results_file: &str) {
    println!();
    print_section("ТЕСТИРОВАНИЕ ЗАВЕРШЕНО");
    println!();
    
    println!("   Общая статистика:");
    print_table(&[
        ("Всего тестов:", &stats.total.to_string()),
        ("Успешных:", &stats.successful.to_string()),
        ("Процент успеха:", &format!("{:.1}%", stats.success_rate())),
    ]);
    
    println!("   Результаты сохранены:");
    print_table(&[
        ("Файл результатов:", results_file),
        ("Папка логов:", &session_dir.display().to_string()),
    ]);
}

fn print_banner(title: &str, subtitle: &str) {
    const WIDTH: usize = 60;
    println!("┌{}┐", "─".repeat(WIDTH));
    println!("│{:^WIDTH$}│", title);
    println!("│{:^WIDTH$}│", subtitle);
    println!("└{}┘", "─".repeat(WIDTH));
}

fn print_section(title: &str) {
    println!();
    println!("▶ {}", title);
    println!("  {}", "─".repeat(58));
}

fn print_table(rows: &[(&str, &str)]) {
    let max_left = rows.iter().map(|(l, _)| l.len()).max().unwrap_or(0);
    
    for (left, right) in rows {
        println!("   {:width$} │ {}", left, right, width = max_left);
    }
    println!();
}

fn print_status(prefix: &str, message: &str) {
    println!("   {} {}", prefix, message);
}

fn wait_for_start() {
    print_status("[?]", "Для начала работы введите 'start' и нажмите Enter:");
    wait_for_input("start", "Пожалуйста, введите 'start' для продолжения:");
    println!();
}

fn confirm_start() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("   Нажмите Enter для начала или Ctrl+C для отмены...");
    let mut input = String::new();
    stdin().read_line(&mut input)?;
    Ok(())
}

fn wait_for_quit() {
    print_status("[?]", "Для выхода введите 'quit' и нажмите Enter:");
    wait_for_input("quit", "Пожалуйста, введите 'quit' для выхода:");
}

fn wait_for_input(expected: &str, retry_message: &str) {
    loop {
        let mut input = String::new();
        if stdin().read_line(&mut input).is_ok() && input.trim().eq_ignore_ascii_case(expected) {
            break;
        }
        print_status("[ERROR]", retry_message);
    }
}

async fn wait_between_groups(delay_ms: u64) {
    println!();
    print_status("[~]", &format!("Ожидание {} мс перед следующей группой...", delay_ms));
    time::sleep(Duration::from_millis(delay_ms)).await;
}

fn sanitize_filename(config: &str, port: u16) -> String {
    let base = config
        .split_whitespace()
        .next()
        .unwrap_or("cfg")
        .chars()
        .take(20)
        .filter(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
        .collect::<String>();
    
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    
    format!("{}_p{}_t{}", base, port, timestamp)
}

fn extract_config_name(config: &str) -> String {
    config
        .split_whitespace()
        .next()
        .unwrap_or("unknown")
        .to_string()
}

fn write_results_file(
    results: &[TestResult],
    filepath: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(filepath)?;

    write_header(&mut file, results.len())?;
    write_top_configs(&mut file, results)?;
    write_detailed_results(&mut file, results)?;
    
    file.flush()?;
    Ok(())
}

fn write_header(
    file: &mut File,
    total_configs: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    writeln!(file, "{}", "=".repeat(70))?;
    writeln!(file, "  BDPI TESTER - RESULTS REPORT")?;
    writeln!(file, "{}", "=".repeat(70))?;
    writeln!(file)?;
    writeln!(file, "Generated: {}", Local::now().format("%Y-%m-%d %H:%M:%S"))?;
    writeln!(file, "Total configs tested: {}", total_configs)?;
    writeln!(file)?;
    Ok(())
}

fn write_top_configs(
    file: &mut File,
    results: &[TestResult],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    writeln!(file, "{}", "─".repeat(70))?;
    writeln!(file, "  TOP 10 BEST PERFORMING CONFIGS")?;
    writeln!(file, "{}", "─".repeat(70))?;
    writeln!(file)?;

    let mut sorted_results: Vec<&TestResult> = results.iter().collect();
    sorted_results.sort_by(|a, b| {
        b.success_rate
            .partial_cmp(&a.success_rate)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.successful_domains.len().cmp(&a.successful_domains.len()))
    });

    for (rank, result) in sorted_results.iter().take(10).enumerate() {
        let total = result.successful_domains.len() + result.failed_domains.len();
        let medal = match rank {
            0 => "\u{1F947}",
            1 => "\u{1F948}",
            2 => "\u{1F949}",
            _ => "  ",
        };
        
        writeln!(
            file,
            "{} #{:<2} {} (port {})",
            medal,
            rank + 1,
            result.config,
            result.socks5_port
        )?;
        writeln!(
            file,
            "       Success: {}/{} ({:.1}%)",
            result.successful_domains.len(),
            total,
            result.success_rate
        )?;
        writeln!(file)?;
    }

    Ok(())
}

fn write_detailed_results(
    file: &mut File,
    results: &[TestResult],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    writeln!(file, "{}", "─".repeat(70))?;
    writeln!(file, "  DETAILED RESULTS FOR ALL CONFIGS")?;
    writeln!(file, "{}", "─".repeat(70))?;
    writeln!(file)?;

    for (idx, result) in results.iter().enumerate() {
        write_single_result(file, idx + 1, result)?;
    }

    Ok(())
}

fn write_single_result(
    file: &mut File,
    index: usize,
    result: &TestResult,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let total = result.successful_domains.len() + result.failed_domains.len();
    
    writeln!(file, "[{}] Config: {}", index, result.config)?;
    writeln!(file, "    Port: {}", result.socks5_port)?;
    writeln!(
        file,
        "    Success Rate: {:.1}% ({}/{})",
        result.success_rate,
        result.successful_domains.len(),
        total
    )?;
    writeln!(file)?;

    if !result.successful_domains.is_empty() {
        writeln!(file, "    ✓ Successful Domains ({}):", result.successful_domains.len())?;
        for (i, domain) in result.successful_domains.iter().enumerate() {
            write!(file, "      {}", domain)?;
            if (i + 1) % 3 == 0 || i == result.successful_domains.len() - 1 {
                writeln!(file)?;
            } else {
                write!(file, ", ")?;
            }
        }
        writeln!(file)?;
    }

    if !result.failed_domains.is_empty() {
        writeln!(file, "    ✗ Failed Domains ({}):", result.failed_domains.len())?;
        for (i, domain) in result.failed_domains.iter().enumerate() {
            write!(file, "      {}", domain)?;
            if (i + 1) % 3 == 0 || i == result.failed_domains.len() - 1 {
                writeln!(file)?;
            } else {
                write!(file, ", ")?;
            }
        }
        writeln!(file)?;
    }

    writeln!(file, "{}", "─".repeat(70))?;
    writeln!(file)?;
    
    Ok(())
}