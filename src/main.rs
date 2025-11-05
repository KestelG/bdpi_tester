use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{BufRead, BufReader, Write, stdin};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    show_welcome_message();
    
    wait_for_start();
    
    let settings: Settings = {
        print_status("[+]", "Загружаем настройки из settings.toml...");
        let s = std::fs::read_to_string("settings.toml")
            .map_err(|e| format!("Failed to read settings.toml: {}", e))?;
        toml::from_str(&s).map_err(|e| format!("Failed to parse settings.toml: {}", e))?
    };

    print_status("[+]", "Читаем файлы конфигураций и доменов...");
    let configs = read_lines("configs.txt")?;
    let domains = read_lines("domains.txt")?;

    println!();
    print_section("СТАТИСТИКА ЗАГРУЗКИ");
    print_table(&[
        ("Конфигураций загружено:", &format!("{}", configs.len())),
        ("Доменов для проверки:", &format!("{}", domains.len())),
    ]);

    print_section("НАСТРОЙКИ");
    print_table(&[
        ("Размер группы:", &format!("{}", settings.group_size)),
        ("Стартовый порт:", &format!("{}", settings.start_port)),
        ("Задержка между группами:", &format!("{} мс", settings.group_delay_ms)),
        ("Таймаут запроса:", &format!("{} сек", settings.request_timeout_sec)),
        ("Папка логов:", &settings.log_dir),
        ("Файл результатов:", &settings.results_file),
    ]);

    print_status("[>]", "Все готово к запуску тестирования!");
    println!("   Нажмите Enter для начала или Ctrl+C для отмены...");
    let mut input = String::new();
    stdin().read_line(&mut input)?;

    create_dir_all(&settings.log_dir)?;

    let now = Local::now().format("%Y-%m-%d_%H-%M-%S").to_string();
    let session_logs_dir = PathBuf::from(&settings.log_dir).join(&now);
    create_dir_all(&session_logs_dir)?;

    let results: Arc<Mutex<Vec<TestResult>>> = Arc::new(Mutex::new(Vec::new()));

    let total_groups = (configs.len() + settings.group_size - 1) / settings.group_size;
    let mut group_results = Vec::new();

    for (group_idx, chunk) in configs.chunks(settings.group_size).enumerate() {
        let group_number = group_idx + 1;
        
        print_section(&format!("ГРУППА {}/{}", group_number, total_groups));
        println!("   Конфигураций в группе: {}", chunk.len());
        println!("   Порт диапазон: {}-{}", 
            settings.start_port, 
            settings.start_port + chunk.len() as u16 - 1
        );

        let group_dir = session_logs_dir.join(format!("group_{}", group_number));
        create_dir_all(&group_dir)?;

        let mut tasks = Vec::new();

        for (i, config) in chunk.iter().enumerate() {
            let socks5_port = settings.start_port + i as u16;
            let config_clone = config.clone();
            let domains_clone = domains.clone();
            let group_dir_clone = group_dir.clone();
            let results_clone = results.clone();
            let settings_clone = settings.clone();

            print_status("[~]", &format!("Запускаем конфиг {} на порту {}...", 
                config.split_whitespace().next().unwrap_or("unknown"), 
                socks5_port
            ));

            tasks.push(tokio::spawn(async move {
                match run_one_config(
                    &config_clone,
                    socks5_port,
                    &domains_clone,
                    &group_dir_clone,
                    &settings_clone,
                    results_clone,
                ).await {
                    Ok(result) => Some(result),
                    Err(e) => {
                        eprintln!("   [ERROR] Ошибка в конфиге '{}': {}", config_clone, e);
                        None
                    }
                }
            }));
        }

        let mut successful_in_group = 0;
        let mut total_in_group = 0;

        for (i, t) in tasks.into_iter().enumerate() {
            match t.await {
                Ok(Some((config, successful, total))) => {
                    total_in_group += total;
                    successful_in_group += successful;
                    let success_rate = (successful as f32 / total as f32 * 100.0) as u32;
                    
                    let status = if success_rate > 80 {
                        "[OK]"
                    } else if success_rate > 50 {
                        "[WARN]"
                    } else {
                        "[FAIL]"
                    };
                    
                    println!("   {} Конфиг {}: {}/{} успешно ({}{}%)", 
                        status,
                        config.split_whitespace().next().unwrap_or("unknown"),
                        successful, total,
                        if success_rate == 100 { "" } else { "~" },
                        success_rate
                    );
                }
                Ok(None) => {
                    println!("   [FAIL] Конфиг {}: завершился с ошибкой", 
                        chunk.get(i).unwrap_or(&"unknown".to_string())
                    );
                }
                Err(e) => {
                    eprintln!("   [ERROR] Ошибка выполнения задачи: {:?}", e);
                }
            }
        }

        group_results.push((successful_in_group, total_in_group));
        
        let group_success_rate = if total_in_group > 0 {
            (successful_in_group as f32 / total_in_group as f32 * 100.0) as u32
        } else { 0 };

        println!();
        println!("   Группа {} завершена: {}/{} успешно ({}{}%)", 
            group_number, successful_in_group, total_in_group,
            if group_success_rate == 100 { "" } else { "~" },
            group_success_rate
        );

        {
            let locked = results.lock().await;
            if let Err(e) = write_results_file(&*locked, &settings.results_file) {
                eprintln!("   [ERROR] Ошибка записи файла результатов: {}", e);
            } else {
                print_status("[+]", &format!("Результаты сохранены в {}", &settings.results_file));
            }
        }

        if group_number < total_groups {
            println!();
            print_status("[~]", &format!("Ожидание {} мс перед следующей группой...", settings.group_delay_ms));
            time::sleep(Duration::from_millis(settings.group_delay_ms)).await;
        }
    }

    show_final_results(&group_results, &session_logs_dir, &settings.results_file).await;

    wait_for_quit();

    Ok(())
}

fn show_welcome_message() {
    println!();
    print_banner("BDPI TESTER", "Инструмент тестирования прокси-конфигураций");
    println!();
    println!("Перед запуском убедитесь, что:");
    println!("   * Файл 'settings.toml' содержит нужные настройки");
    println!("   * Файл 'configs.txt' содержит список конфигураций");
    println!("   * Файл 'domains.txt' содержит домены для проверки");
    println!("   * Исполняемый файл ciadpi доступен в PATH");
    println!();
}

fn print_banner(title: &str, subtitle: &str) {
    let width = 60;
    println!("┌{}┐", "─".repeat(width));
    println!("│{:^60}│", title);
    println!("│{:^60}│", subtitle);
    println!("└{}┘", "─".repeat(width));
}

fn print_table(rows: &[(&str, &str)]) {
    let left_width = rows.iter().map(|(left, _)| left.chars().count()).max().unwrap_or(0);
    let right_width = rows.iter().map(|(_, right)| right.chars().count()).max().unwrap_or(0);
    
    let total_width = left_width + right_width + 5;
    
    println!("   ┌{}┐", "─".repeat(total_width - 2));
    
    for (left, right) in rows {
        let left_padding = left_width - left.chars().count();
        let right_padding = right_width - right.chars().count();
        
        println!("   │ {}{} {}{} │", 
            left, 
            " ".repeat(left_padding),
            " ".repeat(right_padding),
            right
        );
    }
    
    println!("   └{}┘", "─".repeat(total_width - 2));
    println!();
}

fn wait_for_start() {
    print_status("[?]", "Для начала работы введите 'start' и нажмите Enter:");
    loop {
        let mut input = String::new();
        stdin().read_line(&mut input).unwrap();
        if input.trim().eq_ignore_ascii_case("start") {
            break;
        }
        print_status("[ERROR]", "Пожалуйста, введите 'start' для продолжения:");
    }
    println!();
}

async fn show_final_results(
    group_results: &[(usize, usize)], 
    session_logs_dir: &std::path::Path, 
    results_file: &str
) {
    let total_successful: usize = group_results.iter().map(|(s, _)| s).sum();
    let total_tests: usize = group_results.iter().map(|(_, t)| t).sum();
    let overall_success_rate = if total_tests > 0 {
        (total_successful as f32 / total_tests as f32 * 100.0) as u32
    } else { 0 };

    println!();
    print_section("ТЕСТИРОВАНИЕ ЗАВЕРШЕНО");
    println!();
    
    println!("   Общая статистика:");
    print_table(&[
        ("Всего тестов:", &format!("{}", total_tests)),
        ("Успешных:", &format!("{}", total_successful)),
        ("Процент успеха:", &format!("{}%", overall_success_rate)),
    ]);
    
    println!("   Результаты сохранены:");
    print_table(&[
        ("Файл результатов:", results_file),
        ("Папка логов:", &session_logs_dir.to_string_lossy()),
    ]);
}

fn wait_for_quit() {
    print_status("[?]", "Для выхода введите 'quit' и нажмите Enter:");
    loop {
        let mut input = String::new();
        stdin().read_line(&mut input).unwrap();
        if input.trim().eq_ignore_ascii_case("quit") {
            break;
        }
        print_status("[ERROR]", "Пожалуйста, введите 'quit' для выхода:");
    }
}

async fn run_one_config(
    config: &str,
    socks5_port: u16,
    domains: &[String],
    group_dir: &std::path::Path,
    settings: &Settings,
    results: Arc<Mutex<Vec<TestResult>>>,
) -> Result<(String, usize, usize), Box<dyn std::error::Error + Send + Sync>> {
    let short_name = sanitize_filename_for_log(config, socks5_port);
    let ciadpi_log_path = group_dir.join(format!("ciadpi_{}.log", short_name));
    let ciadpi_log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&ciadpi_log_path)?;

    let exe_name = match std::env::consts::OS {
        "windows" => "ciadpi.exe".to_string(),
        _ => "./ciadpi".to_string(),
    };

    let mut cmd = Command::new(&exe_name);
    let args: Vec<&str> = config.split_whitespace().collect();
    cmd.args(&args)
        .arg("--ip")
        .arg("0.0.0.0")
        .arg("--port")
        .arg(socks5_port.to_string())
        .arg("-Y")
        .stdout(Stdio::from(ciadpi_log.try_clone()?))
        .stderr(Stdio::from(ciadpi_log));

    let mut child = cmd.spawn().map_err(|e| {
        format!(
            "Failed to spawn ciadpi (exe = {}): {}, check if the binary exists and is executable",
            exe_name, e
        )
    })?;

    time::sleep(Duration::from_millis(settings.ciadpi_start_delay_ms)).await;

    let mut tasks = Vec::new();
    for domain in domains.iter() {
        let domain_clone = domain.clone();
        let proxy_port = socks5_port;
        let timeout = settings.request_timeout_sec;
        tasks.push(tokio::spawn(async move {
            test_domain_via_socks5(&domain_clone, proxy_port, timeout).await
        }));
    }

    let mut successful = Vec::new();
    let mut failed = Vec::new();

    for t in tasks {
        match t.await {
            Ok((domain, true)) => successful.push(domain),
            Ok((domain, false)) => failed.push(domain),
            Err(e) => {
                eprintln!("   [ERROR] Ошибка теста домена: {:?}", e);
            }
        }
    }

    let _ = child.kill();
    let _ = child.wait();

    let result = TestResult {
        config: config.to_string(),
        socks5_port,
        successful_domains: successful.clone(),
        failed_domains: failed.clone(),
    };

    {
        let mut guard = results.lock().await;
        guard.push(result);
    }

    let config_short = config.split_whitespace().next().unwrap_or("unknown").to_string();
    Ok((config_short, successful.len(), successful.len() + failed.len()))
}

async fn test_domain_via_socks5(domain: &str, port: u16, timeout_sec: u64) -> (String, bool) {
    let proxy = format!("socks5h://127.0.0.1:{}", port);

    let proxy_obj = match reqwest::Proxy::all(&proxy) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("   [ERROR] Ошибка создания прокси {}: {}", proxy, e);
            return (domain.to_string(), false);
        }
    };

    let client = match reqwest::Client::builder()
        .proxy(proxy_obj)
        .timeout(Duration::from_secs(timeout_sec))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("   [ERROR] Ошибка создания HTTP клиента: {}", e);
            return (domain.to_string(), false);
        }
    };

    let url_https = format!("https://{}", domain);
    match client.get(&url_https).send().await {
        Ok(resp) => (domain.to_string(), resp.status().is_success()),
        Err(err_https) => {
            let url_http = format!("http://{}", domain);
            match client.get(&url_http).send().await {
                Ok(resp) => (domain.to_string(), resp.status().is_success()),
                Err(err_http) => {
                    (domain.to_string(), false)
                }
            }
        }
    }
}

fn write_results_file(results: &[TestResult], results_file: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(results_file)?;

    writeln!(f, "=== BDPI Tester Results ===")?;
    writeln!(f, "Generated: {}", Local::now().format("%Y-%m-%d %H:%M:%S"))?;
    writeln!(f, "Total configs tested: {}", results.len())?;
    writeln!(f, "\n=== TOP 10 CONFIGS ===")?;

    let mut refs: Vec<&TestResult> = results.iter().collect();
    refs.sort_by(|a, b| b.successful_domains.len().cmp(&a.successful_domains.len()));

    for (i, r) in refs.iter().take(10).enumerate() {
        let total = r.successful_domains.len() + r.failed_domains.len();
        let success_rate = if total > 0 {
            (r.successful_domains.len() as f32 / total as f32 * 100.0) as u32
        } else { 0 };
        
        writeln!(
            f,
            "{}. {} (port {}) - Success: {}/{} ({}%)",
            i + 1,
            r.config,
            r.socks5_port,
            r.successful_domains.len(),
            total,
            success_rate
        )?;
    }

    writeln!(f, "\n=== DETAILED RESULTS ===")?;
    for r in results {
        let total = r.successful_domains.len() + r.failed_domains.len();
        let success_rate = if total > 0 {
            (r.successful_domains.len() as f32 / total as f32 * 100.0) as u32
        } else { 0 };
        
        writeln!(f, "\nConfig: {} (port {})", r.config, r.socks5_port)?;
        writeln!(f, "Success Rate: {}% ({}/{})", success_rate, r.successful_domains.len(), total)?;
        writeln!(f, "Successful domains:")?;
        for d in &r.successful_domains {
            writeln!(f, "  + {}", d)?;
        }
        if !r.failed_domains.is_empty() {
            writeln!(f, "Failed domains:")?;
            for d in &r.failed_domains {
                writeln!(f, "  - {}", d)?;
            }
        }
        writeln!(f, "{}", "=".repeat(60))?;
    }

    f.flush()?;
    Ok(())
}

fn read_lines(filename: &str) -> Result<Vec<String>, std::io::Error> {
    let file = File::open(filename)?;
    let reader = BufReader::new(file);
    reader.lines().collect()
}

fn sanitize_filename_for_log(config: &str, port: u16) -> String {
    let mut s = config.split_whitespace().next().unwrap_or("cfg").to_string();
    s = s.chars().take(20).collect();
    s = s.replace(|c: char| !c.is_alphanumeric() && c != '-', "_");
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    format!("{}_p{}_t{}", s, port, ts)
}

fn print_status(prefix: &str, message: &str) {
    println!(" {} {}", prefix, message);
}

fn print_section(title: &str) {
    let title_length = title.chars().count();
    let padding = 4;
    
    println!();
    println!("┌{}┐", "─".repeat(title_length + padding));
    println!("│  {}  │", title);
    println!("└{}┘", "─".repeat(title_length + padding));
}
