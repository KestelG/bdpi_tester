use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
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
    // Загружаем настройки
    let settings: Settings = {
        let s = std::fs::read_to_string("settings.toml")
            .map_err(|e| format!("Failed to read settings.toml: {}", e))?;
        toml::from_str(&s).map_err(|e| format!("Failed to parse settings.toml: {}", e))?
    };

    // Читаем входные файлы
    let configs = read_lines("configs.txt")?;
    let domains = read_lines("domains.txt")?;

    println!("Loaded {} configs and {} domains", configs.len(), domains.len());
    println!("Settings: {:?}", settings);

    // Создаем папку логов
    create_dir_all(&settings.log_dir)?;

    // Временная метка для сессии (чтобы групповые логи были в одной папке)
    let now = Local::now().format("%Y-%m-%d_%H-%M-%S").to_string();
    let session_logs_dir = PathBuf::from(&settings.log_dir).join(now);
    create_dir_all(&session_logs_dir)?;

    // Общий результат (аккумулируется по мере выполнения групп)
    let results: Arc<Mutex<Vec<TestResult>>> = Arc::new(Mutex::new(Vec::new()));

    // Проходим группами
    for (group_idx, chunk) in configs.chunks(settings.group_size).enumerate() {
        let group_number = group_idx + 1;
        println!("Starting group {} ({} configs)", group_number, chunk.len());

        let group_dir = session_logs_dir.join(format!("group_{}", group_number));
        create_dir_all(&group_dir)?;

        let mut tasks = Vec::new();

        // для каждой конфигурации в группе порты = start_port + index_in_group
        for (i, config) in chunk.iter().enumerate() {
            let socks5_port = settings.start_port + i as u16; // повторно используется для каждой группы
            let config_clone = config.clone();
            let domains_clone = domains.clone();
            let group_dir_clone = group_dir.clone();
            let results_clone = results.clone();
            let settings_clone = settings.clone();

            tasks.push(tokio::spawn(async move {
                if let Err(e) = run_one_config(
                    &config_clone,
                    socks5_port,
                    &domains_clone,
                    &group_dir_clone,
                    &settings_clone,
                    results_clone,
                )
                .await
                {
                    eprintln!("Error running config '{}': {}", config_clone, e);
                }
            }));
        }

        // Ждём завершения группы
        for t in tasks {
            let _ = t.await;
        }

        // После завершения группы — обновляем общий results файл (truncate)
        {
            let locked = results.lock().await;
            if let Err(e) = write_results_file(&*locked, &settings.results_file) {
                eprintln!("Failed to write results file: {}", e);
            } else {
                println!("Updated results file: {}", &settings.results_file);
            }
        }

        println!("Group {} finished. Sleeping {} ms before next group...", group_number, settings.group_delay_ms);
        time::sleep(Duration::from_millis(settings.group_delay_ms)).await;
    }

    println!("All groups finished.");
    Ok(())
}

/// Запустить ciadpi для одного конфига, протестировать все домены через запущенный прокси,
/// записать логи в group_dir, и положить результат в shared results.
async fn run_one_config(
    config: &str,
    socks5_port: u16,
    domains: &[String],
    group_dir: &std::path::Path,
    settings: &Settings,
    results: Arc<Mutex<Vec<TestResult>>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Лог-файлы для данного конфигура
    let short_name = sanitize_filename_for_log(config, socks5_port);
    let ciadpi_log_path = group_dir.join(format!("ciadpi_{}.log", short_name));
    let ciadpi_log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&ciadpi_log_path)?;

    // Определяем путь к исполняемому файлу ciadpi в зависимости от ОС
    let exe_name = match std::env::consts::OS {
        "windows" => "ciadpi.exe".to_string(),
        _ => "./ciadpi".to_string(),
    };

    // Команда запуска ciadpi (без sudo: пользователь должен запускать программу с нужными правами сам)
    let mut cmd = Command::new(&exe_name);
    // Подставляем аргументы из строки config (если есть дополнительные)
    let args: Vec<&str> = config.split_whitespace().collect();
    cmd.args(&args)
        .arg("--ip")
        .arg("0.0.0.0")
        .arg("--port")
        .arg(socks5_port.to_string())
        .arg("-Y")
        .stdout(Stdio::from(ciadpi_log.try_clone()?))
        .stderr(Stdio::from(ciadpi_log));

    // Запускаем ciadpi
    let mut child = cmd.spawn().map_err(|e| {
        format!(
            "Failed to spawn ciadpi (exe = {}): {}, check if the binary exists and is executable",
            exe_name, e
        )
    })?;

    // Даем время ciadpi подняться
    time::sleep(Duration::from_millis(settings.ciadpi_start_delay_ms)).await;

    // Тестируем домены параллельно (через SOCKS5 на socks5_port)
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
                eprintln!("Domain task join error: {:?}", e);
            }
        }
    }

    // Останавливаем ciadpi
    let _ = child.kill();
    let _ = child.wait();

    // Сохраняем result
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

    // Короткий лог в stdout
    println!(
        "Config '{}' on port {} done: {}/{} successful",
        config,
        socks5_port,
        successful.len(),
        successful.len() + failed.len()
    );

    Ok(())
}

/// Проверка домена через SOCKS5 (socks5h чтобы DNS резолв шел через прокси)
async fn test_domain_via_socks5(domain: &str, port: u16, timeout_sec: u64) -> (String, bool) {
    let proxy = format!("socks5h://127.0.0.1:{}", port);

    let proxy_obj = match reqwest::Proxy::all(&proxy) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Proxy build error for {}: {}", proxy, e);
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
            eprintln!("Reqwest client build error: {}", e);
            return (domain.to_string(), false);
        }
    };

    // Сначала пробуем HTTPS (обычно важнее), если падает — пробуем HTTP
    let url_https = format!("https://{}", domain);
    match client.get(&url_https).send().await {
        Ok(resp) => (domain.to_string(), resp.status().is_success()),
        Err(err_https) => {
            let url_http = format!("http://{}", domain);
            match client.get(&url_http).send().await {
                Ok(resp) => (domain.to_string(), resp.status().is_success()),
                Err(err_http) => {
                    eprintln!(
                        "Both HTTPS and HTTP failed for {} via {}: {}, {}",
                        domain, proxy, err_https, err_http
                    );
                    (domain.to_string(), false)
                }
            }
        }
    }
}

/// Записываем результаты в файл (truncate)
fn write_results_file(results: &[TestResult], results_file: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(results_file)?;

    writeln!(f, "=== BDPI Tester Results ===")?;
    writeln!(f, "Total configs tested: {}", results.len())?;
    writeln!(f, "\n=== TOP 10 CONFIGS ===")?;

    // сортировка по количеству успешных
    let mut refs: Vec<&TestResult> = results.iter().collect();
    refs.sort_by(|a, b| b.successful_domains.len().cmp(&a.successful_domains.len()));

    for (i, r) in refs.iter().take(10).enumerate() {
        writeln!(
            f,
            "{}. {} (port {}) - Success: {}/{}",
            i + 1,
            r.config,
            r.socks5_port,
            r.successful_domains.len(),
            r.successful_domains.len() + r.failed_domains.len()
        )?;
    }

    writeln!(f, "\n=== DETAILED RESULTS ===")?;
    for r in results {
        writeln!(f, "\nConfig: {} (port {})", r.config, r.socks5_port)?;
        writeln!(f, "Successful ({}/{}):", r.successful_domains.len(), r.successful_domains.len() + r.failed_domains.len())?;
        for d in &r.successful_domains {
            writeln!(f, "  ✓ {}", d)?;
        }
        if !r.failed_domains.is_empty() {
            writeln!(f, "Failed:")?;
            for d in &r.failed_domains {
                writeln!(f, "  ✗ {}", d)?;
            }
        }
        writeln!(f, "{}", "=".repeat(50))?;
    }

    f.flush()?;
    Ok(())
}

/// Утилита чтения строк из файла
fn read_lines(filename: &str) -> Result<Vec<String>, std::io::Error> {
    let file = File::open(filename)?;
    let reader = BufReader::new(file);
    reader.lines().collect()
}

/// Простая санитайзация имени лога: на случай, если config - большая строка,
/// создаём читаемый короткий идентификатор: port_index_timestamp
fn sanitize_filename_for_log(config: &str, port: u16) -> String {
    // берем первые 20 символов конфига (заменим пробелы на '_') + порт
    let mut s = config.split_whitespace().next().unwrap_or("cfg").to_string();
    s = s.chars().take(20).collect();
    s = s.replace(|c: char| !c.is_alphanumeric() && c != '-', "_");
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    format!("{}_p{}_t{}", s, port, ts)
}
