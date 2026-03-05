use crate::types::{BenchConfig, BenchResult};
use colored::Colorize;
use comfy_table::{modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL, Cell, Color, Table};

pub fn print_banner(config: &BenchConfig) {
    println!();
    println!("{}", "╔══════════════════════════════════════════════════════════╗".cyan());
    println!("{}", "║  MooseFS Benchmark Suite (Rust)                         ║".cyan());
    println!("{}", "╚══════════════════════════════════════════════════════════╝".cyan());
    println!("  Mode:    {}", if config.quick { "quick" } else { "full" });
    println!("  Local:   {}", config.local_dir.display());
    println!("  MooseFS: {}", config.mfs_dir.display());
    println!("  Date:    {}", chrono::Local::now().format("%Y-%m-%d %H:%M:%S"));
    println!();
}

pub fn section(title: &str) {
    println!();
    println!("{}", format!("▶ {title}").yellow().bold());
}

pub fn print_summary(results: &[BenchResult]) {
    println!();
    println!("{}", "╔══════════════════════════════════════════════════════════════════════════════════╗".cyan());
    println!("{}", "║  BENCHMARK RESULTS                                                             ║".cyan());
    println!("{}", "╚══════════════════════════════════════════════════════════════════════════════════╝".cyan());
    println!();

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_header(vec![
            Cell::new("Test").fg(Color::White),
            Cell::new("Local").fg(Color::White),
            Cell::new("MooseFS").fg(Color::Cyan),
            Cell::new("Unit").fg(Color::White),
            Cell::new("Ratio").fg(Color::Yellow),
            Cell::new("Verdict").fg(Color::White),
        ]);

    for r in results {
        let ratio = r.ratio();
        let ratio_str = ratio
            .map(|v| format!("{v:.2}x"))
            .unwrap_or_else(|| "N/A".to_string());

        let verdict = ratio.map(|v| {
            if r.higher_is_better {
                if v >= 0.9 { "✅" } else if v >= 0.5 { "⚠️" } else { "❌" }
            } else {
                // Lower is better (latency)
                if v <= 1.1 { "✅" } else if v <= 2.0 { "⚠️" } else { "❌" }
            }
        }).unwrap_or("—");

        let ratio_color = ratio.map(|v| {
            if r.higher_is_better {
                if v >= 0.9 { Color::Green } else if v >= 0.5 { Color::Yellow } else { Color::Red }
            } else {
                if v <= 1.1 { Color::Green } else if v <= 2.0 { Color::Yellow } else { Color::Red }
            }
        }).unwrap_or(Color::White);

        table.add_row(vec![
            Cell::new(&r.name),
            Cell::new(format_value(r.local_value)),
            Cell::new(format_value(r.mfs_value)).fg(Color::Cyan),
            Cell::new(&r.unit),
            Cell::new(&ratio_str).fg(ratio_color),
            Cell::new(verdict),
        ]);
    }

    println!("{table}");

    // Summary statistics
    let ratios: Vec<f64> = results
        .iter()
        .filter(|r| r.higher_is_better)  // only throughput metrics for aggregate
        .filter_map(|r| r.ratio())
        .collect();

    if !ratios.is_empty() {
        let mut sorted = ratios.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = sorted[sorted.len() / 2];
        let mean: f64 = ratios.iter().sum::<f64>() / ratios.len() as f64;
        let best = sorted.last().unwrap();
        let worst = sorted.first().unwrap();

        println!();
        println!("  {} (throughput metrics only):", "Summary".bold());
        println!("    Median ratio:  {:.2}x", median);
        println!("    Mean ratio:    {:.2}x", mean);
        println!("    Best:          {:.2}x", best);
        println!("    Worst:         {:.2}x", worst);
        println!();
    }
}

pub fn write_csv(path: &std::path::Path, results: &[BenchResult]) {
    let mut wtr = csv::Writer::from_path(path).expect("Failed to create CSV");
    wtr.write_record(["test", "local", "moosefs", "unit", "ratio"]).ok();
    for r in results {
        let ratio = r.ratio().map(|v| format!("{v:.3}")).unwrap_or_default();
        wtr.write_record([
            &r.name,
            &format!("{:.2}", r.local_value),
            &format!("{:.2}", r.mfs_value),
            &r.unit,
            &ratio,
        ]).ok();
    }
    wtr.flush().ok();
    println!("  Results saved to: {}", path.display().to_string().green());
}

fn format_value(v: f64) -> String {
    if v >= 1_000_000.0 {
        format!("{:.1}M", v / 1_000_000.0)
    } else if v >= 1_000.0 {
        format!("{:.1}K", v / 1_000.0)
    } else if v >= 1.0 {
        format!("{:.1}", v)
    } else if v > 0.0 {
        format!("{:.3}", v)
    } else {
        "0".to_string()
    }
}
