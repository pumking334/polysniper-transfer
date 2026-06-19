use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

const BINANCE_WS_BASE: &str = "wss://stream.binance.com:9443/stream";
const HYPERLIQUID_WS: &str = "wss://api.hyperliquid.xyz/ws";
const GAMMA_URL: &str = "https://gamma-api.polymarket.com";
const CLOB_URL: &str = "https://clob.polymarket.com";

fn display_assets() -> Vec<(&'static str, &'static str)> {
    vec![
        ("btc", "BN"),
        ("eth", "BN"),
        ("sol", "BN"),
        ("bnb", "BN"),
        ("xrp", "BN"),
        ("doge", "BN"),
        ("hype", "HL"),
    ]
}

fn binance_symbols() -> Vec<(&'static str, &'static str)> {
    vec![
        ("btc", "BTCUSDT"),
        ("eth", "ETHUSDT"),
        ("sol", "SOLUSDT"),
        ("bnb", "BNBUSDT"),
        ("xrp", "XRPUSDT"),
        ("doge", "DOGEUSDT"),
    ]
}

#[derive(Clone)]
struct Config {
    live: bool,
    exits_live: bool,
    allow_fail_open_confirmation: bool,
    place_order_script: String,
    shares: f64,
    min_entry: f64,
    max_entry: f64,
    tp_half: f64,
    tp_full: f64,
    sl_half_pct: f64,
    sl_full_pct: f64,
    entry_secs: i64,
    force_exit_secs: i64,
    panic_exit_secs: i64,
    tp_grace_secs: i64,
    tp_grace_band: f64,
    closeout_retry_secs: i64,
    max_post_window_manage_secs: i64,
    max_panic_attempts: u32,
    poll_ms: u64,
    cancel_wait_ms: u64,
    entry_limit_pad: f64,
    hard_max_entry: f64,
    entry_limit_wait_ms: u64,
    entry_limit_poll_ms: u64,
    watch_zone_ratio: f64,
    candidate_poll_ms: u64,
    candidate_quote_ttl_ms: u64,
    exit_limit_retries: u32,
    exit_limit_wait_ms: u64,
    exit_limit_poll_ms: u64,
    exit_limit_step: f64,
    heartbeat_enabled: bool,
    heartbeat_interval_ms: u64,
    dust_ignore_shares: f64,
    flatten_retries: u32,
    flatten_retry_ms: u64,
    thresholds: HashMap<String, f64>,
}

impl Config {
    fn from_env() -> Self {
        dotenvy::dotenv().ok();

        let dry_run = env_bool("DRY_RUN", true);
        let live = !dry_run;
        let exits_live = env_bool("EXITS_LIVE", false);
        let allow_fail_open_confirmation = env_bool("ALLOW_FAIL_OPEN_CONFIRMATION", true);

        if live && !exits_live {
            panic!(
                "Refusing to run with LIVE entries while EXITS_LIVE=false. Set EXITS_LIVE=true for live mode."
            );
        }

        let mut thresholds = HashMap::new();
        for asset in ["btc", "eth", "sol", "bnb", "xrp", "doge"] {
            thresholds.insert(asset.to_string(), 0.050);
        }
        thresholds.insert("hype".to_string(), 0.100);

        Self {
            live,
            exits_live,
            allow_fail_open_confirmation,
            place_order_script: std::env::var("PLACE_ORDER_SCRIPT")
                .unwrap_or_else(|_| "./place_order.py".to_string()),
            shares: env_f64("SHARES", 10.0),
            min_entry: env_f64("MIN_ENTRY", 0.43),
            max_entry: env_f64("MAX_ENTRY", 0.56),
            tp_half: env_f64("TP_HALF", 0.69),
            tp_full: env_f64("TP_FULL", 0.79),
            sl_half_pct: env_f64("SL_HALF_PCT", 0.25),
            sl_full_pct: env_f64("SL_FULL_PCT", 0.30),
            entry_secs: env_i64("ENTRY_SECS", 180),
            force_exit_secs: env_i64("FORCE_EXIT_SECS", 60),
            panic_exit_secs: env_i64("PANIC_EXIT_SECS", 15),
            tp_grace_secs: env_i64("TP_GRACE_SECS", 10),
            tp_grace_band: env_f64("TP_GRACE_BAND", 0.02),
            closeout_retry_secs: env_i64("CLOSEOUT_RETRY_SECS", 3),
            max_post_window_manage_secs: env_i64("MAX_POST_WINDOW_MANAGE_SECS", 120),
            max_panic_attempts: env_u32("MAX_PANIC_ATTEMPTS", 12),
            poll_ms: env_u64("POLL_MS", 400),
            cancel_wait_ms: env_u64("CANCEL_WAIT_MS", 350),
            entry_limit_pad: env_f64("ENTRY_LIMIT_PAD", 0.03),
            hard_max_entry: env_f64("HARD_MAX_ENTRY", 0.59),
            entry_limit_wait_ms: env_u64("ENTRY_LIMIT_WAIT_MS", 2200),
            entry_limit_poll_ms: env_u64("ENTRY_LIMIT_POLL_MS", 250),
            watch_zone_ratio: env_f64("WATCH_ZONE_RATIO", 0.80),
            candidate_poll_ms: env_u64("CANDIDATE_POLL_MS", 120),
            candidate_quote_ttl_ms: env_u64("CANDIDATE_QUOTE_TTL_MS", 1500),
            exit_limit_retries: env_u32("EXIT_LIMIT_RETRIES", 4),
            exit_limit_wait_ms: env_u64("EXIT_LIMIT_WAIT_MS", 1200),
            exit_limit_poll_ms: env_u64("EXIT_LIMIT_POLL_MS", 200),
            exit_limit_step: env_f64("EXIT_LIMIT_STEP", 0.01),
            heartbeat_enabled: env_bool("HEARTBEAT_ENABLED", true),
            heartbeat_interval_ms: env_u64("HEARTBEAT_INTERVAL_MS", 5000),
            dust_ignore_shares: env_f64("DUST_IGNORE_SHARES", 0.01),
            flatten_retries: env_u32("FLATTEN_RETRIES", 8),
            flatten_retry_ms: env_u64("FLATTEN_RETRY_MS", 500),
            thresholds,
        }
    }

    fn threshold_for(&self, asset: &str) -> f64 {
        self.thresholds.get(asset).copied().unwrap_or(0.060)
    }
}

#[derive(Clone, Debug)]
struct Trade {
    time: String,
    asset: String,
    dir: String,
    entry: f64,
    exit: f64,
    shares: f64,
    result: String,
    pnl: f64,
    reason: String,
}

#[derive(Clone, Debug)]
struct OpenPos {
    asset: String,
    dir: String,
    token: String,
    question: String,
    entry: f64,
    held: f64,
    orig: f64,
    current: f64,
    realized: f64,
    note: String,
    events: Vec<String>,
    opened_at: i64,
}

#[derive(Clone, Debug)]
struct UnresolvedPos {
    asset: String,
    dir: String,
    token: String,
    shares: f64,
    note: String,
    since: i64,
}

#[derive(Clone, Debug)]
struct MarketInfo {
    yes_token: String,
    no_token: String,
    question: String,
    min_order_size: f64,
}

#[derive(Clone, Debug)]
struct CandidateQuote {
    asset: String,
    dir: String,
    ask: f64,
    token: String,
    observed_at_ms: i64,
}

#[derive(Clone, Debug)]
struct TpOrder {
    id: String,
    shares: f64,
    target_price: f64,
    filled_seen: f64,
}

struct BotState {
    trades: Vec<Trade>,
    open: HashMap<String, OpenPos>,
    unresolved: HashMap<String, UnresolvedPos>,
    wins: u32,
    losses: u32,
    total_pnl: f64,
    total_risked: f64,
    traded_window: i64,
    last_trade_info: String,
    session_start: i64,
    trade_ledger_file: String,
    market_cache: HashMap<String, MarketInfo>,
    window_opens: HashMap<String, f64>,
    attempt_in_flight: bool,
    recent_skips: Vec<String>,
    recent_signals: Vec<String>,
}

type PriceMap = Arc<DashMap<String, f64>>;
type QuoteMap = Arc<DashMap<String, CandidateQuote>>;
type State = Arc<Mutex<BotState>>;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let cfg = Config::from_env();
    let session_stamp = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();
    let state: State = Arc::new(Mutex::new(BotState {
        trades: Vec::new(),
        open: HashMap::new(),
        unresolved: HashMap::new(),
        wins: 0,
        losses: 0,
        total_pnl: 0.0,
        total_risked: 0.0,
        traded_window: 0,
        last_trade_info: "NO — watching".to_string(),
        session_start: unix_now(),
        trade_ledger_file: format!("trade_ledger_{}.jsonl", session_stamp),
        market_cache: HashMap::new(),
        window_opens: HashMap::new(),
        attempt_in_flight: false,
        recent_skips: Vec::new(),
        recent_signals: Vec::new(),
    }));
    let prices: PriceMap = Arc::new(DashMap::new());
    let quotes: QuoteMap = Arc::new(DashMap::new());

    {
        let p = prices.clone();
        tokio::spawn(async move {
            binance_ws(p).await;
        });
    }
    {
        let p = prices.clone();
        tokio::spawn(async move {
            hyperliquid_ws(p).await;
        });
    }

    let mode = if cfg.live { "LIVE" } else { "DRY RUN" };
    let exit_mode = if cfg.exits_live { "EXITS=LIVE" } else { "EXITS=LOG-ONLY" };
    println!("\n  Polymarket V2 Sniper — {mode} — {exit_mode}");
    println!("  Warming up (10s)...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .pool_max_idle_per_host(4)
        .user_agent("polysniper/0.2-compatible")
        .build()
        .expect("reqwest client");

    if cfg.live && cfg.heartbeat_enabled {
        let hb_cfg = cfg.clone();
        tokio::spawn(async move {
            heartbeat_loop(hb_cfg).await;
        });
    }

    {
        let p = prices.clone();
        let s = state.clone();
        let h = http.clone();
        let q = quotes.clone();
        let c = cfg.clone();
        tokio::spawn(async move {
            candidate_quote_refresher(p, s, h, q, c).await;
        });
    }

    run_bot(prices, quotes, state, http, cfg).await;
}

async fn call_script(cfg: &Config, args: &[String]) -> serde_json::Value {
    let mut cmd = tokio::process::Command::new("python3");
    cmd.arg(&cfg.place_order_script);
    for arg in args {
        cmd.arg(arg);
    }

    match cmd.output().await {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout).to_string();
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();

            if !output.status.success() {
                return serde_json::json!({
                    "ok": false,
                    "error": format!("script exit={} stderr={} stdout={}", output.status, stderr.trim(), stdout.trim())
                });
            }

            let last = stdout
                .lines()
                .rev()
                .find(|line| !line.trim().is_empty())
                .unwrap_or("")
                .trim();

            serde_json::from_str(last).unwrap_or_else(|_| {
                serde_json::json!({
                    "ok": false,
                    "error": format!("bad json stdout={} stderr={}", stdout.trim(), stderr.trim())
                })
            })
        }
        Err(err) => serde_json::json!({
            "ok": false,
            "error": format!("spawn error: {err}")
        }),
    }
}

fn jf(v: &serde_json::Value, key: &str) -> f64 {
    v.get(key)
        .and_then(|x| x.as_f64().or_else(|| x.as_str().and_then(|s| s.parse::<f64>().ok())))
        .unwrap_or(0.0)
}

fn js(v: &serde_json::Value, key: &str) -> String {
    v.get(key)
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string()
}

fn jb(v: &serde_json::Value, key: &str) -> bool {
    v.get(key).and_then(|x| x.as_bool()).unwrap_or(false)
}

async fn heartbeat_loop(cfg: Config) {
    let mut heartbeat_id = String::new();
    let sleep_ms = cfg.heartbeat_interval_ms.max(1000);
    info!("[HB] heartbeat task enabled every {}ms", sleep_ms);

    loop {
        let v = call_script(&cfg, &["heartbeat".to_string(), heartbeat_id.clone()]).await;
        if jb(&v, "ok") {
            let new_id = js(&v, "heartbeat_id");
            if !new_id.is_empty() {
                heartbeat_id = new_id;
            }
            if jb(&v, "resynced") {
                warn!("[HB] session resynced");
            }
        } else {
            warn!("[HB] failed: {}", v);
            heartbeat_id.clear();
        }
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }
}

async fn buy_fok(cfg: &Config, token: &str, price: f64, shares: f64) -> (f64, String, bool) {
    let v = call_script(
        cfg,
        &[
            "buy_fok".to_string(),
            token.to_string(),
            format!("{price:.4}"),
            format!("{:.6}", shares),
        ],
    )
    .await;

    let filled = jf(&v, "filled");
    let order_id = js(&v, "order_id");
    let assume = jb(&v, "assume_filled");

    if !jb(&v, "ok") && !assume {
        warn!("[BUY] bridge returned error: {}", v);
    }

    (filled, order_id, assume)
}

async fn buy_limit(cfg: &Config, token: &str, price: f64, shares: f64) -> Option<String> {
    let v = call_script(
        cfg,
        &[
            "buy_limit".to_string(),
            token.to_string(),
            format!("{price:.4}"),
            format!("{:.6}", shares),
        ],
    )
    .await;

    let id = js(&v, "order_id");
    if jb(&v, "ok") && !id.is_empty() {
        Some(id)
    } else {
        warn!("[BUY-LIMIT] place failed token={} shares={} price={:.0}c resp={}", token, shares, price * 100.0, v);
        None
    }
}

async fn token_shares(cfg: &Config, token: &str) -> Option<f64> {
    let v = call_script(cfg, &["balance".to_string(), token.to_string()]).await;
    if jb(&v, "ok") {
        Some(share_round(jf(&v, "shares")))
    } else {
        None
    }
}

async fn sell_limit(cfg: &Config, token: &str, price: f64, shares: f64) -> Option<String> {
    if shares_zero(shares) {
        return None;
    }

    let v = call_script(
        cfg,
        &[
            "sell_limit".to_string(),
            token.to_string(),
            format!("{price:.4}"),
            format!("{:.6}", shares),
        ],
    )
    .await;

    let id = js(&v, "order_id");
    if jb(&v, "ok") && !id.is_empty() {
        Some(id)
    } else {
        warn!(
            "[TP] place failed token={} shares={} price={:.0}c resp={}",
            token,
            shares,
            price * 100.0,
            v
        );
        None
    }
}

async fn sell_fak(cfg: &Config, token: &str, price: f64, shares: f64) -> (f64, f64) {
    if shares_zero(shares) {
        return (0.0, 0.0);
    }

    let v = call_script(
        cfg,
        &[
            "sell_fak".to_string(),
            token.to_string(),
            format!("{price:.4}"),
            format!("{:.6}", shares),
        ],
    )
    .await;

    if !jb(&v, "ok") {
        warn!("[SELL-FAK] bridge returned error: {}", v);
    }

    (
        share_round(jf(&v, "filled")),
        jf(&v, "avg_fill_price"),
    )
}

async fn cancel_orders(cfg: &Config, ids: &[String]) -> bool {
    let live_ids: Vec<String> = ids.iter().filter(|s| !s.is_empty()).cloned().collect();
    if live_ids.is_empty() {
        return true;
    }

    let v = call_script(cfg, &["cancel".to_string(), live_ids.join(",")]).await;
    if !jb(&v, "ok") {
        warn!("[CANCEL] bridge returned error: {}", v);
    }
    jb(&v, "ok")
}

async fn cancel_and_pause(cfg: &Config, ids: &[String]) {
    let live_ids: Vec<String> = ids.iter().filter(|s| !s.is_empty()).cloned().collect();
    if live_ids.is_empty() {
        return;
    }

    let _ = cancel_orders(cfg, &live_ids).await;
    tokio::time::sleep(Duration::from_millis(cfg.cancel_wait_ms)).await;
}

async fn order_fills(cfg: &Config, order_id: &str, token: &str, fallback_price: f64) -> (f64, f64) {
    if order_id.is_empty() {
        return (0.0, 0.0);
    }

    let v = call_script(
        cfg,
        &[
            "order_fills".to_string(),
            order_id.to_string(),
            token.to_string(),
            format!("{fallback_price:.4}"),
        ],
    )
    .await;

    (
        share_round(jf(&v, "filled")),
        jf(&v, "avg_fill_price"),
    )
}

async fn confirm_buy_fill(
    cfg: &Config,
    token: &str,
    requested: f64,
    bridge_filled: f64,
    assume_filled: bool,
) -> f64 {
    if !shares_zero(bridge_filled) {
        return bridge_filled.min(requested);
    }

    for _ in 0..6 {
        if let Some(wallet) = token_shares(cfg, token).await {
            if !shares_zero(wallet) {
                return wallet.min(requested);
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    if assume_filled && cfg.allow_fail_open_confirmation {
        warn!(
            "[ASSUME-FILLED] token={} could not be balance-confirmed; managing {} shares as open",
            token, requested
        );
        requested
    } else {
        0.0
    }
}

async fn enter_limit_partial(cfg: &Config, token: &str, price: f64, shares: f64) -> (f64, f64) {
    let before = token_shares(cfg, token).await.unwrap_or(0.0);
    let Some(order_id) = buy_limit(cfg, token, price, shares).await else {
        return (0.0, 0.0);
    };

    let mut best_after = before;
    let loops = (cfg.entry_limit_wait_ms / cfg.entry_limit_poll_ms.max(1)).max(1);
    for _ in 0..loops {
        tokio::time::sleep(Duration::from_millis(cfg.entry_limit_poll_ms)).await;
        if let Some(now) = token_shares(cfg, token).await {
            best_after = best_after.max(now);
        }
    }

    cancel_and_pause(cfg, &[order_id.clone()]).await;

    if let Some(now) = token_shares(cfg, token).await {
        best_after = best_after.max(now);
    }

    let delta_filled = shares_sub(best_after, before).min(shares);
    let (order_filled, avg_fill_price) = order_fills(cfg, &order_id, token, price).await;
    let filled = delta_filled.max(order_filled).min(shares);
    let avg = if avg_fill_price > 0.0 { avg_fill_price } else { price };
    (filled, avg)
}

async fn sell_limit_then_cancel(
    cfg: &Config,
    token: &str,
    limit_price: f64,
    desired: f64,
    fallback_held: f64,
    min_order_size: f64,
    wait_ms: u64,
    poll_ms: u64,
) -> (f64, f64) {
    let before = token_shares(cfg, token).await.unwrap_or(fallback_held);
    if shares_zero(before) {
        return (0.0, 0.0);
    }

    let qty = desired.min(before);
    if qty + SHARE_EPS < min_order_size {
        return (0.0, 0.0);
    }
    let Some(order_id) = sell_limit(cfg, token, limit_price, qty).await else {
        return (0.0, 0.0);
    };

    let loops = (wait_ms / poll_ms.max(1)).max(1);
    let mut best_after = before;
    for _ in 0..loops {
        tokio::time::sleep(Duration::from_millis(poll_ms)).await;
        if let Some(now) = token_shares(cfg, token).await {
            best_after = best_after.min(now);
        }
    }

    cancel_and_pause(cfg, &[order_id.clone()]).await;

    if let Some(now) = token_shares(cfg, token).await {
        best_after = best_after.min(now);
    }

    let delta_sold = shares_sub(before, best_after).min(qty);
    let (order_sold, avg_fill_price) = order_fills(cfg, &order_id, token, limit_price).await;
    let sold = delta_sold.max(order_sold).min(qty);
    let avg = if avg_fill_price > 0.0 { avg_fill_price } else { limit_price };
    let proceeds = sold * avg;
    (sold, proceeds)
}

async fn protective_sell_live(
    http: &reqwest::Client,
    cfg: &Config,
    token: &str,
    desired: f64,
    fallback_held: f64,
    min_order_size: f64,
) -> (f64, f64) {
    let mut total_sold = 0.0f64;
    let mut proceeds_floor = 0.0f64;
    let mut remaining = desired.min(fallback_held);

    for attempt in 0..cfg.exit_limit_retries {
        let wallet_before = token_shares(cfg, token)
            .await
            .unwrap_or_else(|| remaining.max(shares_sub(fallback_held, total_sold)));
        if shares_zero(wallet_before) || shares_zero(remaining) {
            return (total_sold, proceeds_floor);
        }

        let qty = remaining.min(wallet_before);
        let bid = get_price(http, token, "SELL").await.unwrap_or(0.01).max(0.01);
        let limit_price = round4((bid - cfg.exit_limit_step * attempt as f64).max(0.01));
        let (sold, proceeds) = sell_limit_then_cancel(
            cfg,
            token,
            limit_price,
            qty,
            wallet_before,
            min_order_size,
            cfg.exit_limit_wait_ms,
            cfg.exit_limit_poll_ms,
        )
        .await;

        if !shares_zero(sold) {
            total_sold += sold;
            proceeds_floor += proceeds;
            remaining = shares_sub(remaining, sold);
            info!(
                "[EXIT-LIMIT] token={} sold={} rem={} @>= {:.0}c attempt={}/{}",
                token,
                fmt_shares(sold),
                fmt_shares(remaining),
                limit_price * 100.0,
                attempt + 1,
                cfg.exit_limit_retries
            );
        } else {
            info!(
                "[EXIT-LIMIT] token={} no fill @ {:.0}c attempt={}/{}",
                token,
                limit_price * 100.0,
                attempt + 1,
                cfg.exit_limit_retries
            );
        }

        if shares_zero(remaining) {
            return (total_sold, proceeds_floor);
        }
    }

    let wallet_before = token_shares(cfg, token).await.unwrap_or(remaining);
    if shares_zero(wallet_before) || shares_zero(remaining) {
        return (total_sold, proceeds_floor);
    }

    let bid = get_price(http, token, "SELL").await.unwrap_or(0.01).max(0.01);
    let qty = remaining.min(wallet_before);
    let (reported, avg_fill_price) = sell_fak(cfg, token, bid, qty).await;
    tokio::time::sleep(Duration::from_millis(cfg.flatten_retry_ms)).await;
    let after = token_shares(cfg, token)
        .await
        .unwrap_or_else(|| shares_sub(wallet_before, reported));
    let sold = shares_sub(wallet_before, after).max(reported).min(qty);
    if !shares_zero(sold) {
        let avg = if avg_fill_price > 0.0 { avg_fill_price } else { bid };
        warn!("[EXIT-FAK-FALLBACK] token={} sold={} @~{:.0}c", token, fmt_shares(sold), avg * 100.0);
        total_sold += sold;
        proceeds_floor += sold * avg;
    }

    (total_sold, proceeds_floor)
}

async fn sell_partial_live(
    http: &reqwest::Client,
    cfg: &Config,
    token: &str,
    desired: f64,
    fallback_held: f64,
    min_order_size: f64,
) -> (f64, f64) {
    protective_sell_live(http, cfg, token, desired, fallback_held, min_order_size).await
}

async fn flatten_remaining_live(
    http: &reqwest::Client,
    cfg: &Config,
    token: &str,
    expected_remaining: f64,
    min_order_size: f64,
) -> (f64, f64) {
    let (sold, proceeds_floor) = protective_sell_live(http, cfg, token, expected_remaining, expected_remaining, min_order_size).await;
    let remaining = shares_sub(expected_remaining, sold);
    (remaining, proceeds_floor)
}

async fn reconcile_wallet_state(cfg: &Config, state: &State) {
    let open_snapshot: Vec<(String, String, f64)> = {
        let st = state.lock();
        st.open
            .iter()
            .map(|(asset, pos)| (asset.clone(), pos.token.clone(), pos.held))
            .collect()
    };

    for (asset, token, tracked) in open_snapshot {
        if let Some(wallet) = token_shares(cfg, &token).await {
            let mut st = state.lock();
            if let Some(pos) = st.open.get_mut(&asset) {
                if shares_zero(wallet) && !shares_zero(pos.held) {
                    pos.held = 0.0;
                    pos.note = format!("wallet reconciled flat | {}", pos.question);
                } else if (wallet - tracked).abs() > SHARE_EPS {
                    pos.held = wallet;
                }
            }
        }
    }

    let unresolved_snapshot: Vec<(String, String)> = {
        let st = state.lock();
        st.unresolved
            .iter()
            .map(|(asset, pos)| (asset.clone(), pos.token.clone()))
            .collect()
    };

    for (asset, token) in unresolved_snapshot {
        if let Some(wallet) = token_shares(cfg, &token).await {
            if shares_zero(wallet) {
                let mut st = state.lock();
                st.unresolved.remove(&asset);
                push_recent(&mut st.recent_skips, format!("{} {} unresolved cleared by wallet reconcile", hhmmss(unix_now()), asset.to_uppercase()), 8);
            }
        }
    }
}

async fn run_bot(prices: PriceMap, quotes: QuoteMap, state: State, http: reqwest::Client, cfg: Config) {
    let mut opens: HashMap<String, f64> = HashMap::new();
    let mut last_window: i64 = 0;
    let mut last_reconcile_at: i64 = 0;

    loop {
        let now = unix_now();
        if now - last_reconcile_at >= 30 {
            reconcile_wallet_state(&cfg, &state).await;
            last_reconcile_at = now;
        }
        let window_ts = (now / 300) * 300;
        let secs_left = (window_ts + 300) - now;
        let elapsed = 300 - secs_left;

        if window_ts != last_window {
            opens.clear();
            last_window = window_ts;

            let snap0: HashMap<String, f64> = prices.iter().map(|e| (e.key().clone(), *e.value())).collect();
            for (asset, _) in display_assets() {
                if let Some(&price) = snap0.get(asset) {
                    opens.insert(asset.to_string(), price);
                }
            }

            {
                let mut st = state.lock();
                st.market_cache.clear();
                st.window_opens = opens.clone();
                st.attempt_in_flight = false;
            }

            info!("── New window {}Z — caching markets ──", hhmm(window_ts));
            let h = http.clone();
            let s = state.clone();
            let c = cfg.clone();
            tokio::spawn(async move {
                cache_markets(h, s, window_ts, c.entry_secs).await;
            });
        }

        let snap: HashMap<String, f64> = prices.iter().map(|e| (e.key().clone(), *e.value())).collect();
        for (asset, _) in display_assets() {
            if let Some(&price) = snap.get(asset) {
                opens.entry(asset.to_string()).or_insert(price);
            }
        }
        {
            let mut st = state.lock();
            st.window_opens = opens.clone();
        }

        if elapsed <= cfg.entry_secs {
            let (entered, busy) = {
                let st = state.lock();
                (st.traded_window == window_ts, st.attempt_in_flight)
            };

            if !entered && !busy {
                let mut best: Option<(String, f64)> = None;

                for (asset, _) in display_assets() {
                    let (asset_open, asset_unresolved) = {
                        let st = state.lock();
                        (st.open.contains_key(asset), st.unresolved.contains_key(asset))
                    };
                    if asset_open || asset_unresolved {
                        continue;
                    }

                    if let (Some(&now_px), Some(&open_px)) = (snap.get(asset), opens.get(asset)) {
                        if open_px > 0.0 {
                            let move_pct = (now_px - open_px) / open_px * 100.0;
                            if move_pct.abs() >= cfg.threshold_for(asset)
                                && best.as_ref().map_or(true, |(_, b)| move_pct.abs() > b.abs())
                            {
                                best = Some((asset.to_string(), move_pct));
                            }
                        }
                    }
                }

                if let Some((asset, mv)) = best {
                    let has_market = {
                        let st = state.lock();
                        st.market_cache.contains_key(&asset)
                    };

                    if has_market {
                        let dir = if mv > 0.0 { "YES" } else { "NO" }.to_string();
                        {
                            let mut st = state.lock();
                            st.attempt_in_flight = true;
                            st.traded_window = window_ts;
                        }

                        info!(
                            "[SIGNAL] {} {} {:+.3}% at {}s",
                            asset.to_uppercase(),
                            if mv > 0.0 { "UP" } else { "DOWN" },
                            mv,
                            elapsed
                        );

                        let h = http.clone();
                        let q = quotes.clone();
                        let s = state.clone();
                        let c = cfg.clone();
                        tokio::spawn(async move {
                            trade_signal(h, q, s, asset, dir, window_ts, c).await;
                        });
                    }
                }
            }
        }

        let frame = dashboard(&snap, &opens, &state, &cfg, window_ts, elapsed, secs_left);
        print!("\x1B[H\x1B[J{frame}");
        let _ = std::io::stdout().flush();
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

async fn candidate_quote_refresher(prices: PriceMap, state: State, http: reqwest::Client, quotes: QuoteMap, cfg: Config) {
    loop {
        let snapshot = prices.iter().map(|e| (e.key().clone(), *e.value())).collect::<HashMap<_, _>>();
        let (market_cache, window_opens, open_assets, unresolved_assets) = {
            let st = state.lock();
            (
                st.market_cache.clone(),
                st.window_opens.clone(),
                st.open.keys().cloned().collect::<Vec<_>>(),
                st.unresolved.keys().cloned().collect::<Vec<_>>(),
            )
        };

        for (asset, _) in display_assets() {
            if open_assets.iter().any(|a| a == asset) || unresolved_assets.iter().any(|a| a == asset) {
                quotes.remove(asset);
                continue;
            }

            let Some(now_px) = snapshot.get(asset).copied() else {
                quotes.remove(asset);
                continue;
            };
            let Some(open_px) = window_opens.get(asset).copied() else {
                quotes.remove(asset);
                continue;
            };
            if open_px <= 0.0 {
                quotes.remove(asset);
                continue;
            }

            let mv = (now_px - open_px) / open_px * 100.0;
            let thr = cfg.threshold_for(asset);
            if mv.abs() < thr * cfg.watch_zone_ratio {
                quotes.remove(asset);
                continue;
            }

            let Some(market) = market_cache.get(asset).cloned() else {
                continue;
            };
            let dir = if mv > 0.0 { "YES" } else { "NO" }.to_string();
            let token = if dir == "YES" { market.yes_token } else { market.no_token };
            if let Some(ask) = get_price(&http, &token, "BUY").await {
                quotes.insert(asset.to_string(), CandidateQuote {
                    asset: asset.to_string(),
                    dir,
                    ask,
                    token,
                    observed_at_ms: unix_now_ms(),
                });
            }
        }

        tokio::time::sleep(Duration::from_millis(cfg.candidate_poll_ms.max(50))).await;
    }
}

async fn cache_markets(http: reqwest::Client, state: State, window_ts: i64, entry_secs: i64) {
    let total = display_assets().len();
    let deadline = window_ts + entry_secs;

    loop {
        if (unix_now() / 300) * 300 != window_ts {
            return;
        }
        if unix_now() > deadline {
            break;
        }

        let missing: Vec<&'static str> = display_assets()
            .into_iter()
            .map(|(asset, _)| asset)
            .filter(|asset| {
                let st = state.lock();
                !st.market_cache.contains_key(*asset)
            })
            .collect();

        for asset in missing {
            if let Some(market) = find_market(&http, asset, window_ts).await {
                let mut st = state.lock();
                st.market_cache.insert(asset.to_string(), market);
            }
        }

        let cached = {
            let st = state.lock();
            st.market_cache.len()
        };

        if cached == total {
            info!("[MKT] {}Z cached {}/{}", hhmm(window_ts), cached, total);
            return;
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    let cached = {
        let st = state.lock();
        st.market_cache.len()
    };
    warn!(
        "[MKT] {}Z cached only {}/{} before entry deadline",
        hhmm(window_ts),
        cached,
        total
    );
}

async fn trade_signal(http: reqwest::Client, quotes: QuoteMap, state: State, asset: String, dir: String, window_ts: i64, cfg: Config) {
    let market = {
        let st = state.lock();
        st.market_cache.get(&asset).cloned()
    };

    let market = match market {
        Some(m) => m,
        None => {
            state.lock().attempt_in_flight = false;
            let msg = format!("{} no market cached", asset.to_uppercase());
            push_skip(&state, msg.clone());
            warn!("[SKIP] {}", msg);
            return;
        }
    };

    let token = if dir == "YES" {
        market.yes_token.clone()
    } else {
        market.no_token.clone()
    };

    let cached_ask = quotes.get(&asset).and_then(|q| {
        if q.dir == dir && q.token == token && unix_now_ms() - q.observed_at_ms <= cfg.candidate_quote_ttl_ms as i64 {
            Some(q.ask)
        } else {
            None
        }
    });

    let ask = if let Some(price) = cached_ask {
        price
    } else {
        match get_price(&http, &token, "BUY").await {
            Some(price) if price > 0.0 => price,
            _ => {
                state.lock().attempt_in_flight = false;
                let msg = format!("{} {} no BUY price", asset.to_uppercase(), dir);
                push_skip(&state, msg.clone());
                warn!("[SKIP] {}", msg);
                return;
            }
        }
    };

    let signal_msg = format!(
        "{} {} ask={:.0}c range={:.0}-{:.0}c q={}",
        asset.to_uppercase(),
        dir,
        ask * 100.0,
        cfg.min_entry * 100.0,
        cfg.max_entry * 100.0,
        market.question
    );
    push_signal(&state, signal_msg);

    if ask < cfg.min_entry || ask > cfg.max_entry {
        state.lock().attempt_in_flight = false;
        let msg = format!(
            "{} {} ask {:.0}c outside {:.0}-{:.0}c",
            asset.to_uppercase(),
            dir,
            ask * 100.0,
            cfg.min_entry * 100.0,
            cfg.max_entry * 100.0
        );
        push_skip(&state, msg.clone());
        info!("[SKIP] {}", msg);
        return;
    }

    let limit_price = round4((ask + cfg.entry_limit_pad).min(cfg.hard_max_entry).min(0.97));
    info!(
        "[ENTER] {} {} @ {:.0}c x{} (LIMIT<= {:.0}c, partial-ok)",
        asset.to_uppercase(),
        dir,
        ask * 100.0,
        fmt_shares(cfg.shares),
        limit_price * 100.0
    );

    let (filled, actual_entry) = if cfg.live {
        enter_limit_partial(&cfg, &token, limit_price, cfg.shares).await
    } else {
        (cfg.shares, ask)
    };

    if shares_zero(filled) {
        state.lock().attempt_in_flight = false;
        let msg = format!("{} {} no entry fill at <= {:.0}c", asset.to_uppercase(), dir, limit_price * 100.0);
        push_skip(&state, msg.clone());
        warn!("[BUY-LIMIT-KILL] {}", msg);
        return;
    }

    {
        let mut st = state.lock();
        st.attempt_in_flight = false;
        st.traded_window = window_ts;
        st.total_risked = round2(st.total_risked + filled * actual_entry);
        st.last_trade_info = format!(
            "{} — {} @ {:.0}c ({}sh)",
            dir,
            asset.to_uppercase(),
            actual_entry * 100.0,
            fmt_shares(filled)
        );
        st.open.insert(
            asset.clone(),
            OpenPos {
                asset: asset.clone(),
                dir: dir.clone(),
                token: token.clone(),
                question: market.question.clone(),
                entry: actual_entry,
                held: filled,
                orig: filled,
                current: ask,
                realized: 0.0,
                note: if filled < cfg.shares {
                    format!("partial-entry {}/{} @ {:.0}c | {}", fmt_shares(filled), fmt_shares(cfg.shares), actual_entry * 100.0, market.question)
                } else {
                    format!("entry {:.0}c | {}", actual_entry * 100.0, market.question)
                },
                events: vec![format!("{} entry {}sh @ {:.0}c", hhmmss(unix_now()), fmt_shares(filled), actual_entry * 100.0)],
                opened_at: unix_now(),
            },
        );
    }

    if filled < cfg.shares {
        info!(
            "[ENTER-PARTIAL] {} {} filled {}/{} @~{:.0}c",
            asset.to_uppercase(),
            dir,
            fmt_shares(filled),
            fmt_shares(cfg.shares),
            actual_entry * 100.0
        );
    }

    run_position(http, state, asset, dir, token, actual_entry, filled, market.min_order_size, window_ts, cfg).await;
}

async fn run_position(
    http: reqwest::Client,
    state: State,
    asset: String,
    dir: String,
    token: String,
    entry: f64,
    orig: f64,
    min_order_size: f64,
    window_ts: i64,
    cfg: Config,
) {
    let window_end = window_ts + 300;
    let half = share_round(orig / 2.0);
    let rem = share_round(orig - half);
    let sim = !cfg.live;
    let split_legal = half + SHARE_EPS >= min_order_size && rem + SHARE_EPS >= min_order_size;
    let single_leg_only = !split_legal;

    let mut held = orig;
    let mut realized = 0.0_f64;
    let mut last_exec_price = entry;
    let mut force_logged = false;
    let mut flush_started = false;
    let mut flush_tp_grace_until: Option<i64> = None;
    let mut flush_orders_cancelled = false;
    let mut last_closeout_attempt_at = 0i64;
    let mut panic_attempts: u32 = 0;
    let mut half_sl_done = false;
    let mut full_sl_logged = false;

    let sl_half_price = round4(entry * (1.0 - cfg.sl_half_pct));
    let sl_full_price = round4(entry * (1.0 - cfg.sl_full_pct));

    let mut tp1 = if single_leg_only {
        if orig + SHARE_EPS >= min_order_size {
            if sim {
                Some(TpOrder {
                    id: "SIM-TP1-SINGLE".to_string(),
                    shares: orig,
                    target_price: cfg.tp_half,
                    filled_seen: 0.0,
                })
            } else {
                sell_limit(&cfg, &token, cfg.tp_half, orig).await.map(|id| TpOrder {
                    id,
                    shares: orig,
                    target_price: cfg.tp_half,
                    filled_seen: 0.0,
                })
            }
        } else {
            None
        }
    } else if !shares_zero(half) {
        if sim {
            Some(TpOrder {
                id: "SIM-TP1".to_string(),
                shares: half,
                target_price: cfg.tp_half,
                filled_seen: 0.0,
            })
        } else {
            sell_limit(&cfg, &token, cfg.tp_half, half).await.map(|id| TpOrder {
                id,
                shares: half,
                target_price: cfg.tp_half,
                filled_seen: 0.0,
            })
        }
    } else {
        None
    };

    let mut tp2 = if single_leg_only {
        None
    } else if !shares_zero(rem) {
        if sim {
            Some(TpOrder {
                id: "SIM-TP2".to_string(),
                shares: rem,
                target_price: cfg.tp_full,
                filled_seen: 0.0,
            })
        } else {
            sell_limit(&cfg, &token, cfg.tp_full, rem).await.map(|id| TpOrder {
                id,
                shares: rem,
                target_price: cfg.tp_full,
                filled_seen: 0.0,
            })
        }
    } else {
        None
    };

    if single_leg_only {
        info!(
            "[MODE] {} single-leg management (orig={} min_order_size={})",
            asset.to_uppercase(),
            fmt_shares(orig),
            fmt_shares(min_order_size)
        );
        push_pos_event(&state, &asset, format!("single-leg mode orig={} min_size={}", fmt_shares(orig), fmt_shares(min_order_size)));
    }

    if sim {
        info!(
            "[TP-SIM] {} TP1={:?} TP2={:?}",
            asset.to_uppercase(),
            tp1.as_ref().map(|t| (fmt_shares(t.shares), t.target_price)),
            tp2.as_ref().map(|t| (fmt_shares(t.shares), t.target_price))
        );
    } else {
        info!(
            "[TP] {} TP1={:?} TP2={:?}",
            asset.to_uppercase(),
            tp1.as_ref().map(|t| (&t.id, fmt_shares(t.shares), t.target_price)),
            tp2.as_ref().map(|t| (&t.id, fmt_shares(t.shares), t.target_price))
        );
    }

    loop {
        let now = unix_now();
        let secs_left = (window_end - now).max(0);
        let bid = get_price(&http, &token, "SELL").await.unwrap_or(entry);

        if sim {
            if let Some(tp) = tp1.as_mut() {
                if bid >= tp.target_price && tp.filled_seen < tp.shares {
                    let delta = tp.shares - tp.filled_seen;
                    realized += (tp.target_price - entry) * delta;
                    held = shares_sub(held, delta);
                    tp.filled_seen = tp.shares;
                    last_exec_price = tp.target_price;
                    info!(
                        "[TP-SIM] {} TP1 filled {}sh @ {:.0}c",
                        asset.to_uppercase(),
                        fmt_shares(delta),
                        tp.target_price * 100.0
                    );
                }
            }

            if let Some(tp) = tp2.as_mut() {
                if bid >= tp.target_price && tp.filled_seen < tp.shares {
                    let delta = tp.shares - tp.filled_seen;
                    realized += (tp.target_price - entry) * delta;
                    held = shares_sub(held, delta);
                    tp.filled_seen = tp.shares;
                    last_exec_price = tp.target_price;
                    info!(
                        "[TP-SIM] {} TP2 filled {}sh @ {:.0}c",
                        asset.to_uppercase(),
                        fmt_shares(delta),
                        tp.target_price * 100.0
                    );
                }
            }
        } else if let Some(wallet_held) = token_shares(&cfg, &token).await {
            if wallet_held < held {
                let delta = held - wallet_held;
                attribute_balance_drop(
                    &asset,
                    &mut tp1,
                    &mut tp2,
                    delta,
                    entry,
                    bid,
                    &mut realized,
                    &mut last_exec_price,
                );
                held = wallet_held;
            } else if wallet_held > held {
                warn!(
                    "[RECON] {} wallet shares {} exceed tracked {}, updating held",
                    asset.to_uppercase(),
                    wallet_held,
                    held
                );
                held = wallet_held;
            }
        }

        {
            let mut st = state.lock();
            if let Some(p) = st.open.get_mut(&asset) {
                p.held = held;
                p.current = bid;
                p.realized = round2(realized);
                if sim {
                    p.note = format!("simulated | {}", p.question);
                }
            }
        }

        if shares_zero(held) {
            if !sim {
                cancel_and_pause(&cfg, &active_ids(&tp1, &tp2)).await;
            }
            finalize(&state, &asset, &dir, entry, last_exec_price, orig, "tp", realized);
            return;
        }

        if !sim && held <= cfg.dust_ignore_shares {
            warn!("[DUST] {} remaining {}sh ignored and finalized", asset.to_uppercase(), fmt_shares(held));
            finalize(&state, &asset, &dir, entry, last_exec_price, orig, "dust", realized);
            return;
        }

        if !sim && !shares_zero(held) && (panic_attempts >= cfg.max_panic_attempts || now >= window_end + cfg.max_post_window_manage_secs) {
            let note = format!("cutoff after {} panic attempts / {}s post-window token={} held={} entry={:.0}c", panic_attempts, cfg.max_post_window_manage_secs, token, fmt_shares(held), entry * 100.0);
            warn!("[UNRESOLVED] {} {}", asset.to_uppercase(), note);
            mark_unresolved(&state, &asset, &dir, &token, held, note);
            return;
        }

        if secs_left <= cfg.force_exit_secs && !shares_zero(held) {
            if !flush_started {
                flush_started = true;
                let near_tp = bid >= (cfg.tp_half - cfg.tp_grace_band).max(0.01);
                if near_tp {
                    let grace = cfg.tp_grace_secs.min(secs_left.max(0));
                    flush_tp_grace_until = Some(now + grace);
                    info!(
                        "[FLUSH-ARMED] {} {} held={} bid={:.0}c tp-grace={}s",
                        asset.to_uppercase(),
                        dir,
                        fmt_shares(held),
                        bid * 100.0,
                        grace
                    );
                } else {
                    flush_tp_grace_until = Some(now);
                    info!(
                        "[FLUSH-ARMED] {} {} held={} bid={:.0}c no tp-grace",
                        asset.to_uppercase(),
                        dir,
                        fmt_shares(held),
                        bid * 100.0
                    );
                }
            }

            let grace_active = flush_tp_grace_until
                .map(|until| now < until && secs_left > cfg.panic_exit_secs)
                .unwrap_or(false);

            if !grace_active {
                if !flush_orders_cancelled {
                    cancel_and_pause(&cfg, &active_ids(&tp1, &tp2)).await;
                    tp1 = None;
                    tp2 = None;
                    flush_orders_cancelled = true;
                    info!(
                        "[FLUSH-{}] {} canceled TP orders and started closeout",
                        cfg.force_exit_secs,
                        asset.to_uppercase()
                    );
                }

                if sim {
                    realized += (bid - entry) * held;
                    held = 0.0;
                    last_exec_price = bid;
                    finalize(&state, &asset, &dir, entry, last_exec_price, orig, "force60", realized);
                    return;
                }

                if now - last_closeout_attempt_at >= cfg.closeout_retry_secs {
                    last_closeout_attempt_at = now;
                    panic_attempts += 1;
                    let (remaining, proceeds_floor) = flatten_remaining_live(&http, &cfg, &token, held, min_order_size).await;
                    let sold = shares_sub(held, remaining);
                    realized += proceeds_floor - (entry * sold);
                    held = remaining;
                    if !shares_zero(sold) {
                        last_exec_price = round4(proceeds_floor / sold);
                    }

                    let tag = if secs_left <= cfg.panic_exit_secs {
                        format!("PANIC-{}", cfg.panic_exit_secs)
                    } else {
                        format!("FLUSH-{}", cfg.force_exit_secs)
                    };

                    if shares_zero(held) {
                        push_pos_event(&state, &asset, format!("{} flattened before resolution sold={} avg={:.0}c", tag, fmt_shares(sold), last_exec_price * 100.0));
                        info!(
                            "[{}] {} fully flattened before resolution",
                            tag,
                            asset.to_uppercase()
                        );
                        finalize(&state, &asset, &dir, entry, last_exec_price, orig, "force60", realized);
                        return;
                    }

                    warn!(
                        "[{}] {} still has {}sh open after closeout attempt",
                        tag,
                        asset.to_uppercase(),
                        fmt_shares(held)
                    );
                }
            }

            tokio::time::sleep(Duration::from_millis(cfg.poll_ms)).await;
            continue;
        }

        if !single_leg_only && bid <= sl_half_price && bid > sl_full_price && !half_sl_done && shares_eq(held, orig) {
            if cfg.exits_live && !sim {
                cancel_and_pause(&cfg, &active_ids(&tp1, &tp2)).await;
                tp1 = None;
                tp2 = None;

                let want = half.max(1.0).min(held);
                let (sold, proceeds_floor) = sell_partial_live(&http, &cfg, &token, want, held, min_order_size).await;
                realized += proceeds_floor - (entry * sold);
                held = shares_sub(held, sold);
                if !shares_zero(sold) {
                    last_exec_price = round4(proceeds_floor / sold);
                }

                if shares_zero(sold) {
                    warn!(
                        "[SL-25] {} sold 0sh at trigger — escalating to full protective closeout",
                        asset.to_uppercase()
                    );
                    let (remaining, proceeds_floor2) = flatten_remaining_live(&http, &cfg, &token, held, min_order_size).await;
                    let sold2 = shares_sub(held, remaining);
                    realized += proceeds_floor2 - (entry * sold2);
                    held = remaining;
                    if !shares_zero(sold2) {
                        last_exec_price = round4(proceeds_floor2 / sold2);
                    }
                    half_sl_done = true;
                    if shares_zero(held) {
                        finalize(&state, &asset, &dir, entry, last_exec_price, orig, "sl", realized);
                        return;
                    }
                    warn!(
                        "[PANIC-SL] {} still has {}sh after escalation",
                        asset.to_uppercase(),
                        held
                    );
                } else {
                    half_sl_done = true;
                    push_pos_event(&state, &asset, format!("SL-25 sold {}sh avg={:.0}c held={}", fmt_shares(sold), last_exec_price * 100.0, fmt_shares(held)));
                    warn!(
                        "[SL-25] {} sold {}sh held={} recovery_tp_rearm={}",
                        asset.to_uppercase(),
                        fmt_shares(sold),
                        fmt_shares(held),
                        !shares_zero(held)
                    );

                    if !shares_zero(held) {
                        tp1 = sell_limit(&cfg, &token, cfg.tp_half, held).await.map(|id| TpOrder {
                            id,
                            shares: held,
                            target_price: cfg.tp_half,
                            filled_seen: 0.0,
                        });
                        tp2 = None;
                        push_pos_event(&state, &asset, format!("RECOVERY-TP armed {}sh @ {:.0}c", fmt_shares(held), cfg.tp_half * 100.0));
                        info!(
                            "[RECOVERY-TP] {} re-armed {}sh @ {:.0}c after SL-25",
                            asset.to_uppercase(),
                            fmt_shares(held),
                            cfg.tp_half * 100.0
                        );
                    }
                }
            } else {
                let would_sell = half.max(1.0).min(held);
                warn!(
                    "[{}] would SL-25 sell {}sh @ {:.0}c",
                    if sim { "SIM" } else { "LOG-ONLY" },
                    fmt_shares(would_sell),
                    bid * 100.0
                );
                if sim {
                    realized += (bid - entry) * would_sell;
                    held = shares_sub(held, would_sell);
                    last_exec_price = bid;
                    if !shares_zero(held) {
                        tp1 = Some(TpOrder {
                            id: "SIM-RECOVERY-TP".to_string(),
                            shares: held,
                            target_price: cfg.tp_half,
                            filled_seen: 0.0,
                        });
                        tp2 = None;
                    }
                }
                half_sl_done = true;
            }
        }

        if bid <= sl_full_price && !shares_zero(held) {
            if cfg.exits_live && !sim {
                cancel_and_pause(&cfg, &active_ids(&tp1, &tp2)).await;
                tp1 = None;
                tp2 = None;

                let (remaining, proceeds_floor) = flatten_remaining_live(&http, &cfg, &token, held, min_order_size).await;
                let sold = shares_sub(held, remaining);
                realized += proceeds_floor - (entry * sold);
                held = remaining;
                if !shares_zero(sold) {
                    last_exec_price = round4(proceeds_floor / sold);
                }

                push_pos_event(&state, &asset, format!("SL-35 sold {}sh avg={:.0}c remaining={}", fmt_shares(sold), last_exec_price * 100.0, fmt_shares(held)));
                warn!(
                    "[SL-35] {} sold={} remaining={} est_px={:.0}c",
                    asset.to_uppercase(),
                    fmt_shares(sold),
                    fmt_shares(held),
                    last_exec_price * 100.0
                );

                if shares_zero(held) {
                    finalize(&state, &asset, &dir, entry, last_exec_price, orig, "sl", realized);
                    return;
                }
            } else if !full_sl_logged {
                warn!(
                    "[{}] would SL-35 flatten {}sh @ {:.0}c",
                    if sim { "SIM" } else { "LOG-ONLY" },
                    fmt_shares(held),
                    bid * 100.0
                );
                full_sl_logged = true;
                if sim {
                    realized += (bid - entry) * held;
                    held = 0.0;
                    last_exec_price = bid;
                    finalize(&state, &asset, &dir, entry, last_exec_price, orig, "sl", realized);
                    return;
                }
            }
        }

        if now >= window_end {
            let reason = "resolution";

            if cfg.exits_live && !sim {
                if !flush_orders_cancelled {
                    cancel_and_pause(&cfg, &active_ids(&tp1, &tp2)).await;
                    tp1 = None;
                    tp2 = None;
                    flush_orders_cancelled = true;
                }

                if now - last_closeout_attempt_at >= cfg.closeout_retry_secs {
                    last_closeout_attempt_at = now;
                    panic_attempts += 1;
                    let (remaining, proceeds_floor) = flatten_remaining_live(&http, &cfg, &token, held, min_order_size).await;
                    let sold = shares_sub(held, remaining);
                    realized += proceeds_floor - (entry * sold);
                    held = remaining;
                    if !shares_zero(sold) {
                        last_exec_price = round4(proceeds_floor / sold);
                    }
                }

                if shares_zero(held) {
                    finalize(&state, &asset, &dir, entry, last_exec_price, orig, reason, realized);
                    return;
                }

                warn!(
                    "[{}] {} still has {}sh open after protective exits; continuing management",
                    asset.to_uppercase(),
                    reason,
                    fmt_shares(held)
                );
            } else {
                if !force_logged {
                    warn!(
                        "[{}] would {} flatten {}sh @ {:.0}c",
                        if sim { "SIM" } else { "LOG-ONLY" },
                        reason,
                        held,
                        bid * 100.0
                    );
                    force_logged = true;
                }

                realized += (bid - entry) * held;
                held = 0.0;
                last_exec_price = bid;
                finalize(&state, &asset, &dir, entry, last_exec_price, orig, reason, realized);
                return;
            }
        }

        tokio::time::sleep(Duration::from_millis(cfg.poll_ms)).await;
    }
}

fn attribute_balance_drop(
    asset: &str,
    tp1: &mut Option<TpOrder>,
    tp2: &mut Option<TpOrder>,
    sold_delta: f64,
    entry: f64,
    bid: f64,
    realized: &mut f64,
    last_exec_price: &mut f64,
) {
    let mut remaining = sold_delta;

    if let Some(tp) = tp1.as_mut() {
        let pending = shares_sub(tp.shares, tp.filled_seen);
        if !shares_zero(pending) && !shares_zero(remaining) {
            let filled = pending.min(remaining);
            tp.filled_seen += filled;
            remaining -= filled;
            *realized += (tp.target_price - entry) * filled;
            *last_exec_price = tp.target_price;
            info!(
                "[TP-FILL] {} TP1 {}sh @ {:.0}c (wallet delta)",
                asset.to_uppercase(),
                filled,
                tp.target_price * 100.0
            );
        }
        if tp.filled_seen >= tp.shares {
            *tp1 = None;
        }
    }

    if let Some(tp) = tp2.as_mut() {
        let pending = shares_sub(tp.shares, tp.filled_seen);
        if !shares_zero(pending) && !shares_zero(remaining) {
            let filled = pending.min(remaining);
            tp.filled_seen += filled;
            remaining -= filled;
            *realized += (tp.target_price - entry) * filled;
            *last_exec_price = tp.target_price;
            info!(
                "[TP-FILL] {} TP2 {}sh @ {:.0}c (wallet delta)",
                asset.to_uppercase(),
                filled,
                tp.target_price * 100.0
            );
        }
        if tp.filled_seen >= tp.shares {
            *tp2 = None;
        }
    }

    if !shares_zero(remaining) {
        *realized += (bid - entry) * remaining;
        *last_exec_price = bid;
        warn!(
            "[RECON] {} unassigned wallet sell delta={} @ {:.0}c",
            asset.to_uppercase(),
            remaining,
            bid * 100.0
        );
    }
}

async fn get_price(http: &reqwest::Client, token: &str, side: &str) -> Option<f64> {
    let url = format!("{CLOB_URL}/price?token_id={token}&side={side}");
    let v: serde_json::Value = http
        .get(&url)
        .timeout(Duration::from_secs(3))
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;

    let price = v
        .get("price")
        .and_then(|x| x.as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| x.as_f64()))?;

    if price > 0.01 {
        Some(price)
    } else {
        None
    }
}

async fn find_market(http: &reqwest::Client, asset: &str, window_ts: i64) -> Option<MarketInfo> {
    let slug = format!("{asset}-updown-5m-{window_ts}");
    let url = format!("{GAMMA_URL}/markets?slug={slug}");

    let resp = match http.get(&url).timeout(Duration::from_secs(6)).send().await {
        Ok(r) => r,
        Err(e) => {
            warn!("[MKT] {} request error: {}", asset, e);
            return None;
        }
    };

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        warn!("[MKT] {} HTTP {} body={}", asset, status, body);
        return None;
    }

    let v: serde_json::Value = serde_json::from_str(&body).ok()?;
    let market = v.as_array()?.first()?;

    let ids = parse_string_array_field(market, "clobTokenIds")?;
    let outcomes = parse_string_array_field(market, "outcomes")?;
    if ids.len() != outcomes.len() || ids.len() < 2 {
        return None;
    }

    let mut yes_token = None;
    let mut no_token = None;

    for (outcome, token_id) in outcomes.iter().zip(ids.iter()) {
        match outcome.to_lowercase().as_str() {
            "yes" | "up" => yes_token = Some(token_id.clone()),
            "no" | "down" => no_token = Some(token_id.clone()),
            _ => {}
        }
    }

    if yes_token.is_none() || no_token.is_none() {
        if ids.len() >= 2 {
            yes_token.get_or_insert_with(|| ids[0].clone());
            no_token.get_or_insert_with(|| ids[1].clone());
        }
    }

    let question = market
        .get("question")
        .and_then(|x| x.as_str())
        .unwrap_or(&slug)
        .to_string();

    let min_order_size = market
        .get("orderMinSize")
        .and_then(|x| x.as_f64().or_else(|| x.as_str().and_then(|s| s.parse::<f64>().ok())))
        .or_else(|| market.get("minimum_order_size").and_then(|x| x.as_f64().or_else(|| x.as_str().and_then(|s| s.parse::<f64>().ok()))))
        .unwrap_or(1.0);

    match (yes_token, no_token) {
        (Some(yes), Some(no)) => Some(MarketInfo {
            yes_token: yes,
            no_token: no,
            question,
            min_order_size,
        }),
        _ => None,
    }
}

fn parse_string_array_field(v: &serde_json::Value, key: &str) -> Option<Vec<String>> {
    if let Some(s) = v.get(key).and_then(|x| x.as_str()) {
        serde_json::from_str::<Vec<String>>(s).ok()
    } else if let Some(arr) = v.get(key).and_then(|x| x.as_array()) {
        Some(
            arr.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect(),
        )
    } else {
        None
    }
}

async fn binance_ws(prices: PriceMap) {
    let map: HashMap<String, String> = binance_symbols()
        .into_iter()
        .map(|(asset, symbol)| (symbol.to_string(), asset.to_string()))
        .collect();

    let streams: Vec<String> = binance_symbols()
        .into_iter()
        .map(|(_, symbol)| format!("{}@bookTicker", symbol.to_lowercase()))
        .collect();

    let url = format!("{}?streams={}", BINANCE_WS_BASE, streams.join("/"));

    loop {
        if let Ok((ws, _)) = connect_async(&url).await {
            let (_, mut reader) = ws.split();
            while let Some(Ok(msg)) = reader.next().await {
                if let Ok(text) = msg.to_text() {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(text) {
                        let d = &v["data"];
                        let sym = d["s"].as_str().unwrap_or("");
                        let bid = d["b"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                        let ask = d["a"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                        let mid = if bid > 0.0 && ask > 0.0 { (bid + ask) / 2.0 } else { bid.max(ask) };
                        if mid > 0.0 {
                            if let Some(asset) = map.get(sym) {
                                prices.insert(asset.clone(), mid);
                            }
                        }
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn hyperliquid_ws(prices: PriceMap) {
    loop {
        if let Ok((ws, _)) = connect_async(HYPERLIQUID_WS).await {
            let (mut writer, mut reader) = ws.split();
            let sub = serde_json::json!({
                "method": "subscribe",
                "subscription": {"type": "trades", "coin": "HYPE"}
            });
            let _ = writer.send(Message::Text(sub.to_string().into())).await;

            while let Some(Ok(msg)) = reader.next().await {
                if let Ok(text) = msg.to_text() {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(text) {
                        if v["channel"] == "trades" {
                            if let Some(arr) = v["data"].as_array() {
                                for tr in arr {
                                    if tr["coin"].as_str() == Some("HYPE") {
                                        let px = tr["px"]
                                            .as_str()
                                            .and_then(|s| s.parse::<f64>().ok())
                                            .or_else(|| tr["px"].as_f64())
                                            .unwrap_or(0.0);
                                        if px > 0.0 {
                                            prices.insert("hype".to_string(), px);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

fn dashboard(
    snap: &HashMap<String, f64>,
    opens: &HashMap<String, f64>,
    state: &State,
    cfg: &Config,
    window_ts: i64,
    elapsed: i64,
    secs_left: i64,
) -> String {
    let (trades, open_pos, unresolved_pos, wins, losses, total_pnl, risked, note, start, cached, recent_skips, recent_signals) = {
        let st = state.lock();
        (
            st.trades.clone(),
            st.open.values().cloned().collect::<Vec<_>>(),
            st.unresolved.values().cloned().collect::<Vec<_>>(),
            st.wins,
            st.losses,
            st.total_pnl,
            st.total_risked,
            st.last_trade_info.clone(),
            st.session_start,
            st.market_cache.len(),
            st.recent_skips.clone(),
            st.recent_signals.clone(),
        )
    };

    let tc = wins + losses;
    let wr = if tc > 0 { wins as f64 / tc as f64 * 100.0 } else { 0.0 };
    let roi = if risked > 0.0 { total_pnl / risked * 100.0 } else { 0.0 };
    let sess = ((unix_now() - start) / 300) + 1;
    let bar = "=".repeat(64);
    let dash = "-".repeat(64);
    let mode = if cfg.live { "LIVE" } else { "DRY" };
    let ex = if cfg.exits_live { "EXITS:LIVE" } else { "EXITS:LOG" };

    let mut live_ranges: Vec<String> = Vec::new();
    for (asset, _) in display_assets() {
        if let (Some(now_px), Some(open_px)) = (snap.get(asset).copied(), opens.get(asset).copied()) {
            if open_px > 0.0 {
                let mv = (now_px - open_px) / open_px * 100.0;
                let thr = cfg.threshold_for(asset);
                if mv.abs() >= thr {
                    live_ranges.push(format!(
                        "{} {} {:+.3}% thr={:.3}%",
                        asset.to_uppercase(),
                        if mv > 0.0 { "UP" } else { "DOWN" },
                        mv,
                        thr
                    ));
                }
            }
        }
    }
    live_ranges.sort_by(|a, b| a.cmp(b));

    let mut s = String::with_capacity(6144);
    s.push_str(&format!(
        "{bar}\n  Polymarket V2 Sniper — {mode} {ex} — {}Z\n{bar}\n",
        hhmmss(unix_now())
    ));
    s.push_str(&format!(
        "  Sess:{:>2} Win:{}Z El:{:>3}s Left:{:>3}s Mkts:{}/7\n  Trades:{} W:{} L:{} WR:{:.0}% P&L:${:+.2} ROI:{:+.1}% Risked:${:.2}\n  Entry:{:.0}-{:.0}c Cap:{:.0}c TP:{:.0}/{:.0}c SL:-{:.0}/-{:.0}% Shares:{}\n{dash}\n",
        sess,
        hhmm(window_ts),
        elapsed,
        secs_left,
        cached,
        tc,
        wins,
        losses,
        wr,
        total_pnl,
        roi,
        risked,
        cfg.min_entry * 100.0,
        cfg.max_entry * 100.0,
        cfg.hard_max_entry * 100.0,
        cfg.tp_half * 100.0,
        cfg.tp_full * 100.0,
        cfg.sl_half_pct * 100.0,
        cfg.sl_full_pct * 100.0,
        fmt_shares(cfg.shares)
    ));

    for (asset, src) in display_assets() {
        let thr = cfg.threshold_for(asset);
        match (snap.get(asset).copied(), opens.get(asset).copied()) {
            (Some(now_px), Some(open_px)) if open_px > 0.0 => {
                let mv = (now_px - open_px) / open_px * 100.0;
                let scl = if mv.abs() >= thr {
                    if mv > 0.0 { "SCL UP" } else { "SCL DN" }
                } else {
                    ""
                };
                s.push_str(&format!(
                    "  {:<5}[{}] open={:>10.4} now={:>10.4} mv={:+.3}% thr={:.3}% {}\n",
                    asset.to_uppercase(),
                    src,
                    open_px,
                    now_px,
                    mv,
                    thr,
                    scl
                ));
            }
            _ => s.push_str(&format!("  {:<5}[{}] waiting...\n", asset.to_uppercase(), src)),
        }
    }

    if !open_pos.is_empty() {
        s.push_str(&format!("{dash}\n"));
        for p in &open_pos {
            let upnl = round2(p.realized + p.held * (p.current - p.entry));
            s.push_str(&format!(
                "  > {:<4} {} entry={:.0}c now={:.0}c held={}/{} realized=${:+.2} uP&L=${:+.2} {}\n",
                p.asset.to_uppercase(),
                p.dir,
                p.entry * 100.0,
                p.current * 100.0,
                fmt_shares(p.held),
                fmt_shares(p.orig),
                p.realized,
                upnl,
                p.note
            ));
        }
    }

    s.push_str(&format!("{dash}\n  Live range signals:\n"));
    if live_ranges.is_empty() {
        s.push_str("  - none\n");
    } else {
        for line in live_ranges.iter().take(6) {
            s.push_str(&format!("  - {}\n", line));
        }
    }

    s.push_str("  Recent skipped:\n");
    if recent_skips.is_empty() {
        s.push_str("  - none\n");
    } else {
        for line in recent_skips.iter().rev().take(5).rev() {
            s.push_str(&format!("  - {}\n", line));
        }
    }

    s.push_str("  Recent signal checks:\n");
    if recent_signals.is_empty() {
        s.push_str("  - none\n");
    } else {
        for line in recent_signals.iter().rev().take(5).rev() {
            s.push_str(&format!("  - {}\n", line));
        }
    }

    s.push_str("  Unresolved / manual attention:\n");
    if unresolved_pos.is_empty() {
        s.push_str("  - none\n");
    } else {
        for u in unresolved_pos.iter().take(5) {
            s.push_str(&format!("  - {} {} {}sh {}\n", u.asset.to_uppercase(), u.dir, fmt_shares(u.shares), u.note));
        }
    }

    s.push_str(&format!("{dash}\n  Traded: {note}\n"));
    for t in trades.iter().rev().take(5).rev() {
        s.push_str(&format!(
            "  {} {:<4} {:<3} {:.0}c->{:.0}c {}sh {:<4} ${:+.2} {}\n",
            t.time,
            t.asset,
            t.dir,
            t.entry * 100.0,
            t.exit * 100.0,
            fmt_shares(t.shares),
            t.result,
            t.pnl,
            t.reason
        ));
    }
    s.push_str(&format!("{bar}\n"));
    s
}

fn finalize(
    state: &State,
    asset: &str,
    dir: &str,
    entry: f64,
    exit: f64,
    shares: f64,
    reason: &str,
    realized: f64,
) {
    let mut st = state.lock();
    let pnl = round2(realized);
    let closed_at = hhmm(unix_now());
    let removed = st.open.remove(asset);

    if pnl >= 0.0 {
        st.wins += 1;
    } else {
        st.losses += 1;
    }

    st.total_pnl = round2(st.total_pnl + pnl);
    st.last_trade_info = format!("NO — watching (last: {} ${:+.2})", reason, pnl);
    st.unresolved.remove(asset);
    st.trades.push(Trade {
        time: closed_at.clone(),
        asset: asset.to_uppercase(),
        dir: dir.to_string(),
        entry,
        exit,
        shares,
        result: if pnl >= 0.0 { "WIN" } else { "LOSS" }.to_string(),
        pnl,
        reason: reason.to_string(),
    });

    let ledger_path = st.trade_ledger_file.clone();
    let (note, events) = if let Some(pos) = removed {
        (pos.note, pos.events)
    } else {
        (String::new(), Vec::new())
    };

    append_trade_ledger(&ledger_path, &serde_json::json!({
        "time": closed_at,
        "asset": asset.to_uppercase(),
        "dir": dir,
        "entry": round4(entry),
        "exit": round4(exit),
        "shares": share_round(shares),
        "result": if pnl >= 0.0 { "WIN" } else { "LOSS" },
        "pnl": pnl,
        "reason": reason,
        "note": note,
        "events": events,
        "logged_at": chrono::Utc::now().to_rfc3339(),
    }));
}

fn mark_unresolved(state: &State, asset: &str, dir: &str, token: &str, shares: f64, note: String) {
    let mut st = state.lock();
    st.open.remove(asset);
    st.unresolved.insert(asset.to_string(), UnresolvedPos {
        asset: asset.to_string(),
        dir: dir.to_string(),
        token: token.to_string(),
        shares,
        note: note.clone(),
        since: unix_now(),
    });
    st.last_trade_info = format!("UNRESOLVED — {} {} {}sh", asset.to_uppercase(), dir, fmt_shares(shares));
    push_recent(&mut st.recent_skips, format!("{} UNRESOLVED {} {} {}", hhmmss(unix_now()), asset.to_uppercase(), dir, note), 8);
    let ledger_path = st.trade_ledger_file.clone();
    append_trade_ledger(&ledger_path, &serde_json::json!({
        "time": hhmm(unix_now()),
        "asset": asset.to_uppercase(),
        "dir": dir,
        "shares": share_round(shares),
        "result": "UNRESOLVED",
        "reason": "unresolved",
        "note": note,
        "logged_at": chrono::Utc::now().to_rfc3339(),
    }));
}

const SHARE_EPS: f64 = 0.000_001;

fn share_round(x: f64) -> f64 {
    (x * 1_000_000.0).round() / 1_000_000.0
}

fn shares_zero(x: f64) -> bool {
    x.abs() <= SHARE_EPS
}

fn shares_eq(a: f64, b: f64) -> bool {
    (a - b).abs() <= SHARE_EPS
}

fn shares_sub(a: f64, b: f64) -> f64 {
    share_round((a - b).max(0.0))
}

fn fmt_shares(x: f64) -> String {
    let y = share_round(x);
    if (y.fract()).abs() < SHARE_EPS {
        format!("{:.0}", y)
    } else if ((y * 10.0).fract()).abs() < SHARE_EPS {
        format!("{:.1}", y)
    } else if ((y * 100.0).fract()).abs() < SHARE_EPS {
        format!("{:.2}", y)
    } else {
        format!("{:.3}", y)
    }
}

fn active_ids(tp1: &Option<TpOrder>, tp2: &Option<TpOrder>) -> Vec<String> {
    let mut ids = Vec::new();
    if let Some(tp) = tp1 {
        if !tp.id.is_empty() && !tp.id.starts_with("SIM-") {
            ids.push(tp.id.clone());
        }
    }
    if let Some(tp) = tp2 {
        if !tp.id.is_empty() && !tp.id.starts_with("SIM-") {
            ids.push(tp.id.clone());
        }
    }
    ids
}

fn push_recent(list: &mut Vec<String>, msg: String, keep: usize) {
    list.push(msg);
    if list.len() > keep {
        let drop_n = list.len() - keep;
        list.drain(0..drop_n);
    }
}

fn push_skip(state: &State, msg: String) {
    let mut st = state.lock();
    push_recent(&mut st.recent_skips, format!("{} {}", hhmmss(unix_now()), msg), 8);
}

fn push_signal(state: &State, msg: String) {
    let mut st = state.lock();
    push_recent(&mut st.recent_signals, format!("{} {}", hhmmss(unix_now()), msg), 8);
}

fn push_pos_event(state: &State, asset: &str, msg: String) {
    let mut st = state.lock();
    if let Some(pos) = st.open.get_mut(asset) {
        pos.events.push(format!("{} {}", hhmmss(unix_now()), msg));
        if pos.events.len() > 20 {
            let drop_n = pos.events.len() - 20;
            pos.events.drain(0..drop_n);
        }
    }
}

fn append_trade_ledger(path: &str, value: &serde_json::Value) {
    if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(f, "{}", value);
    }
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "y" | "on"))
        .unwrap_or(default)
}

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn env_i64(name: &str, default: i64) -> i64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn unix_now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

fn round4(x: f64) -> f64 {
    (x * 10000.0).round() / 10000.0
}

fn hhmm(ts: i64) -> String {
    format!("{:02}:{:02}", (ts % 86400) / 3600, (ts % 3600) / 60)
}

fn hhmmss(ts: i64) -> String {
    format!(
        "{:02}:{:02}:{:02}",
        (ts % 86400) / 3600,
        (ts % 3600) / 60,
        ts % 60
    )
}
