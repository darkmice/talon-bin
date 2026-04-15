/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 */
//! Talon full bundle server binary.
//!
//! Includes core + AI + LLM + Agent + Trace + Sandbox + EvoCore.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

fn main() {
    init_extensions();
    print_legal_banner();
    let args: Vec<String> = std::env::args().collect();
    let mut data_path = "talon_data".to_string();
    let mut addr = "127.0.0.1:7720".to_string();
    let mut tcp_addr: Option<String> = None;
    let mut pg_addr: Option<String> = None;
    let mut token: Option<String> = None;
    let mut role_str = "standalone".to_string();
    let mut repl_addr: Option<String> = None;
    let mut repl_token: Option<String> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--data" => {
                i += 1;
                if i < args.len() {
                    data_path = args[i].clone();
                }
            }
            "--addr" => {
                i += 1;
                if i < args.len() {
                    addr = args[i].clone();
                }
            }
            "--tcp-addr" => {
                i += 1;
                if i < args.len() {
                    tcp_addr = Some(args[i].clone());
                }
            }
            "--pg-addr" => {
                i += 1;
                if i < args.len() {
                    pg_addr = Some(args[i].clone());
                }
            }
            "--token" => {
                i += 1;
                if i < args.len() {
                    token = Some(args[i].clone());
                }
            }
            "--role" => {
                i += 1;
                if i < args.len() {
                    role_str = args[i].clone();
                }
            }
            "--repl-addr" => {
                i += 1;
                if i < args.len() {
                    repl_addr = Some(args[i].clone());
                }
            }
            "--repl-token" => {
                i += 1;
                if i < args.len() {
                    repl_token = Some(args[i].clone());
                }
            }
            "--help" | "-h" => {
                print_help();
                return;
            }
            _ => {
                eprintln!("未知参数: {}", args[i]);
                std::process::exit(1);
            }
        }
        i += 1;
    }

    let cluster_role = match role_str.as_str() {
        "standalone" => talon::ClusterRole::Standalone,
        "primary" => talon::ClusterRole::Primary,
        "replica" => talon::ClusterRole::Replica {
            primary_addr: repl_addr.clone().unwrap_or_default(),
        },
        _ => {
            eprintln!("无效角色: {}（可选: standalone/primary/replica）", role_str);
            std::process::exit(1);
        }
    };

    if matches!(cluster_role, talon::ClusterRole::Primary) && repl_addr.is_none() {
        eprintln!("Primary 模式需要 --repl-addr 指定复制监听地址");
        std::process::exit(1);
    }
    if matches!(cluster_role, talon::ClusterRole::Replica { .. }) && repl_addr.is_none() {
        eprintln!("Replica 模式需要 --repl-addr 指定主节点地址");
        std::process::exit(1);
    }

    let cluster_config = talon::ClusterConfig {
        role: cluster_role.clone(),
        replication_addr: repl_addr.clone().unwrap_or_else(|| "0.0.0.0:7721".into()),
        replication_token: repl_token.clone(),
        ..Default::default()
    };

    let db = match talon::Talon::open_with_cluster(
        &data_path,
        talon::StorageConfig::default(),
        cluster_config.clone(),
    ) {
        Ok(db) => Arc::new(db),
        Err(e) => {
            eprintln!("打开数据库失败: {}", e);
            std::process::exit(1);
        }
    };

    let config = talon::ServerConfig {
        http_addr: addr.clone(),
        auth_token: token,
        ..Default::default()
    };

    println!("Talon Server 启动中...");
    println!("  数据目录: {}", data_path);
    println!("  HTTP 地址: {}", addr);
    if let Some(ref ta) = tcp_addr {
        println!("  TCP  地址: {}", ta);
    }
    if let Some(ref pa) = pg_addr {
        println!("  PgWire 地址: {}", pa);
    }
    println!("  角色: {:?}", cluster_role);
    if let Some(ref ra) = repl_addr {
        println!("  复制地址: {}", ra);
    }
    println!("  扩展: ai + llm + agent + trace + sandbox + evocore");
    println!(
        "  认证: {}",
        if config.auth_token.is_some() {
            "已启用"
        } else {
            "未启用"
        }
    );

    let _ts_cleaner = talon::start_ts_retention_cleaner(db.store(), 60);
    let _ttl_cleaner = if !cluster_role.is_readonly() {
        match db.kv() {
            Ok(guard) => Some(guard.start_ttl_cleaner(60)),
            Err(e) => {
                eprintln!("KV TTL 清理线程启动失败: {}", e);
                None
            }
        }
    } else {
        None
    };

    let stop = Arc::new(AtomicBool::new(false));
    let _repl_handle = start_replication(&db, &cluster_config, Arc::clone(&stop));

    let tcp_handle = tcp_addr.map(|ta| {
        let tcp_server = talon::TcpServer::new(config.clone(), Arc::clone(&db), ta);
        let stop2 = Arc::clone(&stop);
        std::thread::spawn(move || {
            if let Err(e) = tcp_server.run(stop2) {
                eprintln!("TCP Server 错误: {}", e);
            }
        })
    });

    #[cfg(feature = "pgwire-server")]
    let pg_handle = pg_addr.map(|pa| {
        let db_pg = Arc::clone(&db);
        std::thread::spawn(move || match tokio::runtime::Runtime::new() {
            Ok(rt) => rt.block_on(async move {
                talon::server::pgwire::run_pgwire_server(db_pg, &pa).await;
            }),
            Err(e) => eprintln!("PgWire 运行时启动失败: {}", e),
        })
    });
    #[cfg(not(feature = "pgwire-server"))]
    let pg_handle: Option<std::thread::JoinHandle<()>> = None;

    let server = talon::HttpServer::new(config, db);
    if let Err(e) = server.run() {
        eprintln!("HTTP Server 错误: {}", e);
    }

    stop.store(true, Ordering::Relaxed);
    if let Some(h) = _repl_handle {
        let _ = h.join();
    }
    if let Some(h) = tcp_handle {
        let _ = h.join();
    }
    if let Some(h) = pg_handle {
        let _ = h.join();
    }
}

fn init_extensions() {
    talon::talon_bundle_init_ai();
    talon::talon_bundle_init_evo();
}

fn start_replication(
    db: &Arc<talon::Talon>,
    config: &talon::ClusterConfig,
    stop: Arc<AtomicBool>,
) -> Option<std::thread::JoinHandle<()>> {
    match &config.role {
        talon::ClusterRole::Primary => {
            let oplog = match db.oplog_arc() {
                Some(o) => o,
                None => {
                    eprintln!("Primary 模式但 OpLog 未初始化");
                    return None;
                }
            };
            let sender = talon::ReplSender::new(
                config.clone(),
                oplog,
                db.store().clone(),
                Arc::clone(&stop),
            )
            .with_replicas(db.replica_infos_arc());
            println!("  复制: ReplSender 监听 {}", config.replication_addr);
            Some(std::thread::spawn(move || {
                if let Err(e) = sender.run() {
                    eprintln!("ReplSender 错误: {}", e);
                }
            }))
        }
        talon::ClusterRole::Replica { primary_addr } => {
            let addr = primary_addr.clone();
            let token = config.replication_token.clone();
            let timeout = config.replication_timeout_secs;
            let db2 = Arc::clone(db);
            let stop2 = Arc::clone(&stop);
            let replayer = Arc::new(talon::Replayer::new(0));

            let receiver = talon::ReplReceiver::new(
                addr.clone(),
                token,
                timeout,
                stop2,
                0,
                Box::new(move |entry| replayer.replay_one(&db2, entry).map(|_| ())),
            )
            .with_store(db.store().clone());

            println!("  复制: ReplReceiver 连接 {}", addr);
            Some(std::thread::spawn(move || {
                if let Err(e) = receiver.run() {
                    eprintln!("ReplReceiver 退出: {}", e);
                }
            }))
        }
        talon::ClusterRole::Standalone => None,
    }
}

fn print_help() {
    println!("Talon — 全量扩展服务端 (core + ai + llm + agent + trace + sandbox + evocore)");
    println!();
    println!("用法: talon [选项]");
    println!();
    println!("选项:");
    println!("  --data <path>           数据目录 (默认: talon_data)");
    println!("  --addr <host:port>      HTTP 监听地址 (默认: 127.0.0.1:7720)");
    println!("  --tcp-addr <host:port>  TCP 二进制协议监听地址 (可选)");
    println!("  --pg-addr <host:port>   PgWire 兼容协议监听地址 (可选)");
    println!("  --token <token>         认证 token (默认: 无认证)");
    println!("  --role <role>           集群角色: standalone/primary/replica (默认: standalone)");
    println!("  --repl-addr <host:port> 复制地址 (Primary: 监听; Replica: 主节点地址)");
    println!("  --repl-token <token>    复制认证 token (可选)");
    println!("  --help, -h              显示帮助");
}

fn print_legal_banner() {
    println!("--------------------------------------------------");
    println!("Talon v0.1.0 - AI-Native Data Engine");
    println!("Flavor: full bundle (core + ai + llm + agent + trace + sandbox + evocore)");
    println!("License: Talon Community Dual License Agreement (SSPL / Commercial)");
    println!("Notice: Commercial SaaS/Embedding requires a license.");
    println!("Legal: By using this software, you agree to the AI Disclaimer.");
    println!("--------------------------------------------------");
}
