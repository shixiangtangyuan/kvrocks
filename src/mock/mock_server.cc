#include "mock_server.h"

#include <event2/thread.h>

#include <cstdlib>
#include <filesystem>

#include "cluster/cluster.h"
#include "common/io_util.h"
#include "vendor/crc64.h"

CLIOptions MockOptions::GetCliOptions() {
  // TODO(ying.qiu): generate available ports
  if (!mkdtemp(root_dir.data())) {
    std::cerr << "Create root dir failed, pattern:" << root_dir << std::endl;
    exit(1);
  }

  std::string log_dir = root_dir + "/logs";
  if (!std::filesystem::create_directory(log_dir)) {
    std::cerr << "Create log dir failed, log_dir:" << log_dir << std::endl;
    exit(1);
  }
  std::stringstream ss;
  for (const auto id : db_ids) {
    std::string volume_dir = root_dir + "/" + std::to_string(id);
    if (!std::filesystem::create_directory(volume_dir)) {
      std::cerr << "Create volume dir failed, db_id:" << id << std::endl;
      exit(1);
    }
    ss << volume_dir << "\t";
  }
  auto data_dirs = ss.str();
  if (!data_dirs.empty() && data_dirs.back() == '\t') {
    data_dirs.pop_back();
  }

  CLIOptions cli_opts;
  cli_opts.cli_options.emplace_back("workers", std::to_string(workers));
  cli_opts.cli_options.emplace_back("controller-heartbeat-interval-milliseconds",
                                    std::to_string(heartbeat_interval_milliseconds));
  cli_opts.cli_options.emplace_back("port", std::to_string(port));
  cli_opts.cli_options.emplace_back("log-dir", log_dir);
  cli_opts.cli_options.emplace_back("datadir-list", data_dirs);
  cli_opts.cli_options.emplace_back("controller-addr", controller_addr);
  cli_opts.cli_options.emplace_back("datanode-id", datanode_id);
  cli_opts.cli_options.emplace_back("cluster-id", cluster_id);
  cli_opts.cli_options.emplace_back("pool", pool);
  cli_opts.cli_options.emplace_back("log-level", log_level);
  cli_opts.cli_options.emplace_back("vlog-level", std::to_string(vlog_level));
  cli_opts.cli_options.emplace_back("grpc-client-initial-reconnect-backoff-ms",
                                    std::to_string(grpc_client_initial_reconnect_backoff_ms));
  return cli_opts;
}

MockServer::MockServer(MockOptions opts) : opts_(std::move(opts)) {
  if (auto status = start(); !status.IsOK()) {
    std::cerr << "Start mock server failed, status:" << status.Msg() << std::endl;
    exit(1);
  }
}

MockServer::~MockServer() {
  bool running = false;
  if (srv_ && stopped_.compare_exchange_strong(running, true)) {
    srv_->Stop();
    srv_->Join();
  }

  std::error_code err;
  std::filesystem::remove_all(opts_.root_dir, err);
  if (err) {
    std::cerr << "Remove root dir failed, error=" << err << std::endl;
  }
}

static void InitGoogleLog(const Config *config) {
  FLAGS_minloglevel = config->log_level;
  FLAGS_v = config->vlog_level;
  FLAGS_max_log_size = 100;
  FLAGS_logbufsecs = 0;

  if (util::EqualICase(config->log_dir, "stdout")) {
    for (int level = google::INFO; level <= google::FATAL; level++) {
      google::SetLogDestination(level, "");
    }
    FLAGS_stderrthreshold = google::ERROR;
    FLAGS_logtostdout = true;
  } else {
    FLAGS_log_dir = config->log_dir + "/";
    if (config->log_retention_days != -1) {
      google::EnableLogCleaner(config->log_retention_days);
    }
  }
}

Status MockServer::start() {
  auto status = cfg_.Load(opts_.GetCliOptions());
  if (!status.IsOK()) {
    return status;
  }

  crc64_init();
  InitGoogleLog(&cfg_);
  evthread_use_pthreads();
  if (!cfg_.binds.empty()) {
    uint32_t ports[] = {cfg_.port, cfg_.GetGrpcPort(), cfg_.tls_port, 0};
    for (uint32_t *port = ports; *port; ++port) {
      if (util::IsPortInUse(*port)) {
        std::cerr << "Port already in use, port:" << *port << std::endl;
        exit(1);
      }
    }
  }

  srv_ = std::make_shared<Server>(&cfg_);
  return srv_->Start();
}
