// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "storage/lake/segment_warmup_manager.h"

#include <brpc/controller.h>

#include "cache/block_cache/block_cache.h"
#include "common/config.h"
#include "common/logging.h"
#include "fs/fs.h"
#include "gen_cpp/lake_service.pb.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec_env.h"
#include "service/brpc.h"
#include "storage/lake/filenames.h"
#include "storage/lake/tablet_manager.h"
#include "util/brpc_stub_cache.h"
#include "util/defer_op.h"
#include "util/threadpool.h"

namespace starrocks::lake {

SegmentWarmupManager::SegmentWarmupManager(ExecEnv* env, TabletManager* tablet_mgr)
        : _env(env), _tablet_mgr(tablet_mgr) {
    // Parse peer nodes at construction time
    _peer_nodes_config_cache = config::lake_segment_warmup_peer_nodes;
    _peer_nodes = parse_peer_nodes(_peer_nodes_config_cache);
    
    if (!_peer_nodes.empty()) {
        LOG(INFO) << "SegmentWarmupManager initialized with " << _peer_nodes.size() << " peer nodes";
    }
}

SegmentWarmupManager::~SegmentWarmupManager() = default;

bool SegmentWarmupManager::should_apply_backpressure() const {
    // Check if pending segments exceed threshold
    if (_pending_segment_count.load(std::memory_order_relaxed) >= config::lake_segment_warmup_max_pending_segments) {
        return true;
    }

    // Check if pending memory exceeds threshold
    if (_pending_memory_bytes.load(std::memory_order_relaxed) >= config::lake_segment_warmup_max_pending_memory_mb * 1024 * 1024) {
        return true;
    }

    return false;
}

std::vector<std::pair<std::string, int>> SegmentWarmupManager::parse_peer_nodes(const std::string& config_str) {
    std::vector<std::pair<std::string, int>> peer_nodes;
    
    if (config_str.empty()) {
        return peer_nodes;
    }

    // Parse comma-separated list: "host1:port1,host2:port2,..."
    size_t start = 0;
    while (start < config_str.size()) {
        size_t comma_pos = config_str.find(',', start);
        size_t end = (comma_pos == std::string::npos) ? config_str.size() : comma_pos;
        
        std::string node_str = config_str.substr(start, end - start);
        
        // Trim whitespace
        size_t first = node_str.find_first_not_of(" \t\r\n");
        size_t last = node_str.find_last_not_of(" \t\r\n");
        if (first != std::string::npos && last != std::string::npos) {
            node_str = node_str.substr(first, last - first + 1);
        }
        
        if (!node_str.empty()) {
            // Parse "host:port"
            size_t colon_pos = node_str.find(':');
            if (colon_pos != std::string::npos) {
                std::string host = node_str.substr(0, colon_pos);
                std::string port_str = node_str.substr(colon_pos + 1);
                try {
                    int port = std::stoi(port_str);
                    if (port > 0 && port <= 65535) {
                        peer_nodes.emplace_back(host, port);
                        VLOG(3) << "Parsed peer node: " << host << ":" << port;
                    } else {
                        LOG(WARNING) << "Invalid port in peer node config: " << node_str;
                    }
                } catch (const std::exception& e) {
                    LOG(WARNING) << "Failed to parse port in peer node config: " << node_str 
                                 << " error=" << e.what();
                }
            } else {
                LOG(WARNING) << "Invalid peer node format (expected host:port): " << node_str;
            }
        }
        
        if (comma_pos == std::string::npos) {
            break;
        }
        start = comma_pos + 1;
    }

    return peer_nodes;
}

std::vector<std::pair<std::string, int>> SegmentWarmupManager::get_peer_nodes() {
    std::string current_config = config::lake_segment_warmup_peer_nodes;
    
    // Fast path: if config hasn't changed, return cached result
    {
        std::lock_guard<std::mutex> lock(_peer_nodes_mutex);
        if (current_config == _peer_nodes_config_cache) {
            return _peer_nodes;
        }
    }
    
    // Config changed, re-parse
    auto new_peer_nodes = parse_peer_nodes(current_config);
    
    {
        std::lock_guard<std::mutex> lock(_peer_nodes_mutex);
        _peer_nodes_config_cache = current_config;
        _peer_nodes = new_peer_nodes;
        
        LOG(INFO) << "Peer nodes config changed. New peer count: " << _peer_nodes.size()
                  << " config: " << current_config;
    }
    
    return new_peer_nodes;
}

Status SegmentWarmupManager::warm_up_segment(int64_t tablet_id, const std::string& segment_path,
                                              int64_t warehouse_id) {
    // Check if warmup is enabled
    if (!config::lake_enable_segment_warmup) {
        return Status::OK();
    }

    // Check backpressure
    if (should_apply_backpressure()) {
        _backpressure_skip_count.fetch_add(1, std::memory_order_relaxed);
        VLOG(2) << "Skip segment warmup due to backpressure. tablet_id=" << tablet_id
                << " segment_path=" << segment_path << " pending_segments=" << _pending_segment_count.load()
                << " pending_memory_mb=" << (_pending_memory_bytes.load() / 1024 / 1024);
        return Status::OK();
    }

    _total_warmup_requests.fetch_add(1, std::memory_order_relaxed);

    // Get peer CN nodes (cached, auto-refreshed on config change)
    std::vector<std::pair<std::string, int>> peer_nodes = get_peer_nodes();
    if (peer_nodes.empty()) {
        VLOG(3) << "No peer nodes configured for warmup. tablet_id=" << tablet_id 
                << " warehouse_id=" << warehouse_id;
        return Status::OK();
    }

    // Increment pending segment count
    _pending_segment_count.fetch_add(1, std::memory_order_relaxed);
    DeferOp defer([this]() { _pending_segment_count.fetch_sub(1, std::memory_order_relaxed); });

    // Warmup segment blocks
    Status st = warmup_segment_blocks(tablet_id, segment_path, peer_nodes);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to warmup segment blocks. tablet_id=" << tablet_id << " segment_path=" << segment_path
                     << " error=" << st;
    }

    return st;
}

void SegmentWarmupManager::warm_up_segment_async(int64_t tablet_id, std::string segment_path,
                                                  int64_t warehouse_id) {
    // Check if warmup is enabled
    if (!config::lake_enable_segment_warmup) {
        return;
    }

    // Check backpressure - skip if over threshold
    if (should_apply_backpressure()) {
        _backpressure_skip_count.fetch_add(1, std::memory_order_relaxed);
        VLOG(2) << "Skip async segment warmup due to backpressure. tablet_id=" << tablet_id
                << " segment_path=" << segment_path;
        return;
    }

    _total_warmup_requests.fetch_add(1, std::memory_order_relaxed);

    // Submit to thread pool for async execution
    // Use ExecEnv's lake_memtable_flush_executor or other suitable thread pool
    auto* env = _env;
    auto* tablet_mgr = _tablet_mgr;
    
    // Capture by value to ensure lifetime
    auto task = [env, tablet_mgr, tablet_id, segment_path = std::move(segment_path), warehouse_id, this]() {
        // Get peer CN nodes (cached, auto-refreshed on config change)
        std::vector<std::pair<std::string, int>> peer_nodes = get_peer_nodes();
        if (peer_nodes.empty()) {
            VLOG(3) << "No peer nodes configured for async warmup. tablet_id=" << tablet_id
                    << " warehouse_id=" << warehouse_id;
            return;
        }

        Status st = warmup_segment_blocks(tablet_id, segment_path, peer_nodes);
        if (!st.ok()) {
            LOG(WARNING) << "Async warmup failed. tablet_id=" << tablet_id 
                         << " segment_path=" << segment_path << " error=" << st;
        }
    };

    // Use a thread pool to execute the warmup task
    // Try to get a suitable thread pool from ExecEnv
    ThreadPool* pool = nullptr;
    if (_env && _env->agent_server()) {
        // Use agent server's thread pool for background tasks
        pool = _env->agent_server()->get_thread_pool(TTaskType::MAKE_SNAPSHOT);
    }
    
    if (pool) {
        Status st = pool->submit_func(std::move(task));
        if (!st.ok()) {
            LOG(WARNING) << "Failed to submit async warmup task. tablet_id=" << tablet_id
                         << " segment_path=" << segment_path << " error=" << st;
        }
    } else {
        // Fallback: execute in current thread (not ideal but better than nothing)
        VLOG(1) << "No thread pool available for async warmup, executing synchronously";
        task();
    }
}

Status SegmentWarmupManager::warmup_segment_blocks(int64_t tablet_id, const std::string& segment_path,
                                                    const std::vector<std::pair<std::string, int>>& peer_nodes) {
    // Get file system and file size first
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(segment_path));
    ASSIGN_OR_RETURN(auto file_size, fs->get_file_size(segment_path));

    // Get block size from BlockCache
    auto block_cache = BlockCache::instance();
    if (!block_cache->available()) {
        return Status::NotSupported("BlockCache is not available");
    }
    size_t block_size = block_cache->block_size();
    if (block_size == 0) {
        return Status::InternalError("Invalid block size");
    }

    // Open file once for better performance
    ASSIGN_OR_RETURN(auto file, fs->new_random_access_file(segment_path));

    // Stream-based sending: read a batch → send → read next batch
    size_t offset = 0;
    int batch_num = 0;
    int64_t total_blocks = 0;
    int64_t total_bytes_sent = 0;
    Status final_status = Status::OK();

    std::vector<char> buffer(block_size);

    while (offset < file_size) {
        // Build one batch (up to max_blocks_per_request blocks)
        WarmUpSegmentRequest request;
        request.set_tablet_id(tablet_id);
        request.set_segment_path(segment_path);

        butil::IOBuf attachment;
        int64_t batch_bytes = 0;

        // Read blocks for this batch
        while (offset < file_size && request.blocks_size() < config::lake_segment_warmup_max_blocks_per_request) {
            size_t read_size = std::min(block_size, file_size - offset);

            // Read block from file
            Status st = file->read_at_fully(offset, buffer.data(), read_size);
            if (!st.ok()) {
                LOG(WARNING) << "Failed to read block at offset " << offset << " for segment " << segment_path
                             << " error=" << st;
                final_status.update(st);
                // Skip this block and continue with next
                offset += read_size;
                continue;
            }

            // Add block metadata to request (no data!)
            auto* block_data = request.add_blocks();
            block_data->set_cache_key(segment_path);
            block_data->set_offset(offset);
            block_data->set_size(read_size);

            // Append block data to attachment (zero-copy)
            attachment.append(buffer.data(), read_size);

            batch_bytes += read_size;
            offset += read_size;
        }

        if (request.blocks_size() == 0) {
            // No blocks in this batch (all failed to read), skip
            continue;
        }

        // Update pending memory for this batch
        _pending_memory_bytes.fetch_add(batch_bytes, std::memory_order_relaxed);
        DeferOp defer([this, batch_bytes]() { _pending_memory_bytes.fetch_sub(batch_bytes, std::memory_order_relaxed); });

        // Send this batch to all peer nodes
        VLOG(2) << "Sending warmup batch. tablet_id=" << tablet_id << " segment_path=" << segment_path
                << " batch_num=" << batch_num << " block_count=" << request.blocks_size() 
                << " batch_bytes=" << batch_bytes << " progress=" << offset << "/" << file_size;

        for (const auto& [host, port] : peer_nodes) {
            Status st = send_warmup_rpc_to_peer(host, port, request, attachment);
            if (!st.ok()) {
                LOG(WARNING) << "Failed to send warmup RPC batch " << batch_num << " to peer. host=" << host 
                             << " port=" << port << " error=" << st;
                final_status.update(st);
            }
        }

        total_blocks += request.blocks_size();
        total_bytes_sent += batch_bytes;
        batch_num++;
    }

    LOG(INFO) << "Completed warming up segment. tablet_id=" << tablet_id << " segment_path=" << segment_path
              << " total_blocks=" << total_blocks << " total_bytes=" << total_bytes_sent 
              << " batches=" << batch_num << " peer_count=" << peer_nodes.size();

    return final_status;
}


Status SegmentWarmupManager::send_warmup_rpc_to_peer(const std::string& host, int port,
                                                       const WarmUpSegmentRequest& request,
                                                       const butil::IOBuf& attachment) {
    // Get stub
    auto stub_cache = _env->brpc_stub_cache();
    if (stub_cache == nullptr) {
        return Status::InternalError("BrpcStubCache is null");
    }

    auto stub = stub_cache->get_stub(host, port);
    if (stub == nullptr) {
        return Status::InternalError(strings::Substitute("Failed to get stub for $0:$1", host, port));
    }

    // Send RPC with attachment for zero-copy data transmission
    brpc::Controller cntl;
    WarmUpSegmentResponse response;
    cntl.set_timeout_ms(config::lake_segment_warmup_rpc_timeout_ms);
    
    // Append attachment (block data) - this is zero-copy
    cntl.request_attachment().append(attachment);

    VLOG(2) << "Sending warmup RPC. host=" << host << " port=" << port
            << " blocks=" << request.blocks_size() 
            << " attachment_size=" << cntl.request_attachment().size();

    stub->warm_up_segment(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        return Status::InternalError(
                strings::Substitute("Failed to send warmup RPC to $0:$1: $2", host, port, cntl.ErrorText()));
    }

    if (response.status().status_code() != 0) {
        return Status::InternalError(strings::Substitute("Warmup RPC failed on peer $0:$1: $2", host, port,
                                                          response.status().DebugString()));
    }

    VLOG(2) << "Successfully sent warmup RPC to peer. host=" << host << " port=" << port
            << " cached_blocks=" << response.cached_block_count();

    return Status::OK();
}

} // namespace starrocks::lake

