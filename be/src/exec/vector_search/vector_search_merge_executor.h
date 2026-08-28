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

#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "common/status.h"
#include "exec/vector_search/vector_search_executor.h"

namespace starrocks::vector_search {

struct VectorSearchTaskId {
    int64_t hi = 0;
    int64_t lo = 0;

    bool operator==(const VectorSearchTaskId& rhs) const { return hi == rhs.hi && lo == rhs.lo; }
};

struct VectorSearchTaskIdHash {
    size_t operator()(const VectorSearchTaskId& id) const {
        size_t seed = std::hash<int64_t>{}(id.hi);
        return seed ^ (std::hash<int64_t>{}(id.lo) + 0x9e3779b9 + (seed << 6) + (seed >> 2));
    }
};

struct VectorSearchTask {
    VectorSearchTaskId id;
    VectorSearchExecutionOptions options;
    workgroup::WorkGroupPtr workgroup;
    std::vector<VectorSearchWork> work_items;
    VectorSearchCompletion completion;
};

// The policy owns the compatibility rules and, in a future implementation, may fold `incoming`
// into a queued task. V1 deliberately installs NoopVectorSearchMergePolicy so requests never wait
// for batching and retain one-request/one-result semantics.
class VectorSearchMergePolicy {
public:
    virtual ~VectorSearchMergePolicy() = default;

    virtual bool try_merge(const std::shared_ptr<VectorSearchTask>& queued,
                           const std::shared_ptr<VectorSearchTask>& incoming) = 0;
};

class NoopVectorSearchMergePolicy final : public VectorSearchMergePolicy {
public:
    bool try_merge(const std::shared_ptr<VectorSearchTask>& queued,
                   const std::shared_ptr<VectorSearchTask>& incoming) override {
        return false;
    }
};

struct VectorSearchMergeExecutorOptions {
    size_t max_pending_tasks = 1024;
    size_t max_inflight_tasks = 64;
};

// A single scheduling thread owns admission and pending-task ordering. It never performs ANN or
// storage work; admitted tasks are dispatched to VectorSearchExecutor, whose lanes run on the
// WorkGroup ScanExecutor. Keeping tasks pending here provides the future merge policy a real queue
// to operate on without adding an artificial batching delay.
class VectorSearchMergeExecutor {
public:
    VectorSearchMergeExecutor(
            VectorSearchExecutor* executor, VectorSearchMergeExecutorOptions options,
            std::unique_ptr<VectorSearchMergePolicy> merge_policy = std::make_unique<NoopVectorSearchMergePolicy>());
    ~VectorSearchMergeExecutor();

    VectorSearchMergeExecutor(const VectorSearchMergeExecutor&) = delete;
    VectorSearchMergeExecutor& operator=(const VectorSearchMergeExecutor&) = delete;

    Status submit(std::shared_ptr<VectorSearchTask> task);
    bool cancel(const VectorSearchTaskId& id);
    void shutdown();

    size_t pending_tasks() const;
    size_t inflight_tasks() const;

private:
    struct ActiveTask {
        std::shared_ptr<VectorSearchTask> task;
        std::shared_ptr<VectorSearchExecutionHandle> handle;
        bool cancel_requested = false;
    };

    void _schedule_loop();
    void _dispatch(std::shared_ptr<VectorSearchTask> task);
    void _finish(const std::shared_ptr<VectorSearchTask>& task, Status status,
                 std::vector<VectorSearchCandidate> candidates);
    static void _complete(const std::shared_ptr<VectorSearchTask>& task, Status status,
                          std::vector<VectorSearchCandidate> candidates);

    VectorSearchExecutor* _executor;
    const VectorSearchMergeExecutorOptions _options;
    std::unique_ptr<VectorSearchMergePolicy> _merge_policy;

    mutable std::mutex _mutex;
    std::condition_variable _cv;
    std::deque<std::shared_ptr<VectorSearchTask>> _pending;
    std::unordered_map<VectorSearchTaskId, ActiveTask, VectorSearchTaskIdHash> _tasks;
    size_t _inflight = 0;
    bool _stopping = false;
    std::thread _scheduler;
};

} // namespace starrocks::vector_search
