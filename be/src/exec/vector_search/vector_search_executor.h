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
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <queue>
#include <string>
#include <vector>

#include "common/status.h"
#include "common/statusor.h"
#include "compute_env/workgroup/work_group_fwd.h"

namespace starrocks::workgroup {
class ScanExecutor;
}

namespace starrocks::vector_search {

enum class VectorSearchResultOrder : uint8_t {
    ASCENDING = 0,
    DESCENDING = 1,
};

// The executor only needs an ordering key and an opaque payload. The first implementation will
// place an encoded MySQL row in `encoded_row`; a candidate-first implementation may instead carry
// an opaque locator until late materialization.
struct VectorSearchCandidate {
    float score = 0;
    uint64_t tie_breaker = 0;
    std::string encoded_row;
};

class VectorSearchTopK {
public:
    VectorSearchTopK(size_t limit, VectorSearchResultOrder order);

    void add(VectorSearchCandidate candidate);
    void add(std::vector<VectorSearchCandidate> candidates);
    std::vector<VectorSearchCandidate> take_sorted();

    size_t size() const { return _heap.size(); }
    size_t limit() const { return _limit; }

private:
    struct BetterComparator {
        VectorSearchResultOrder order = VectorSearchResultOrder::ASCENDING;

        bool operator()(const VectorSearchCandidate& lhs, const VectorSearchCandidate& rhs) const;
    };

    const size_t _limit;
    const VectorSearchResultOrder _order;
    std::priority_queue<VectorSearchCandidate, std::vector<VectorSearchCandidate>, BetterComparator> _heap;
};

struct VectorSearchExecutionOptions {
    size_t top_k = 0;
    size_t max_parallelism = 1;
    VectorSearchResultOrder result_order = VectorSearchResultOrder::ASCENDING;
};

using VectorSearchWork = std::function<Status(std::vector<VectorSearchCandidate>*)>;
using VectorSearchCompletion = std::function<void(Status, std::vector<VectorSearchCandidate>)>;

class VectorSearchExecutionState;

// A lightweight cancellation handle. The asynchronous state is owned by the submitted ScanTasks;
// keeping a handle does not keep a completed execution alive.
class VectorSearchExecutionHandle {
public:
    void cancel();
    bool is_cancelled() const;
    bool is_finished() const;

private:
    explicit VectorSearchExecutionHandle(const std::shared_ptr<VectorSearchExecutionState>& state) : _state(state) {}

    std::weak_ptr<VectorSearchExecutionState> _state;

    friend class VectorSearchExecutor;
};

// Executes a vector-search request as a bounded number of cooperative ScanTasks. Each lane runs one
// work item per scheduling turn and owns a private TopK heap. This keeps queue size bounded and avoids
// a shared-heap lock on the hot candidate path.
class VectorSearchExecutor {
public:
    explicit VectorSearchExecutor(workgroup::ScanExecutor* scan_executor) : _scan_executor(scan_executor) {}

    StatusOr<std::shared_ptr<VectorSearchExecutionHandle>> submit(VectorSearchExecutionOptions options,
                                                                  workgroup::WorkGroupPtr workgroup,
                                                                  std::vector<VectorSearchWork> work_items,
                                                                  VectorSearchCompletion completion);

private:
    workgroup::ScanExecutor* _scan_executor;
};

} // namespace starrocks::vector_search
