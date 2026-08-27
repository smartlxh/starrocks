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

#include "exec/vector_search/vector_search_executor.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <mutex>
#include <utility>

#include "compute_env/workgroup/scan_executor.h"
#include "compute_env/workgroup/scan_task.h"
#include "gen_cpp/InternalService_types.h"

namespace starrocks::vector_search {

namespace {

bool candidate_is_better(const VectorSearchCandidate& lhs, const VectorSearchCandidate& rhs,
                         VectorSearchResultOrder order) {
    const bool lhs_nan = std::isnan(lhs.score);
    const bool rhs_nan = std::isnan(rhs.score);
    if (lhs_nan != rhs_nan) {
        return !lhs_nan;
    }
    if (lhs_nan) {
        return lhs.tie_breaker < rhs.tie_breaker;
    }
    if (lhs.score != rhs.score) {
        return order == VectorSearchResultOrder::ASCENDING ? lhs.score < rhs.score : lhs.score > rhs.score;
    }
    return lhs.tie_breaker < rhs.tie_breaker;
}

} // namespace

bool VectorSearchTopK::BetterComparator::operator()(const VectorSearchCandidate& lhs,
                                                    const VectorSearchCandidate& rhs) const {
    // std::priority_queue keeps the element for which Compare(element, other) is false at the top.
    // Defining Compare as "better than" therefore keeps the worst retained candidate at the top.
    return candidate_is_better(lhs, rhs, order);
}

VectorSearchTopK::VectorSearchTopK(size_t limit, VectorSearchResultOrder order)
        : _limit(limit), _order(order), _heap(BetterComparator{order}) {}

void VectorSearchTopK::add(VectorSearchCandidate candidate) {
    if (_limit == 0) {
        return;
    }
    if (_heap.size() < _limit) {
        _heap.emplace(std::move(candidate));
        return;
    }
    if (candidate_is_better(candidate, _heap.top(), _order)) {
        _heap.pop();
        _heap.emplace(std::move(candidate));
    }
}

void VectorSearchTopK::add(std::vector<VectorSearchCandidate> candidates) {
    for (auto& candidate : candidates) {
        add(std::move(candidate));
    }
}

std::vector<VectorSearchCandidate> VectorSearchTopK::take_sorted() {
    std::vector<VectorSearchCandidate> result;
    result.reserve(_heap.size());
    while (!_heap.empty()) {
        // priority_queue::top() is a const reference, so the opaque payload is copied at most K times.
        result.emplace_back(_heap.top());
        _heap.pop();
    }
    std::sort(result.begin(), result.end(),
              [order = _order](const auto& lhs, const auto& rhs) { return candidate_is_better(lhs, rhs, order); });
    return result;
}

class VectorSearchExecutionState : public std::enable_shared_from_this<VectorSearchExecutionState> {
public:
    VectorSearchExecutionState(VectorSearchExecutionOptions options, workgroup::WorkGroupPtr workgroup,
                               std::vector<VectorSearchWork> work_items, VectorSearchCompletion completion,
                               size_t num_lanes)
            : options(std::move(options)),
              workgroup(std::move(workgroup)),
              work_items(std::move(work_items)),
              completion(std::move(completion)),
              remaining_lanes(num_lanes) {
        lane_topks.reserve(num_lanes);
        for (size_t i = 0; i < num_lanes; ++i) {
            lane_topks.emplace_back(this->options.top_k, this->options.result_order);
        }
    }

    void cancel() {
        bool expected = false;
        if (cancelled.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            set_first_error(Status::Cancelled("vector search execution cancelled"));
        }
    }

    bool is_cancelled() const { return cancelled.load(std::memory_order_acquire); }
    bool is_finished() const { return completed.load(std::memory_order_acquire); }

    void set_first_error(const Status& status) {
        if (status.ok()) {
            return;
        }
        std::lock_guard lock(status_mutex);
        if (first_error.ok()) {
            first_error = status;
        }
    }

    Status status() const {
        std::lock_guard lock(status_mutex);
        return first_error;
    }

    void finish_lane() {
        if (remaining_lanes.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            finish_execution();
        }
    }

    void finish_execution() {
        bool expected = false;
        if (!completed.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            return;
        }

        VectorSearchTopK final_topk(options.top_k, options.result_order);
        for (auto& lane_topk : lane_topks) {
            final_topk.add(lane_topk.take_sorted());
        }
        auto result = final_topk.take_sorted();
        auto final_status = status();
        if (completion != nullptr) {
            completion(std::move(final_status), std::move(result));
        }
    }

    VectorSearchExecutionOptions options;
    workgroup::WorkGroupPtr workgroup;
    std::vector<VectorSearchWork> work_items;
    VectorSearchCompletion completion;
    std::vector<VectorSearchTopK> lane_topks;

    std::atomic<size_t> next_work_item{0};
    std::atomic<size_t> remaining_lanes{0};
    std::atomic<bool> cancelled{false};
    std::atomic<bool> completed{false};

    mutable std::mutex status_mutex;
    Status first_error;
};

void VectorSearchExecutionHandle::cancel() {
    if (auto state = _state.lock(); state != nullptr) {
        state->cancel();
    }
}

bool VectorSearchExecutionHandle::is_cancelled() const {
    if (auto state = _state.lock(); state != nullptr) {
        return state->is_cancelled();
    }
    return false;
}

bool VectorSearchExecutionHandle::is_finished() const {
    if (auto state = _state.lock(); state != nullptr) {
        return state->is_finished();
    }
    return true;
}

StatusOr<std::shared_ptr<VectorSearchExecutionHandle>> VectorSearchExecutor::submit(
        VectorSearchExecutionOptions options, workgroup::WorkGroupPtr workgroup,
        std::vector<VectorSearchWork> work_items, VectorSearchCompletion completion) {
    if (_scan_executor == nullptr) {
        return Status::InvalidArgument("vector search ScanExecutor is null");
    }
    if (workgroup == nullptr) {
        return Status::InvalidArgument("vector search WorkGroup is null");
    }
    if (options.top_k == 0) {
        return Status::InvalidArgument("vector search top_k must be positive");
    }
    if (options.max_parallelism == 0) {
        return Status::InvalidArgument("vector search max_parallelism must be positive");
    }
    if (completion == nullptr) {
        return Status::InvalidArgument("vector search completion callback is null");
    }

    if (work_items.empty()) {
        completion(Status::OK(), {});
        return std::shared_ptr<VectorSearchExecutionHandle>(
                new VectorSearchExecutionHandle(std::shared_ptr<VectorSearchExecutionState>{}));
    }

    const size_t num_lanes = std::min(options.max_parallelism, work_items.size());
    auto state = std::make_shared<VectorSearchExecutionState>(std::move(options), std::move(workgroup),
                                                              std::move(work_items), std::move(completion), num_lanes);
    auto handle = std::shared_ptr<VectorSearchExecutionHandle>(new VectorSearchExecutionHandle(state));

    for (size_t lane = 0; lane < num_lanes; ++lane) {
        workgroup::ScanTask task(state->workgroup, [state, lane](workgroup::YieldContext& yield_ctx) {
            if (yield_ctx.total_yield_point_cnt == 0) {
                // Keep the task unfinished until the lane has exhausted the shared work cursor.
                yield_ctx.total_yield_point_cnt = 1;
            }

            if (state->is_cancelled()) {
                yield_ctx.set_finished();
                state->finish_lane();
                return;
            }

            const size_t work_index = state->next_work_item.fetch_add(1, std::memory_order_relaxed);
            if (work_index >= state->work_items.size()) {
                yield_ctx.set_finished();
                state->finish_lane();
                return;
            }

            std::vector<VectorSearchCandidate> candidates;
            Status status = state->work_items[work_index](&candidates);
            if (!status.ok()) {
                state->set_first_error(status);
                state->cancelled.store(true, std::memory_order_release);
                yield_ctx.set_finished();
                state->finish_lane();
                return;
            }

            if (!state->is_cancelled()) {
                state->lane_topks[lane].add(std::move(candidates));
            }
            // Returning with an unfinished YieldContext makes ScanExecutor requeue this lane. This
            // provides a cooperative scheduling boundary after every tablet/work item.
        });
        task.set_query_type(TQueryType::SELECT);

        if (!_scan_executor->submit(std::move(task))) {
            state->set_first_error(Status::ServiceUnavailable("vector search ScanExecutor queue is full"));
            state->cancelled.store(true, std::memory_order_release);
            state->finish_lane();
        }
    }
    return handle;
}

} // namespace starrocks::vector_search
