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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <thread>
#include <utility>

#include "base/testutil/assert.h"
#include "common/system/cpu_info.h"
#include "common/thread/threadpool.h"
#include "compute_env/workgroup/priority_scan_task_queue.h"
#include "compute_env/workgroup/scan_executor.h"
#include "compute_env/workgroup/work_group.h"
#include "exec/vector_search/vector_search_merge_executor.h"
#if __has_include("exec_primitive/pipeline/primitives/pipeline_metrics.h")
#include "exec_primitive/pipeline/primitives/pipeline_metrics.h"
#else
#include "exec/pipeline/primitives/pipeline_metrics.h"
#endif

namespace starrocks::vector_search {

namespace {

class VectorSearchExecutorTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() { CpuInfo::init(); }

    void SetUp() override {
        auto queue = std::make_unique<workgroup::PriorityScanTaskQueue>(1024);
        std::unique_ptr<ThreadPool> thread_pool;
        ASSERT_OK(ThreadPoolBuilder("vector_search_test")
                          .set_min_threads(0)
                          .set_max_threads(kNumThreads)
                          .set_max_queue_size(1024)
                          .build(&thread_pool));
        _executor = std::make_unique<workgroup::ScanExecutor>(std::move(thread_pool), std::move(queue), &_metrics);
        _executor->initialize(kNumThreads);
        _workgroup = std::make_shared<workgroup::WorkGroup>(
                "vector_search_test_wg", 101, workgroup::WorkGroup::DEFAULT_VERSION, 1, 1.0, 1, 1.0,
                workgroup::WorkGroupType::WG_NORMAL, workgroup::WorkGroup::DEFAULT_MEM_POOL);
    }

    void TearDown() override {
        if (_executor != nullptr) {
            _executor->close();
        }
    }

    static constexpr int kNumThreads = 4;

    pipeline::ScanExecutorMetrics _metrics;
    std::unique_ptr<workgroup::ScanExecutor> _executor;
    workgroup::WorkGroupPtr _workgroup;
};

struct AsyncResult {
    Status status;
    std::vector<VectorSearchCandidate> candidates;
};

class CountingNoopMergePolicy final : public VectorSearchMergePolicy {
public:
    bool try_merge(const std::shared_ptr<VectorSearchTask>& queued,
                   const std::shared_ptr<VectorSearchTask>& incoming) override {
        attempts.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    std::atomic<int> attempts{0};
};

bool wait_until(const std::function<bool()>& predicate) {
    for (int i = 0; i < 1000; ++i) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return false;
}

} // namespace

TEST_F(VectorSearchExecutorTest, TopKAscendingAndTieBreaker) {
    VectorSearchExecutor executor(_executor.get());
    std::vector<VectorSearchWork> work_items;
    work_items.emplace_back([](auto* out) {
        out->push_back({4.0F, 40, "four"});
        out->push_back({1.0F, 20, "one-20"});
        return Status::OK();
    });
    work_items.emplace_back([](auto* out) {
        out->push_back({2.0F, 30, "two"});
        out->push_back({1.0F, 10, "one-10"});
        return Status::OK();
    });

    std::promise<AsyncResult> promise;
    auto future = promise.get_future();
    ASSERT_OK(executor.submit({.top_k = 3, .max_parallelism = 2}, _workgroup, std::move(work_items),
                              [&promise](Status status, auto candidates) {
                                  promise.set_value({std::move(status), std::move(candidates)});
                              }));

    auto result = future.get();
    ASSERT_OK(result.status);
    ASSERT_EQ(3, result.candidates.size());
    EXPECT_EQ(1.0F, result.candidates[0].score);
    EXPECT_EQ(10, result.candidates[0].tie_breaker);
    EXPECT_EQ(1.0F, result.candidates[1].score);
    EXPECT_EQ(20, result.candidates[1].tie_breaker);
    EXPECT_EQ(2.0F, result.candidates[2].score);
}

TEST_F(VectorSearchExecutorTest, TopKDescending) {
    VectorSearchExecutor executor(_executor.get());
    std::vector<VectorSearchWork> work_items;
    for (int i = 0; i < 8; ++i) {
        work_items.emplace_back([i](auto* out) {
            out->push_back({static_cast<float>(i), static_cast<uint64_t>(i), std::to_string(i)});
            return Status::OK();
        });
    }

    std::promise<AsyncResult> promise;
    auto future = promise.get_future();
    ASSERT_OK(executor.submit({.top_k = 3, .max_parallelism = 3, .result_order = VectorSearchResultOrder::DESCENDING},
                              _workgroup, std::move(work_items), [&promise](Status status, auto candidates) {
                                  promise.set_value({std::move(status), std::move(candidates)});
                              }));

    auto result = future.get();
    ASSERT_OK(result.status);
    ASSERT_EQ(3, result.candidates.size());
    EXPECT_EQ(7.0F, result.candidates[0].score);
    EXPECT_EQ(6.0F, result.candidates[1].score);
    EXPECT_EQ(5.0F, result.candidates[2].score);
}

TEST_F(VectorSearchExecutorTest, BoundedParallelism) {
    VectorSearchExecutor executor(_executor.get());
    constexpr int kWorkItems = 24;
    constexpr int kParallelism = 3;
    std::atomic<int> active = 0;
    std::atomic<int> max_active = 0;
    std::atomic<int> processed = 0;
    std::vector<VectorSearchWork> work_items;
    for (int i = 0; i < kWorkItems; ++i) {
        work_items.emplace_back([i, &active, &max_active, &processed](auto* out) {
            int current = active.fetch_add(1, std::memory_order_acq_rel) + 1;
            int observed = max_active.load(std::memory_order_relaxed);
            while (current > observed &&
                   !max_active.compare_exchange_weak(observed, current, std::memory_order_relaxed)) {
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            out->push_back({static_cast<float>(i), static_cast<uint64_t>(i), {}});
            processed.fetch_add(1, std::memory_order_relaxed);
            active.fetch_sub(1, std::memory_order_acq_rel);
            return Status::OK();
        });
    }

    std::promise<AsyncResult> promise;
    auto future = promise.get_future();
    ASSERT_OK(executor.submit({.top_k = 5, .max_parallelism = kParallelism}, _workgroup, std::move(work_items),
                              [&promise](Status status, auto candidates) {
                                  promise.set_value({std::move(status), std::move(candidates)});
                              }));

    auto result = future.get();
    ASSERT_OK(result.status);
    EXPECT_LE(max_active.load(), kParallelism);
    EXPECT_EQ(kWorkItems, processed.load());
}

TEST_F(VectorSearchExecutorTest, ErrorCancelsRemainingWorkAndCompletesOnce) {
    VectorSearchExecutor executor(_executor.get());
    std::atomic<int> callbacks = 0;
    std::atomic<int> processed = 0;
    std::vector<VectorSearchWork> work_items;
    for (int i = 0; i < 64; ++i) {
        work_items.emplace_back([i, &processed](auto* out) {
            processed.fetch_add(1, std::memory_order_relaxed);
            if (i == 3) {
                return Status::InvalidArgument("injected vector search failure");
            }
            out->push_back({static_cast<float>(i), static_cast<uint64_t>(i), {}});
            return Status::OK();
        });
    }

    std::promise<AsyncResult> promise;
    auto future = promise.get_future();
    ASSERT_OK(executor.submit({.top_k = 5, .max_parallelism = 4}, _workgroup, std::move(work_items),
                              [&promise, &callbacks](Status status, auto candidates) {
                                  callbacks.fetch_add(1, std::memory_order_relaxed);
                                  promise.set_value({std::move(status), std::move(candidates)});
                              }));

    auto result = future.get();
    EXPECT_TRUE(result.status.is_invalid_argument());
    EXPECT_EQ(1, callbacks.load());
    EXPECT_LT(processed.load(), 64);
}

TEST_F(VectorSearchExecutorTest, ExplicitCancellation) {
    VectorSearchExecutor executor(_executor.get());
    std::atomic<int> processed = 0;
    std::promise<void> started;
    std::atomic<bool> started_set = false;
    std::vector<VectorSearchWork> work_items;
    for (int i = 0; i < 128; ++i) {
        work_items.emplace_back([i, &processed, &started, &started_set](auto* out) {
            if (!started_set.exchange(true)) {
                started.set_value();
            }
            processed.fetch_add(1, std::memory_order_relaxed);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            out->push_back({static_cast<float>(i), static_cast<uint64_t>(i), {}});
            return Status::OK();
        });
    }

    std::promise<AsyncResult> promise;
    auto future = promise.get_future();
    auto handle_or = executor.submit({.top_k = 5, .max_parallelism = 4}, _workgroup, std::move(work_items),
                                     [&promise](Status status, auto candidates) {
                                         promise.set_value({std::move(status), std::move(candidates)});
                                     });
    ASSERT_OK(handle_or);
    started.get_future().get();
    handle_or.value()->cancel();

    auto result = future.get();
    EXPECT_TRUE(result.status.is_cancelled());
    EXPECT_LT(processed.load(), 128);
    EXPECT_TRUE(handle_or.value()->is_finished());
}

TEST_F(VectorSearchExecutorTest, RejectsInvalidOptions) {
    VectorSearchExecutor executor(_executor.get());
    std::vector<VectorSearchWork> work_items;
    work_items.emplace_back([](auto*) { return Status::OK(); });

    auto result =
            executor.submit({.top_k = 0, .max_parallelism = 1}, _workgroup, std::move(work_items), [](Status, auto) {});
    EXPECT_TRUE(result.status().is_invalid_argument());
}

TEST_F(VectorSearchExecutorTest, MergeExecutorKeepsRequestsSeparateAndBoundsInflight) {
    VectorSearchExecutor executor(_executor.get());
    auto policy = std::make_unique<CountingNoopMergePolicy>();
    auto* policy_ptr = policy.get();
    VectorSearchMergeExecutor merge_executor(&executor, {.max_pending_tasks = 8, .max_inflight_tasks = 1},
                                             std::move(policy));

    std::promise<void> first_started;
    std::promise<void> release_first;
    auto release_future = release_first.get_future().share();
    std::promise<AsyncResult> first_result;
    std::promise<AsyncResult> second_result;
    std::promise<AsyncResult> third_result;

    auto first = std::make_shared<VectorSearchTask>();
    first->id = {1, 1};
    first->options = {.top_k = 1, .max_parallelism = 1};
    first->workgroup = _workgroup;
    first->work_items.emplace_back([&](auto* out) {
        first_started.set_value();
        release_future.wait();
        out->push_back({1.0F, 1, "first"});
        return Status::OK();
    });
    first->completion = [&first_result](Status status, auto candidates) {
        first_result.set_value({std::move(status), std::move(candidates)});
    };
    ASSERT_OK(merge_executor.submit(first));
    first_started.get_future().get();

    auto second = std::make_shared<VectorSearchTask>();
    second->id = {2, 2};
    second->options = {.top_k = 1, .max_parallelism = 1};
    second->workgroup = _workgroup;
    second->work_items.emplace_back([](auto* out) {
        out->push_back({2.0F, 2, "second"});
        return Status::OK();
    });
    second->completion = [&second_result](Status status, auto candidates) {
        second_result.set_value({std::move(status), std::move(candidates)});
    };
    ASSERT_OK(merge_executor.submit(second));

    auto third = std::make_shared<VectorSearchTask>();
    third->id = {5, 5};
    third->options = {.top_k = 1, .max_parallelism = 1};
    third->workgroup = _workgroup;
    third->work_items.emplace_back([](auto* out) {
        out->push_back({3.0F, 3, "third"});
        return Status::OK();
    });
    third->completion = [&third_result](Status status, auto candidates) {
        third_result.set_value({std::move(status), std::move(candidates)});
    };
    ASSERT_OK(merge_executor.submit(third));

    ASSERT_TRUE(wait_until([&] { return merge_executor.pending_tasks() == 2; }));
    EXPECT_EQ(1, merge_executor.inflight_tasks());
    EXPECT_GE(policy_ptr->attempts.load(), 1);

    release_first.set_value();
    auto result1 = first_result.get_future().get();
    auto result2 = second_result.get_future().get();
    auto result3 = third_result.get_future().get();
    ASSERT_OK(result1.status);
    ASSERT_OK(result2.status);
    ASSERT_OK(result3.status);
    ASSERT_EQ(1, result1.candidates.size());
    ASSERT_EQ(1, result2.candidates.size());
    ASSERT_EQ(1, result3.candidates.size());
    EXPECT_EQ("first", result1.candidates[0].encoded_row);
    EXPECT_EQ("second", result2.candidates[0].encoded_row);
    EXPECT_EQ("third", result3.candidates[0].encoded_row);
}

TEST_F(VectorSearchExecutorTest, MergeExecutorCancelsPendingRequest) {
    VectorSearchExecutor executor(_executor.get());
    VectorSearchMergeExecutor merge_executor(&executor, {.max_pending_tasks = 8, .max_inflight_tasks = 1});

    std::promise<void> first_started;
    std::promise<void> release_first;
    auto release_future = release_first.get_future().share();
    std::promise<AsyncResult> first_result;
    std::promise<AsyncResult> pending_result;

    auto first = std::make_shared<VectorSearchTask>();
    first->id = {3, 3};
    first->options = {.top_k = 1, .max_parallelism = 1};
    first->workgroup = _workgroup;
    first->work_items.emplace_back([&](auto*) {
        first_started.set_value();
        release_future.wait();
        return Status::OK();
    });
    first->completion = [&first_result](Status status, auto candidates) {
        first_result.set_value({std::move(status), std::move(candidates)});
    };
    ASSERT_OK(merge_executor.submit(first));
    first_started.get_future().get();

    auto pending = std::make_shared<VectorSearchTask>();
    pending->id = {4, 4};
    pending->options = {.top_k = 1, .max_parallelism = 1};
    pending->workgroup = _workgroup;
    pending->work_items.emplace_back([](auto*) { return Status::OK(); });
    pending->completion = [&pending_result](Status status, auto candidates) {
        pending_result.set_value({std::move(status), std::move(candidates)});
    };
    ASSERT_OK(merge_executor.submit(pending));
    ASSERT_TRUE(wait_until([&] { return merge_executor.pending_tasks() == 1; }));

    EXPECT_TRUE(merge_executor.cancel(pending->id));
    auto cancelled = pending_result.get_future().get();
    EXPECT_TRUE(cancelled.status.is_cancelled());
    EXPECT_EQ(0, merge_executor.pending_tasks());

    release_first.set_value();
    ASSERT_OK(first_result.get_future().get().status);
}

} // namespace starrocks::vector_search
