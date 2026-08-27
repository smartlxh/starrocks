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

#include <benchmark/benchmark.h>

#include <atomic>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <vector>

#include "common/logging.h"
#include "common/system/cpu_info.h"
#include "common/thread/threadpool.h"
#include "compute_env/workgroup/priority_scan_task_queue.h"
#include "compute_env/workgroup/scan_executor.h"
#include "compute_env/workgroup/work_group.h"
#if __has_include("exec_primitive/pipeline/primitives/pipeline_metrics.h")
#include "exec_primitive/pipeline/primitives/pipeline_metrics.h"
#else
#include "exec/pipeline/primitives/pipeline_metrics.h"
#endif
#include "exec/vector_search/vector_search_executor.h"

namespace starrocks::vector_search {

namespace {

constexpr size_t kCandidateCount = 32;
constexpr size_t kTopK = 10;

class BenchmarkExecutor {
public:
    explicit BenchmarkExecutor(int num_threads) {
        CpuInfo::init();
        auto queue = std::make_unique<workgroup::PriorityScanTaskQueue>(1 << 20);
        std::unique_ptr<ThreadPool> thread_pool;
        auto status = ThreadPoolBuilder("vector_search_bench")
                              .set_min_threads(0)
                              .set_max_threads(num_threads)
                              .set_max_queue_size(1 << 20)
                              .build(&thread_pool);
        CHECK(status.ok()) << status;
        executor = std::make_unique<workgroup::ScanExecutor>(std::move(thread_pool), std::move(queue), &metrics);
        executor->initialize(num_threads);
        workgroup = std::make_shared<workgroup::WorkGroup>(
                "vector_search_bench_wg", 102, workgroup::WorkGroup::DEFAULT_VERSION, 1, 1.0, 1, 1.0,
                workgroup::WorkGroupType::WG_NORMAL, workgroup::WorkGroup::DEFAULT_MEM_POOL);
    }

    ~BenchmarkExecutor() { executor->close(); }

    pipeline::ScanExecutorMetrics metrics;
    std::unique_ptr<workgroup::ScanExecutor> executor;
    workgroup::WorkGroupPtr workgroup;
};

BenchmarkExecutor& benchmark_executor(size_t num_threads) {
    static BenchmarkExecutor two_threads(2);
    static BenchmarkExecutor four_threads(4);
    static BenchmarkExecutor eight_threads(8);
    switch (num_threads) {
    case 2:
        return two_threads;
    case 4:
        return four_threads;
    case 8:
        return eight_threads;
    default:
        LOG(FATAL) << "unsupported benchmark thread count: " << num_threads;
        __builtin_unreachable();
    }
}

std::vector<VectorSearchCandidate> generate_candidates(size_t work_index, int spin) {
    uint64_t value = work_index * 0x9e3779b97f4a7c15ULL + 1;
    for (int i = 0; i < spin; ++i) {
        value = value * 2862933555777941757ULL + 3037000493ULL;
        benchmark::DoNotOptimize(value);
    }

    std::vector<VectorSearchCandidate> result;
    result.reserve(kCandidateCount);
    for (size_t i = 0; i < kCandidateCount; ++i) {
        value = value * 2862933555777941757ULL + 3037000493ULL;
        result.push_back({static_cast<float>(value % 100000), work_index * kCandidateCount + i, {}});
    }
    return result;
}

void BM_VectorSearchBoundedLanes(benchmark::State& state) {
    const size_t num_work_items = state.range(0);
    const size_t max_parallelism = state.range(1);
    const int spin = state.range(2);
    auto& harness = benchmark_executor(max_parallelism);
    VectorSearchExecutor executor(harness.executor.get());

    for (auto _ : state) {
        std::vector<VectorSearchWork> work_items;
        work_items.reserve(num_work_items);
        for (size_t i = 0; i < num_work_items; ++i) {
            work_items.emplace_back([i, spin](auto* out) {
                *out = generate_candidates(i, spin);
                return Status::OK();
            });
        }

        std::promise<std::vector<VectorSearchCandidate>> promise;
        auto future = promise.get_future();
        auto handle = executor.submit(
                {.top_k = kTopK, .max_parallelism = max_parallelism}, harness.workgroup, std::move(work_items),
                [&promise](Status status, auto candidates) {
                    if (!status.ok()) {
                        promise.set_exception(std::make_exception_ptr(std::runtime_error(status.to_string())));
                        return;
                    }
                    promise.set_value(std::move(candidates));
                });
        if (!handle.ok()) {
            state.SkipWithError(handle.status().to_string().c_str());
            break;
        }
        auto result = future.get();
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations() * num_work_items);
}

// Models the scheduling shape of the current pipeline scan path: one queued task per tablet/work
// item and all producers merge directly into a shared TopK. It intentionally excludes FE planning,
// FragmentExecutor and PipelineDriver construction, so it is a conservative control-path comparison,
// not an end-to-end SQL benchmark.
void BM_PipelineStyleFanoutSharedTopK(benchmark::State& state) {
    const size_t num_work_items = state.range(0);
    const size_t max_parallelism = state.range(1);
    const int spin = state.range(2);
    auto& harness = benchmark_executor(max_parallelism);

    for (auto _ : state) {
        VectorSearchTopK shared_topk(kTopK, VectorSearchResultOrder::ASCENDING);
        std::mutex heap_mutex;
        std::atomic<size_t> remaining = num_work_items;
        std::promise<std::vector<VectorSearchCandidate>> promise;
        auto future = promise.get_future();
        for (size_t i = 0; i < num_work_items; ++i) {
            workgroup::ScanTask task(harness.workgroup, [&, i, spin](workgroup::YieldContext& ctx) {
                auto candidates = generate_candidates(i, spin);
                {
                    std::lock_guard lock(heap_mutex);
                    shared_topk.add(std::move(candidates));
                }
                if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    promise.set_value(shared_topk.take_sorted());
                }
                ctx.set_finished();
            });
            task.set_query_type(TQueryType::SELECT);
            harness.executor->force_submit(std::move(task));
        }
        auto result = future.get();
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations() * num_work_items);
}

} // namespace

BENCHMARK(BM_VectorSearchBoundedLanes)
        ->Args({8, 2, 0})
        ->Args({64, 4, 0})
        ->Args({256, 8, 0})
        ->Args({64, 4, 2000})
        ->Args({64, 4, 200000})
        ->UseRealTime();

BENCHMARK(BM_PipelineStyleFanoutSharedTopK)
        ->Args({8, 2, 0})
        ->Args({64, 4, 0})
        ->Args({256, 8, 0})
        ->Args({64, 4, 2000})
        ->Args({64, 4, 200000})
        ->UseRealTime();

} // namespace starrocks::vector_search

BENCHMARK_MAIN();
