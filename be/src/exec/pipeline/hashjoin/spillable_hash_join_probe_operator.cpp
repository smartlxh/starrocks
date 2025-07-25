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

#include "exec/pipeline/hashjoin/spillable_hash_join_probe_operator.h"

#include <algorithm>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>

#include "common/config.h"
#include "exec/hash_joiner.h"
#include "exec/pipeline/hashjoin/hash_join_probe_operator.h"
#include "exec/pipeline/hashjoin/hash_joiner_factory.h"
#include "exec/pipeline/query_context.h"
#include "exec/spill/executor.h"
#include "exec/spill/partition.h"
#include "exec/spill/spill_components.h"
#include "exec/spill/spiller.h"
#include "exec/spill/spiller.hpp"
#include "gen_cpp/PlanNodes_types.h"
#include "gutil/casts.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"
#include "util/failpoint/fail_point.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {

DEFINE_FAIL_POINT(spill_hash_join_throw_bad_alloc)

Status SpillableHashJoinProbeOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(HashJoinProbeOperator::prepare(state));
    _need_post_probe = has_post_probe(_join_prober->join_type());
    _probe_spiller->set_metrics(spill::SpillProcessMetrics(_unique_metrics.get(), state->mutable_total_spill_bytes()));
    metrics.hash_partitions = ADD_COUNTER(_unique_metrics.get(), "SpillPartitions", TUnit::UNIT);
    metrics.build_partition_peak_memory_usage = _unique_metrics->AddHighWaterMarkCounter(
            "SpillBuildPartitionPeakMemoryUsage", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TUnit::BYTES));
    metrics.prober_peak_memory_usage = _unique_metrics->AddHighWaterMarkCounter(
            "SpillProberPeakMemoryUsage", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TUnit::BYTES));
    metrics.peak_processing_partition_count = _unique_metrics->AddHighWaterMarkCounter(
            "SpillPeakProcessingPartitionCount", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TUnit::UNIT));
    RETURN_IF_ERROR(_probe_spiller->prepare(state));
    auto wg = state->fragment_ctx()->workgroup();
    return Status::OK();
}

void SpillableHashJoinProbeOperator::close(RuntimeState* state) {
    HashJoinProbeOperator::close(state);
}

bool SpillableHashJoinProbeOperator::has_output() const {
    if (!is_ready()) {
        DCHECK(false) << "is_ready() must be true before call has_output";
        return false;
    }
    if (!spilled()) {
        return HashJoinProbeOperator::has_output();
    }

    // if any partition hash_table is loading. just return false
    if (!_latch.ready()) {
        return false;
    }

    if (!_status().ok()) {
        return true;
    }

    if (_processing_partitions.empty()) {
        as_mutable()->_acquire_next_partitions();
        _update_status(as_mutable()->_load_all_partition_build_side(runtime_state()));
        return false;
    }

    // if any hash_join_prober has data.
    for (auto prober : _probers) {
        if (!prober->probe_chunk_empty()) {
            return true;
        }
    }

    //
    if (_probe_spiller->is_full()) {
        return false;
    }

    if (_is_finishing) {
        if (_all_partition_finished()) {
            return false;
        }

        // reader is empty.
        // need to call pull_chunk to acquire next partitions
        if (_current_reader.empty()) {
            return true;
        }

        for (size_t i = 0; i < _probers.size(); ++i) {
            if (_current_reader[i]->has_output_data()) {
                return true;
            } else if (!_current_reader[i]->has_restore_task()) {
                // if trigger_restore returns error, should record this status and return it in pull_chunk
                _update_status(_current_reader[i]->trigger_restore(
                        runtime_state(),
                        RESOURCE_TLS_MEMTRACER_GUARD(runtime_state(), std::weak_ptr(_current_reader[i]))));
                if (!_status().ok()) {
                    return true;
                }
            }
        }
    }

    return false;
}

bool SpillableHashJoinProbeOperator::need_input() const {
    if (!is_ready()) {
        DCHECK(false) << "is_ready() must be true before call has_output";
        return false;
    }
    if (!spilled()) {
        return HashJoinProbeOperator::need_input();
    }

    if (!_latch.ready()) {
        return false;
    }

    if (_processing_partitions.empty()) {
        as_mutable()->_acquire_next_partitions();
        _update_status(as_mutable()->_load_all_partition_build_side(runtime_state()));
        return false;
    }

    if (_probe_spiller->is_full()) {
        return false;
    }

    for (auto prober : _probers) {
        if (!prober->probe_chunk_empty()) {
            return false;
        }
    }

    return true;
}

bool SpillableHashJoinProbeOperator::is_finished() const {
    if (!spilled()) {
        return HashJoinProbeOperator::is_finished();
    }

    if (_is_finished) {
        return true;
    }

    if (_is_finishing && _all_partition_finished()) {
        return true;
    }

    return false;
}

Status SpillableHashJoinProbeOperator::set_finishing(RuntimeState* state) {
    if (!spilled()) {
        return HashJoinProbeOperator::set_finishing(state);
    }
    if (state->is_cancelled()) {
        _probe_spiller->cancel();
    }
    _is_finishing = true;
    return Status::OK();
}

Status SpillableHashJoinProbeOperator::set_finished(RuntimeState* state) {
    _is_finished = true;
    return HashJoinProbeOperator::set_finished(state);
}

Status SpillableHashJoinProbeOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    RETURN_IF_ERROR(_status());
    if (!spilled()) {
        return HashJoinProbeOperator::push_chunk(state, chunk);
    }

    RETURN_IF_ERROR(_push_probe_chunk(state, chunk));

    return Status::OK();
}

Status SpillableHashJoinProbeOperator::_push_probe_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    // compute hash
    size_t num_rows = chunk->num_rows();
    auto hash_column = spill::SpillHashColumn::create(num_rows);
    auto& hash_values = hash_column->get_data();

    // TODO: use another hash function
    for (auto& expr_ctx : _join_prober->probe_expr_ctxs()) {
        ASSIGN_OR_RETURN(auto res, expr_ctx->evaluate(chunk.get()));
        res->fnv_hash(hash_values.data(), 0, num_rows);
    }

    auto partition_processer = [&chunk, this, state, &hash_values](spill::SpilledPartition* probe_partition,
                                                                   const std::vector<uint32_t>& selection, int32_t from,
                                                                   int32_t size) {
        // nothing to do for empty partition
        if (could_short_circuit(_join_prober->join_type())) {
            // For left semi join and inner join we can just skip the empty partition
            auto build_partition_iter = _pid_to_build_partition.find(probe_partition->partition_id);
            if (build_partition_iter != _pid_to_build_partition.end()) {
                if (build_partition_iter->second->empty()) {
                    return;
                }
            }
        }

        for (size_t i = from; i < from + size; ++i) {
            DCHECK_EQ(hash_values[selection[i]] & probe_partition->mask(),
                      probe_partition->partition_id & probe_partition->mask());
        }

        auto iter = _pid_to_process_id.find(probe_partition->partition_id);
        if (iter == _pid_to_process_id.end()) {
            auto mem_table = probe_partition->spill_writer->mem_table();
            (void)mem_table->append_selective(*chunk, selection.data(), from, size);
        } else {
            // maybe has some small chunk problem
            // TODO: add chunk accumulator here
            auto partitioned_chunk = chunk->clone_empty();
            (void)partitioned_chunk->append_selective(*chunk, selection.data(), from, size);
            (void)_probers[iter->second]->push_probe_chunk(state, std::move(partitioned_chunk));
        }
        probe_partition->num_rows += size;
    };
    RETURN_IF_ERROR(_probe_spiller->partitioned_spill(state, chunk, hash_column.get(), partition_processer,
                                                      TRACKER_WITH_SPILLER_GUARD(state, _probe_spiller)));

    return Status::OK();
}

Status SpillableHashJoinProbeOperator::_load_partition_build_side(workgroup::YieldContext& ctx, RuntimeState* state,
                                                                  const std::shared_ptr<spill::SpillerReader>& reader,
                                                                  size_t idx) {
    using SyncTaskExecutor = spill::SyncTaskExecutor;
    using MemTrackerGuard = spill::MemTrackerGuard;
    TRY_CATCH_ALLOC_SCOPE_START()
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(state->instance_mem_tracker());
    auto builder = _builders[idx];
    auto prober = _probers[idx];
    bool finish = false;
    int64_t hash_table_mem_usage = builder->ht_mem_usage();
    enum SpillLoadPartitionStage { BEGIN = 0, FINISH = 1 };
    ctx.total_yield_point_cnt = FINISH;
    auto wg = ctx.wg;
    while (!finish && !_is_finished) {
        BREAK_IF_YIELD(wg, &ctx.need_yield, ctx.time_spent_ns);
        {
            SCOPED_RAW_TIMER(&ctx.time_spent_ns);
            if (state->is_cancelled()) {
                return Status::Cancelled("cancelled");
            }

            RETURN_IF_ERROR(reader->trigger_restore<spill::SyncTaskExecutor>(state, MemTrackerGuard(tls_mem_tracker)));
            auto chunk_st = reader->restore<spill::SyncTaskExecutor>(state, MemTrackerGuard(tls_mem_tracker));

            FAIL_POINT_TRIGGER_EXECUTE(spill_hash_join_throw_bad_alloc, { throw std::bad_alloc(); });

            if (chunk_st.ok() && chunk_st.value() != nullptr && !chunk_st.value()->is_empty()) {
                int64_t old_mem_usage = hash_table_mem_usage;
                RETURN_IF_ERROR(builder->append_chunk(std::move(chunk_st.value())));
                hash_table_mem_usage = builder->ht_mem_usage();
                COUNTER_ADD(metrics.build_partition_peak_memory_usage, hash_table_mem_usage - old_mem_usage);
            } else if (chunk_st.status().is_end_of_file()) {
                RETURN_IF_ERROR(builder->build(state));
                prober->attach(builder, _join_prober->probe_metrics());
                finish = true;
            } else if (!chunk_st.ok()) {
                return chunk_st.status();
            }
        }
    }
    if (finish) {
        DCHECK_EQ(builder->hash_table_row_count(), _processing_partitions[idx]->num_rows);
    }
    TRY_CATCH_ALLOC_SCOPE_END()
    return Status::OK();
}

Status SpillableHashJoinProbeOperator::_load_all_partition_build_side(RuntimeState* state) {
    auto spill_readers = _join_builder->spiller()->get_partition_spill_readers(_processing_partitions);
    _latch.reset(_processing_partitions.size());
    int32_t driver_id = CurrentThread::current().get_driver_id();
    auto query_ctx = state->query_ctx()->weak_from_this();
    for (size_t i = 0; i < _processing_partitions.size(); ++i) {
        std::shared_ptr<spill::SpillerReader> reader = std::move(spill_readers[i]);
        auto task = [this, state, reader, i, query_ctx, driver_id](auto& yield_ctx) {
            if (auto acquired = query_ctx.lock()) {
                SCOPED_SET_TRACE_INFO(driver_id, state->query_id(), state->fragment_instance_id());
                auto defer = CancelableDefer([&]() {
                    _latch.count_down();
                    yield_ctx.set_finished();
                });
                if (!_status().ok()) {
                    return;
                }
                if (!yield_ctx.task_context_data.has_value()) {
                    yield_ctx.task_context_data = std::make_shared<spill::SpillIOTaskContext>();
                }
                yield_ctx.time_spent_ns = 0;
                yield_ctx.need_yield = false;
                _update_status(_load_partition_build_side(yield_ctx, state, reader, i));
                if (yield_ctx.need_yield) {
                    defer.cancel();
                }
            } else {
                yield_ctx.set_finished();
            }
        };
        auto yield_func = [&](workgroup::ScanTask&& task) { spill::IOTaskExecutor::force_submit(std::move(task)); };
        auto io_task =
                workgroup::ScanTask(_join_builder->spiller()->options().wg, std::move(task), std::move(yield_func));
        RETURN_IF_ERROR(spill::IOTaskExecutor::submit(std::move(io_task)));
    }
    return Status::OK();
}

void SpillableHashJoinProbeOperator::_update_status(Status&& status) const {
    if (!status.ok()) {
        std::lock_guard guard(_mutex);
        _operator_status = std::move(status);
    }
}

Status SpillableHashJoinProbeOperator::_status() const {
    std::lock_guard guard(_mutex);
    return _operator_status;
}

void SpillableHashJoinProbeOperator::_check_partitions() {
    if (_is_finishing) {
#ifndef NDEBUG
        auto partitioned_writer = down_cast<spill::PartitionedSpillerWriter*>(_probe_spiller->writer().get());
        size_t build_rows = 0;
        for (const auto& [level, partitions] : partitioned_writer->level_to_partitions()) {
            auto writer = down_cast<spill::PartitionedSpillerWriter*>(_join_builder->spiller()->writer().get());
            auto& build_partitions = writer->level_to_partitions().find(level)->second;
            DCHECK_EQ(build_partitions.size(), partitions.size());
            for (size_t i = 0; i < partitions.size(); ++i) {
                build_rows += build_partitions[i]->num_rows;
            }
            // CHECK if left table is the same as right table
            // for (size_t i = 0; i < partitions.size(); ++i) {
            //     DCHECK_EQ(partitions[i]->num_rows, build_partitions[i]->num_rows);
            // }
        }
        DCHECK_EQ(build_rows, _join_builder->spiller()->spilled_append_rows());
#endif
    }
}

Status SpillableHashJoinProbeOperator::_restore_probe_partition(RuntimeState* state) {
    for (size_t i = 0; i < _probers.size(); ++i) {
        // probe partition has been processed
        if (_probe_read_eofs[i]) continue;
        if (!_current_reader[i]->has_restore_task()) {
            RETURN_IF_ERROR(_current_reader[i]->trigger_restore(
                    state, RESOURCE_TLS_MEMTRACER_GUARD(state, std::weak_ptr(_current_reader[i]))));
        }
        if (_current_reader[i]->has_output_data()) {
            auto chunk_st = _current_reader[i]->restore(
                    state, RESOURCE_TLS_MEMTRACER_GUARD(state, std::weak_ptr(_current_reader[i])));
            if (chunk_st.ok() && chunk_st.value() && !chunk_st.value()->is_empty()) {
                RETURN_IF_ERROR(_probers[i]->push_probe_chunk(state, std::move(chunk_st.value())));
            } else if (chunk_st.status().is_end_of_file()) {
                _probe_read_eofs[i] = true;
            } else if (!chunk_st.ok()) {
                return chunk_st.status();
            }
        }
    }
    return Status::OK();
}

StatusOr<ChunkPtr> SpillableHashJoinProbeOperator::pull_chunk(RuntimeState* state) {
    RETURN_IF_ERROR(_status());
    if (!spilled()) {
        return HashJoinProbeOperator::pull_chunk(state);
    }

    _check_partitions();

    auto all_probe_partition_is_empty = [this]() {
        for (auto& _prober : _probers) {
            if (!_prober->probe_chunk_empty()) {
                return false;
            }
        }
        return true;
    };

    bool probe_has_no_output = all_probe_partition_is_empty() && !_has_probe_remain;

    if (_current_reader.empty() && _is_finishing && probe_has_no_output) {
        // init spill reader
        _current_reader = _probe_spiller->get_partition_spill_readers(_processing_partitions);
        _probe_read_eofs.assign(_current_reader.size(), false);
        _probe_post_eofs.assign(_current_reader.size(), false);
        _has_probe_remain = true;
    }

    // restore chunk from spilled partition then push it to hash join prober
    if (!_current_reader.empty() && all_probe_partition_is_empty()) {
        RETURN_IF_ERROR(_restore_probe_partition(state));
    }

    // probe chunk
    for (size_t i = 0; i < _probers.size(); ++i) {
        if (!_probers[i]->probe_chunk_empty()) {
            ASSIGN_OR_RETURN(auto res, _probers[i]->probe_chunk(state));
            return res;
        }
    }

    size_t eofs = std::accumulate(_probe_read_eofs.begin(), _probe_read_eofs.end(), 0);
    if (_need_post_probe && _has_probe_remain) {
        if (_is_finishing) {
            bool has_remain = false;
            for (size_t i = 0; i < _probers.size(); ++i) {
                if (!_probe_post_eofs[i] && _probe_read_eofs[i]) {
                    bool has_remain = false;
                    ASSIGN_OR_RETURN(auto res, _probers[i]->probe_remain(state, &has_remain));
                    _probe_post_eofs[i] = !has_remain;
                    if (res && !res->is_empty()) {
                        return res;
                    }
                }
                has_remain |= !_probe_post_eofs[i];
            }
            _has_probe_remain = has_remain;
        }
    } else {
        _has_probe_remain = false;
    }

    // processing partitions
    if (_is_finishing && eofs == _processing_partitions.size() && !_has_probe_remain) {
        DCHECK(all_probe_partition_is_empty());
        // current partition is finished
        for (auto* partition : _processing_partitions) {
            _processed_partitions.emplace(partition->partition_id);
        }
        _processing_partitions.clear();
        _current_reader.clear();
        _has_probe_remain = false;
        _builders.clear();
        COUNTER_SET(metrics.build_partition_peak_memory_usage, 0);
        COUNTER_SET(metrics.peak_processing_partition_count, 0);
    }

    return nullptr;
}

bool SpillableHashJoinProbeOperator::spilled() const {
    return _join_builder->spiller()->spilled();
}

void SpillableHashJoinProbeOperator::_acquire_next_partitions() {
    // get all spill partition
    if (_build_partitions.empty()) {
        _join_builder->spiller()->get_all_partitions(&_build_partitions);
        for (const auto* partition : _build_partitions) {
            _pid_to_build_partition[partition->partition_id] = partition;
        }

        _probe_spiller->set_partition(_build_partitions);
        COUNTER_SET(metrics.hash_partitions, (int64_t)_build_partitions.size());
    }

    size_t bytes_usage = 0;
    size_t avaliable_bytes =
            std::min<size_t>(_mem_resource_manager.operator_avaliable_memory_bytes(),
                             static_cast<size_t>(_spill_hash_join_probe_op_max_bytes / _degree_of_parallelism));
    // process the partition in memory firstly
    if (_processing_partitions.empty()) {
        for (auto partition : _build_partitions) {
            if (partition->in_mem && !_processed_partitions.count(partition->partition_id)) {
                if ((partition->mem_size + bytes_usage < avaliable_bytes || _processing_partitions.empty()) &&
                    std::find(_processing_partitions.begin(), _processing_partitions.end(), partition) ==
                            _processing_partitions.end()) {
                    _processing_partitions.emplace_back(partition);
                    bytes_usage += partition->mem_size;
                    _pid_to_process_id.emplace(partition->partition_id, _processing_partitions.size() - 1);
                    COUNTER_ADD(metrics.peak_processing_partition_count, 1);
                }
            }
        }
    }

    // process the partition could be hold in memory
    if (_processing_partitions.empty()) {
        for (const auto* partition : _build_partitions) {
            if (!partition->in_mem && !_processed_partitions.count(partition->partition_id)) {
                if ((partition->bytes + bytes_usage < avaliable_bytes || _processing_partitions.empty()) &&
                    std::find(_processing_partitions.begin(), _processing_partitions.end(), partition) ==
                            _processing_partitions.end()) {
                    _processing_partitions.emplace_back(partition);
                    bytes_usage += partition->bytes;
                    _pid_to_process_id.emplace(partition->partition_id, _processing_partitions.size() - 1);
                    COUNTER_ADD(metrics.peak_processing_partition_count, 1);
                }
            }
        }
    }
    _component_pool.clear();
    size_t process_partition_nums = _processing_partitions.size();
    _probers.resize(process_partition_nums);
    _builders.resize(process_partition_nums);
    for (size_t i = 0; i < process_partition_nums; ++i) {
        _probers[i] = _join_prober->new_prober(&_component_pool);
        _builders[i] = _join_builder->new_builder(&_component_pool);
        _builders[i]->create(_join_builder->hash_table_param());
        _probe_read_eofs.assign(process_partition_nums, true);
        _probe_post_eofs.assign(process_partition_nums, false);
    }
}

bool SpillableHashJoinProbeOperator::_all_loaded_partition_data_ready() {
    // check all loaded partition data ready
    return std::all_of(_builders.begin(), _builders.end(), [](const auto* builder) { return builder->ready(); });
}

bool SpillableHashJoinProbeOperator::_all_partition_finished() const {
    // In some cases has_output may be skipped.
    // So we call build_partitions.empty() first to make sure the parition loads
    return !_build_partitions.empty() && _processed_partitions.size() == _build_partitions.size();
}

Status SpillableHashJoinProbeOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(HashJoinProbeOperatorFactory::prepare(state));

    _spill_options = std::make_shared<spill::SpilledOptions>(config::spill_init_partition, false);
    _spill_options->spill_mem_table_bytes_size = state->spill_mem_table_size();
    _spill_options->mem_table_pool_size = state->spill_mem_table_num();
    _spill_options->spill_type = spill::SpillFormaterType::SPILL_BY_COLUMN;
    _spill_options->block_manager = state->query_ctx()->spill_manager()->block_manager();
    _spill_options->name = "hash-join-probe";
    _spill_options->plan_node_id = _plan_node_id;
    _spill_options->encode_level = state->spill_encode_level();
    _spill_options->wg = state->fragment_ctx()->workgroup();
    _spill_options->enable_buffer_read = state->enable_spill_buffer_read();
    _spill_options->max_read_buffer_bytes = state->max_spill_read_buffer_bytes_per_driver();
    _spill_options->spill_hash_join_probe_op_max_bytes = state->spill_hash_join_probe_op_max_bytes();

    return Status::OK();
}

OperatorPtr SpillableHashJoinProbeOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    auto spiller = _spill_factory->create(*_spill_options);

    auto prober = std::make_shared<SpillableHashJoinProbeOperator>(
            this, _id, "spillable_hash_join_probe", _plan_node_id, driver_sequence,
            _hash_joiner_factory->create_prober(degree_of_parallelism, driver_sequence),
            _hash_joiner_factory->get_builder(degree_of_parallelism, driver_sequence));

    prober->set_probe_spiller(spiller);
    prober->set_degree_of_parallelism(degree_of_parallelism);
    prober->set_spill_hash_join_probe_op_max_bytes(_spill_options->spill_hash_join_probe_op_max_bytes);

    return prober;
}

} // namespace starrocks::pipeline
