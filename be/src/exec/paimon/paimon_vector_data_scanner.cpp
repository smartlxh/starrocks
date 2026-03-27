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

#include "exec/paimon/paimon_vector_data_scanner.h"

#include <glog/logging.h>

#include <algorithm>
#include <cmath>
#include <random>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "types/datum.h"

namespace starrocks {

Status PaimonVectorDataScanner::do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) {
    _vector_condition = scanner_params.paimon_vector_search_condition;
    if (_vector_condition == nullptr) {
        return Status::InvalidArgument("PaimonVectorDataScanner requires a vector search condition");
    }
    LOG(INFO) << "PaimonVectorDataScanner::do_init shard_id=" << _vector_condition->shard_id
              << " function=" << _vector_condition->score_function_name
              << " column=" << _vector_condition->vector_column_name
              << " limit_per_shard=" << _vector_condition->limit_per_shard;
    return Status::OK();
}

Status PaimonVectorDataScanner::do_open(RuntimeState* state) {
    // TODO: open Paimon native vector index and execute ANN search for this shard
    // For now we generate mock data in do_get_next.
    return Status::OK();
}

void PaimonVectorDataScanner::do_close(RuntimeState* runtime_state) noexcept {
    // TODO: release Paimon native index resources
}

// Compute L2 distance between two vectors.
static double compute_l2_distance(const std::vector<double>& query, const std::vector<float>& target) {
    double dist = 0.0;
    size_t dim = std::min(query.size(), target.size());
    for (size_t i = 0; i < dim; i++) {
        double d = query[i] - static_cast<double>(target[i]);
        dist += d * d;
    }
    return std::sqrt(dist);
}

// Compute cosine similarity between two vectors.
static double compute_cosine_similarity(const std::vector<double>& query, const std::vector<float>& target) {
    double dot = 0.0, norm_q = 0.0, norm_t = 0.0;
    size_t dim = std::min(query.size(), target.size());
    for (size_t i = 0; i < dim; i++) {
        dot += query[i] * static_cast<double>(target[i]);
        norm_q += query[i] * query[i];
        norm_t += static_cast<double>(target[i]) * static_cast<double>(target[i]);
    }
    double denom = std::sqrt(norm_q) * std::sqrt(norm_t);
    return denom > 0 ? dot / denom : 0.0;
}

Status PaimonVectorDataScanner::do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) {
    if (_is_finished) {
        return Status::EndOfFile("PaimonVectorDataScanner finished");
    }
    _is_finished = true;

    // Determine how many mock rows to produce.
    int64_t limit = 10; // default
    if (_vector_condition->__isset.limit_per_shard && _vector_condition->limit_per_shard > 0) {
        limit = _vector_condition->limit_per_shard;
    }
    const int64_t num_rows = limit;

    // Determine vector dimension from query_vector.
    int dim = 3; // default
    if (_vector_condition->__isset.query_vector && !_vector_condition->query_vector.empty()) {
        dim = static_cast<int>(_vector_condition->query_vector.size());
    }

    // Generate mock vector data and compute scores.
    std::mt19937 gen(42 + _vector_condition->shard_id);
    std::uniform_real_distribution<float> dist(0.0f, 10.0f);

    struct MockRow {
        std::string pk;
        std::vector<float> vec;
        double score;
    };
    std::vector<MockRow> rows;
    rows.reserve(num_rows);

    bool is_l2 = (_vector_condition->score_function_name.find("l2") != std::string::npos);

    for (int64_t i = 0; i < num_rows; i++) {
        MockRow row;
        row.pk = "mock_" + std::to_string(_vector_condition->shard_id) + "_" + std::to_string(i);
        row.vec.resize(dim);
        for (int d = 0; d < dim; d++) {
            row.vec[d] = dist(gen);
        }
        if (_vector_condition->__isset.query_vector && !_vector_condition->query_vector.empty()) {
            if (is_l2) {
                row.score = compute_l2_distance(_vector_condition->query_vector, row.vec);
            } else {
                row.score = compute_cosine_similarity(_vector_condition->query_vector, row.vec);
            }
        } else {
            row.score = static_cast<double>(i);
        }
        rows.push_back(std::move(row));
    }

    // Sort by score: ASC for L2 distance, DESC for cosine similarity.
    bool ascending = !_vector_condition->__isset.result_order || _vector_condition->result_order == 0;
    if (ascending) {
        std::sort(rows.begin(), rows.end(), [](const MockRow& a, const MockRow& b) { return a.score < b.score; });
    } else {
        std::sort(rows.begin(), rows.end(), [](const MockRow& a, const MockRow& b) { return a.score > b.score; });
    }

    // Build chunk according to materialized_columns in scanner context.
    // The FE tells us which columns to output and in which order via materialized_columns.
    auto& ctx = _scanner_ctx;
    ChunkPtr result = std::make_shared<Chunk>();

    for (auto& col_info : ctx.materialized_columns) {
        const std::string& col_name = col_info.slot_desc->col_name();
        auto col = ColumnHelper::create_column(col_info.slot_desc->type(), col_info.slot_desc->is_nullable());

        if (col_name == _vector_condition->vector_column_name) {
            // Vector column: ARRAY<FLOAT>
            for (int64_t i = 0; i < num_rows; i++) {
                DatumArray arr;
                for (float v : rows[i].vec) {
                    arr.emplace_back(v);
                }
                col->append_datum(Datum(arr));
            }
        } else if (col_name == "__vector_score__" || col_name == "score" ||
                   col_info.slot_desc->type().type == TYPE_DOUBLE || col_info.slot_desc->type().type == TYPE_FLOAT) {
            // Score column: use computed score.
            // Heuristic: if it's a double/float column and not the vector column, treat as score.
            // In practice the FE rewrites approx_l2_distance(...) into a score output slot.
            for (int64_t i = 0; i < num_rows; i++) {
                col->append_datum(Datum(rows[i].score));
            }
        } else {
            // Other columns (e.g. pk): fill with mock string/int data.
            for (int64_t i = 0; i < num_rows; i++) {
                if (col_info.slot_desc->type().type == TYPE_VARCHAR || col_info.slot_desc->type().type == TYPE_CHAR) {
                    col->append_datum(Datum(Slice(rows[i].pk)));
                } else if (col_info.slot_desc->type().type == TYPE_INT) {
                    col->append_datum(Datum(static_cast<int32_t>(i)));
                } else if (col_info.slot_desc->type().type == TYPE_BIGINT) {
                    col->append_datum(Datum(static_cast<int64_t>(i)));
                } else {
                    // Fallback: append null or default.
                    col->append_default();
                }
            }
        }
        result->append_column(col, col_info.slot_desc->id());
    }

    // Append partition and extended columns.
    ctx.append_or_update_partition_column_to_chunk(&result, num_rows);
    ctx.append_or_update_extended_column_to_chunk(&result, num_rows);

    LOG(INFO) << "PaimonVectorDataScanner::do_get_next produced " << num_rows << " mock rows"
              << " for shard_id=" << _vector_condition->shard_id;

    *chunk = std::move(result);
    return Status::OK();
}

} // namespace starrocks
