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

#include "exec/vector_search/lake_vector_search_reader.h"

#include <algorithm>
#include <utility>

#include "base/utility/defer_op.h"
#include "column/chunk_factory.h"
#include "storage/chunk_helper.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/primitive/vector_search_option.h"
#include "storage/tablet_reader_params.h"
#include "storage/tablet_schema.h"
#include "types/datum.h"
#include "types/logical_type.h"

namespace starrocks::vector_search {

Status search_lake_tablet(lake::TabletManager* tablet_manager, const LakeVectorSearchSpec& spec,
                          const LakeVectorSearchTablet& tablet, std::vector<VectorSearchCandidate>* candidates) {
    if (tablet_manager == nullptr || candidates == nullptr) {
        return Status::InvalidArgument("vector search tablet manager or output is null");
    }
    if (tablet.tablet_id <= 0 || tablet.version <= 0) {
        return Status::InvalidArgument("vector search tablet id and version must be positive");
    }
    if (spec.top_k <= 0 || spec.query_vector.empty()) {
        return Status::InvalidArgument("vector search top_k and query vector must be non-empty");
    }

    ASSIGN_OR_RETURN(auto versioned_tablet, tablet_manager->get_tablet(tablet.tablet_id, tablet.version));
    auto tablet_schema = versioned_tablet.get_schema();
    if (tablet_schema == nullptr) {
        return Status::InternalError("vector search tablet schema is null");
    }

    const int32_t id_column_id = tablet_schema->field_index(spec.id_column_name);
    const int32_t vector_column_id = tablet_schema->field_index(spec.vector_column_name);
    if (id_column_id < 0 || vector_column_id < 0) {
        return Status::InvalidArgument("vector search id or vector column does not exist");
    }
    if (tablet_schema->column(id_column_id).type() != TYPE_BIGINT) {
        return Status::NotSupported("direct vector search V1 requires a BIGINT id column");
    }

    Schema read_schema = ChunkHelper::convert_schema(tablet_schema, {static_cast<ColumnId>(id_column_id)});
    ASSIGN_OR_RETURN(auto reader, versioned_tablet.new_reader(std::move(read_schema)));
    DeferOp close_reader([&reader] { reader->close(); });

    TabletReaderParams params;
    params.reader_type = READER_QUERY;
    params.is_pipeline = false;
    params.skip_aggregation = true;
    params.use_page_cache = true;
    params.chunk_size = std::max<int32_t>(spec.top_k, 1024);
    params.use_vector_index = true;
    params.has_predicate_above_iterator = false;
    params.vector_search_option = std::make_shared<VectorSearchOption>();
    params.vector_search_option->k = spec.top_k;
    params.vector_search_option->query_vector = spec.query_vector;
    params.vector_search_option->vector_distance_column_name = "__vector_search_distance";
    params.vector_search_option->use_vector_index = true;
    params.vector_search_option->vector_column_id = vector_column_id;
    params.vector_search_option->vector_slot_id = tablet_schema->num_columns();
    params.vector_search_option->vector_range = -1;
    params.vector_search_option->result_order = static_cast<int>(spec.result_order);
    params.vector_search_option->refine_distance = false;
    params.vector_search_option->k_factor = 1.0;
    params.vector_search_option->pq_refine_factor = 1.0;
    if (spec.ef_search > 0) {
        params.vector_search_option->query_params["efSearch"] = std::to_string(spec.ef_search);
    }

    RETURN_IF_ERROR(reader->prepare());
    RETURN_IF_ERROR(reader->open(params));

    while (true) {
        auto chunk = ChunkFactory::new_chunk(reader->output_schema(), params.chunk_size);
        Status status = reader->get_next(chunk.get());
        if (status.is_end_of_file()) {
            break;
        }
        RETURN_IF_ERROR(status);
        if (chunk->num_columns() < 2) {
            return Status::InternalError("vector search reader did not append a distance column");
        }

        const auto& id_column = chunk->get_column_by_index(0);
        const auto& score_column = chunk->get_column_by_index(chunk->num_columns() - 1);
        for (size_t row = 0; row < chunk->num_rows(); ++row) {
            Datum id = id_column->get(row);
            Datum score = score_column->get(row);
            if (id.is_null() || score.is_null()) {
                continue;
            }
            const int64_t int64_id = id.get_int64();
            candidates->push_back(VectorSearchCandidate{.score = score.get_float(),
                                                        .tie_breaker = static_cast<uint64_t>(int64_id),
                                                        .encoded_row = {},
                                                        .int64_id = int64_id});
        }
    }
    return Status::OK();
}

VectorSearchWork make_lake_vector_search_work(lake::TabletManager* tablet_manager,
                                              std::shared_ptr<const LakeVectorSearchSpec> spec,
                                              LakeVectorSearchTablet tablet) {
    return [tablet_manager, spec = std::move(spec), tablet](std::vector<VectorSearchCandidate>* candidates) {
        if (spec == nullptr) {
            return Status::InvalidArgument("vector search specification is null");
        }
        return search_lake_tablet(tablet_manager, *spec, tablet, candidates);
    };
}

} // namespace starrocks::vector_search
