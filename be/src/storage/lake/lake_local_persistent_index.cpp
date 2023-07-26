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

#include "storage/lake/lake_local_persistent_index.h"

#include "gen_cpp/persistent_index.pb.h"
#include "storage/chunk_helper.h"
#include "storage/lake/meta_file.h"
#include "storage/primary_key_encoder.h"
#include "storage/tablet_meta_manager.h"

namespace starrocks::lake {
    Status LakeLocalPersistentIndex::_insert_rowsets(starrocks::lake::Tablet *tablet,
                                                     const Schema &pkey_schema, int64_t base_version,
                                                     std::unique_ptr <Column> pk_column, const TabletMetadata &metadata,
                                                     MetaFileBuilder *builder,
                                                     size_t total_data_size,
                                                     size_t total_segments, size_t total_rows) {
        vector <uint32_t> rowids;
        rowids.reserve(4096);
        auto chunk_shared_ptr = ChunkHelper::new_chunk(pkey_schema, 4096);
        auto chunk = chunk_shared_ptr.get();
        // 2. scan all rowsets and segments to build primary index
        auto rowsets = tablet->get_rowsets(metadata);
        if (!rowsets.ok()) {
            return rowsets.status();
        }

        OlapReaderStatistics stats;

        // NOTICE: primary index will be builded by segment files in metadata, and delvecs.
        // The delvecs we need are stored in delvec file by base_version and current MetaFileBuilder's cache.
        for (auto &rowset: *rowsets) {
            total_data_size += rowset->data_size();
            total_segments += rowset->num_segments();
            total_rows += rowset->num_rows();

            auto res = rowset->get_each_segment_iterator_with_delvec(pkey_schema, base_version, builder, &stats);
            if (!res.ok()) {
                return res.status();
            }
            auto &itrs = res.value();
            CHECK(itrs.size() == rowset->num_segments()) << "itrs.size != num_segments";
            for (size_t i = 0; i < itrs.size(); i++) {
                auto itr = itrs[i].get();
                if (itr == nullptr) {
                    continue;
                }
                while (true) {
                    chunk->reset();
                    rowids.clear();
                    auto st = itr->get_next(chunk, &rowids);
                    if (st.is_end_of_file()) {
                        break;
                    } else if (!st.ok()) {
                        return st;
                    } else {
                        Column *pkc = nullptr;
                        if (pk_column) {
                            pk_column->reset_column();
                            PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, chunk->num_rows(), pk_column.get());
                            pkc = pk_column.get();
                        } else {
                            pkc = chunk->columns()[0].get();
                        }
                        uint32_t rssid = rowset->id() + i;
                        uint64_t base = ((uint64_t) rssid) << 32;
                        std::vector <IndexValue> values;
                        values.reserve(pkc->size());
                        DCHECK(pkc->size() <= rowids.size());
                        for (uint32_t i = 0; i < pkc->size(); i++) {
                            values.emplace_back(base + rowids[i]);
                        }
                        Status st;
                        if (pkc->is_binary()) {
                            st = insert(pkc->size(), reinterpret_cast<const Slice *>(pkc->raw_data()), values.data(),
                                        false);
                        } else {
                            std::vector <Slice> keys;
                            keys.reserve(pkc->size());
                            const auto *fkeys = pkc->continuous_data();
                            for (size_t i = 0; i < pkc->size(); ++i) {
                                keys.emplace_back(fkeys, _key_size);
                                fkeys += _key_size;
                            }
                            st = insert(pkc->size(), reinterpret_cast<const Slice *>(keys.data()), values.data(),
                                        false);
                        }
                        if (!st.ok()) {
                            LOG(ERROR) << "load index failed\n";
                            return st;
                        }
                    }
                }
                itr->close();
            }
        }

    }

    Status
    LakeLocalPersistentIndex::load_from_lake_tablet(starrocks::lake::Tablet *tablet, const TabletMetadata &metadata,
                                                    int64_t base_version, const MetaFileBuilder *builder) {
        if (!is_primary_key(metadata)) {
            LOG(WARNING) << "tablet: " << tablet->id() << " is not primary key tablet";
            return Status::NotSupported("Only PrimaryKey table is supported to use persistent index");
        }

        MonotonicStopWatch timer;
        timer.start();

        PersistentIndexMetaPB index_meta;

        // persistent_index_dir has been checked
        Status status = TabletMetaManager::get_persistent_index_meta(
                StorageEngine::instance()->get_persistent_index_store(), tablet->id(), &index_meta);
        if (!status.ok() && !status.is_not_found()) {
            LOG(ERROR) << "get tablet persistent index meta failed, tablet: " << tablet->id()
                       << "version: " << base_version;
            return Status::InternalError("get tablet persistent index meta failed");
        }

        EditVersion applied_version = EditVersion(base_version, 0);
        if (status.ok()) {
            auto load_index_status = try_load_from_persistent_index(tablet->id(), index_meta, applied_version, timer);

            if (load_index_status.ok()) {
                return load_index_status;
            }
        }

        // 1. create and set key column schema
        std::unique_ptr<TabletSchema> tablet_schema = std::make_unique<TabletSchema>(metadata.schema());
        vector<ColumnId> pk_columns(tablet_schema->num_key_columns());
        for (auto i = 0; i < tablet_schema->num_key_columns(); i++) {
            pk_columns[i] = (ColumnId)i;
        }
        auto pkey_schema = ChunkHelper::convert_schema(*tablet_schema, pk_columns);
        size_t fix_size = PrimaryKeyEncoder::get_encoded_fixed_size(pkey_schema);

        RETURN_IF_ERROR(init_persistent_index(index_meta, applied_version, fix_size));

        size_t total_data_size = 0;
        size_t total_segments = 0;
        size_t total_rows = 0;
        std::unique_ptr <Column> pk_column;
        RETURN_IF_ERROR(_insert_rowsets(tablet, pkey_schema, base_version, std::move(pk_column), total_data_size,
                                        total_segments, total_rows));

        // commit: flush _l0 and build _l1
        // write PersistentIndexMetaPB in RocksDB
        RETURN_IF_ERROR(
                _build_commit(StorageEngine::instance()->get_persistent_index_store(), index_meta, tablet->id()));

        LOG(INFO) << "build persistent index finish tablet: " << tablet->id() << " version:" << base_version
                  << " #rowset:" << tablet->get_rowsets(metadata)->size() << " #segment:" << total_segments << " data_size:"
                  << total_data_size
                  << " size: " << _size << " l0_size: " << _l0->size() << " l0_capacity:" << _l0->capacity()
                  << " #shard: " << (_has_l1 ? _l1_vec[0]->_shards.size() : 0)
                  << " l1_size:" << (_has_l1 ? _l1_vec[0]->_size : 0) << " memory: " << memory_usage()
                  << " time: " << timer.elapsed_time() / 1000000 << "ms";
        return Status::OK();
    }

} // namespace starrocks::lake