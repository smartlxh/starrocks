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

#include "scan_profile_collector.h"

#include <rapidjson/allocators.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "runtime/exec_env.h"
#include "util/runtime_profile.h"

namespace starrocks {

const std::vector<std::string> ScanProfileCollector::hive_profile_names{"DataCacheReadCounter",
                                                                        "DataCacheReadBytes",
                                                                        "DataCacheReadMemBytes",
                                                                        "DataCacheReadDiskBytes",
                                                                        "DataCacheSkipReadCounter",
                                                                        "DataCacheSkipReadBytes",
                                                                        "DataCacheReadTimer",
                                                                        "DataCacheWriteCounter",
                                                                        "DataCacheWriteBytes",
                                                                        "DataCacheWriteTimer",
                                                                        "DataCacheWriteFailCounter",
                                                                        "DataCacheWriteFailBytes",
                                                                        "DataCacheReadBlockBufferCounter",
                                                                        "DataCacheReadBlockBufferBytes",
                                                                        "AppIOBytesRead",
                                                                        "AppIOCounter",
                                                                        "AppIOTime",
                                                                        "FSIOBytesRead",
                                                                        "FSIOCounter",
                                                                        "FSIOTime"};
const std::vector<std::string> ScanProfileCollector::lake_profile_names{"CompressedBytesRead",
                                                                        "UncompressedBytesRead",
                                                                        "CompressedBytesReadLocalDisk",
                                                                        "CompressedBytesReadRemote",
                                                                        "CompressedBytesReadRequest",
                                                                        "CompressedBytesReadTotal",
                                                                        "IOCountLocalDisk",
                                                                        "IOCountRemote",
                                                                        "IOCountRequest",
                                                                        "IOCountTotal",
                                                                        "IOTimeLocalDisk",
                                                                        "IOTimeRemote",
                                                                        "IOTimeTotal",
                                                                        "PagesCountLocalDisk",
                                                                        "PagesCountMemory",
                                                                        "PagesCountRemote",
                                                                        "PagesCountTotal",
                                                                        "BytesRead"};
const std::vector<std::string> ScanProfileCollector::common_profile_names{"IOTaskExecTime", "ScanTime"};
const std::string ScanProfileCollector::hive_connector_name = "HiveDataSource";
const std::string ScanProfileCollector::lake_connector_name = "LakeDataSource";
const std::string ScanProfileCollector::unknown_connector_type = "$unknown$";
const std::string ScanProfileCollector::unknown_table_name = "$unknown$";

ScanProfileCollector::ScanProfileCollector(std::vector<std::shared_ptr<RuntimeProfile>>& connector_profiles,
                                           const std::string& query_id, const std::string& fragment_instance_id)
        : _connector_profiles(connector_profiles), _query_id(query_id), _fragment_instance_id(fragment_instance_id) {}

void ScanProfileCollector::print_profile_if_necessary() {
    const auto& connector_name = connector_type();
    if (connector_name == hive_connector_name) {
        do_print(hive_profile_names);
    } else if (connector_name == lake_connector_name) {
        do_print(lake_profile_names);
    } else {
        LOG_EVERY_N(INFO, 100) << "Skip print scan operator's profile since its type is " << connector_name;
    }
}

std::string ScanProfileCollector::connector_type() {
    if (_connector_profiles.size() <= 0) {
        return unknown_connector_type;
    }
    auto* data_source = _connector_profiles[0]->get_child("DataSource");
    if (data_source == nullptr) {
        return unknown_connector_type;
    }
    auto* data_source_type = data_source->get_info_string("DataSourceType");
    if (data_source_type != nullptr) {
        return *data_source_type;
    }
    return unknown_connector_type;
}

std::string ScanProfileCollector::table_name() {
    if (_connector_profiles.size() <= 0) {
        return unknown_table_name;
    }
    auto* data_source = _connector_profiles[0]->get_child("DataSource");
    if (data_source == nullptr) {
        return unknown_table_name;
    }
    auto* table_name = data_source->get_info_string("Table");
    if (table_name != nullptr) {
        return *table_name;
    }
    return unknown_table_name;
}

void ScanProfileCollector::do_print(const std::vector<std::string>& profile_names) {
    std::vector<int64_t> counter_value(profile_names.size() + common_profile_names.size(), 0);
    for (const auto& connector_profile : _connector_profiles) {
        auto* data_source = connector_profile->get_child("DataSource");
        if (data_source == nullptr) {
            continue;
        }
        for (size_t i = 0; i < profile_names.size(); ++i) {
            counter_value[i] += get_counter_value(data_source, profile_names[i]);
        }
        for (size_t i = 0; i < common_profile_names.size(); ++i) {
            counter_value[profile_names.size() + i] += get_counter_value(data_source, common_profile_names[i]);
        }
    }

    rapidjson::Document root;
    root.SetObject();
    auto& allocator = root.GetAllocator();
    for (size_t i = 0; i < profile_names.size(); ++i) {
        rapidjson::Value key(profile_names[i].c_str(), allocator);
        root.AddMember(key, counter_value[i], allocator);
    }
    for (size_t i = 0; i < common_profile_names.size(); ++i) {
        rapidjson::Value key(common_profile_names[i].c_str(), allocator);
        root.AddMember(key, counter_value[profile_names.size() + i], allocator);
    }

    rapidjson::Value value(table_name().c_str(), allocator);
    root.AddMember("Table", value, allocator);

    rapidjson::StringBuffer buffer;
    rapidjson::Writer writer(buffer);
    root.Accept(writer);
    const std::string& profile = buffer.GetString();

    auto& profile_writer = ExecEnv::GetInstance()->get_profile_writer();
    if (profile_writer != nullptr) {
        std::stringstream ss;
        ss << "ScanOperator's profile for query " << _query_id << ", fragment " << _fragment_instance_id << ": "
           << profile << "\n";
        profile_writer->write(ss.str());
    }
}

int64_t ScanProfileCollector::get_counter_value(RuntimeProfile* data_source, const std::string& counter_name) {
    auto* counter = data_source->get_counter(counter_name);
    return counter == nullptr ? 0 : counter->value();
}

} // namespace starrocks