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

#include <memory>
#include <string>
#include <vector>

namespace starrocks {

class RuntimeProfile;

class ScanProfileCollector {
public:
    explicit ScanProfileCollector(std::vector<std::shared_ptr<RuntimeProfile>>& connector_profiles,
                                  const std::string& query_id, const std::string& fragment_instance_id);

    void print_profile_if_necessary();

private:
    static const std::vector<std::string> lake_profile_names;
    static const std::vector<std::string> hive_profile_names;

    static const std::string hive_connector_name;
    static const std::string lake_connector_name;

    static const std::string unknown_connector_type;
    static const std::string unknown_table_name;

    void do_print(const std::vector<std::string>& profile_names);

    std::string connector_type();
    std::string table_name();

    int64_t get_counter_value(RuntimeProfile* data_source, const std::string& counter_name);

    std::vector<std::shared_ptr<RuntimeProfile>>& _connector_profiles;
    const std::string& _query_id;
    const std::string& _fragment_instance_id;
};

} // namespace starrocks