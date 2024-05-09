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
#include <queue>
#include <shared_mutex>

namespace starrocks {

class WritableFile;
class Status;

class ProfileWriter {
public:
    ProfileWriter(const std::string& dir, const uint64_t& max_file_size, const uint32_t& max_file_num);
    ~ProfileWriter();

    bool initialize();
    void write(const std::string& message);

private:
    void rotate(const std::string& new_log_path);
    std::unique_ptr<WritableFile> new_file_writer();
    std::string get_log_suffix();
    void close_file();

    void remove_first_expired_file();

    const static std::string profile_name;

    const std::string _dir;
    const std::string _profile_path;

    uint64_t _current_file_size = 0;
    const uint64_t _max_file_size;

    const uint32_t _max_file_num;

    std::shared_mutex _mutex;
    std::unique_ptr<WritableFile> _writer;
    std::queue<std::string> _file_queue;
};

} // namespace starrocks
