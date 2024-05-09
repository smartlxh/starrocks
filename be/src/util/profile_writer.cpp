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

#include "profile_writer.h"

#include <chrono>
#include <iomanip>

#include "fs/fs.h"

namespace starrocks {

const std::string ProfileWriter::profile_name = "profile.log";

ProfileWriter::ProfileWriter(const std::string& dir, const uint64_t& max_file_size, const uint32_t& max_file_num)
        : _dir(std::move(dir)),
          _profile_path(_dir + "/" + profile_name),
          _max_file_size(max_file_size),
          _max_file_num(max_file_num),
          _writer(nullptr) {}

ProfileWriter::~ProfileWriter() {
    std::unique_lock<std::shared_mutex> lock(_mutex);
    if (_writer != nullptr) {
        close_file();
        _writer = nullptr;
    }
}

bool ProfileWriter::initialize() {
    auto st = FileSystem::Default()->is_directory(_dir);
    if (!st.ok()) {
        LOG(ERROR) << "ScanOperator's profile log dir " << _dir << ", reason: " << st.status().message();
        return false;
    }
    std::vector<std::string> children;
    auto status = FileSystem::Default()->get_children(_dir, &children);
    if (!status.ok()) {
        LOG(ERROR) << "Failed to list ScanOperator's profile log dir " << _dir << ", reason: " << status.message();
        return false;
    }
    std::sort(children.begin(), children.end());

    // remove expired log.
    for (const auto& file : children) {
        if (file.starts_with(profile_name)) {
            const std::string& absolute_path = _dir + "/" + file;
            _file_queue.push(absolute_path);
        } else if (file.starts_with(_profile_path)) {
            _file_queue.push(file);
        }
    }

    while (_file_queue.size() > _max_file_num) {
        remove_first_expired_file();
    }
    return true;
}

void ProfileWriter::remove_first_expired_file() {
    // remove expired file.
    const auto& expired_log_path = _file_queue.front();
    _file_queue.pop();
    auto status = FileSystem::Default()->delete_file(expired_log_path);
    if (!status.ok()) {
        LOG(WARNING) << "Failed to delete log file " << expired_log_path << ", reason: " << status.message();
    }
}

void ProfileWriter::close_file() {
    Status st = _writer->sync();
    if (!st.ok()) {
        LOG(WARNING) << "Failed to do sync for file " << _writer->filename() << ", reason: " << st.message();
    }
    st = _writer->close();
    if (!st.ok()) {
        LOG(WARNING) << "Failed to do close for file " << _writer->filename() << ", reason: " << st.message();
    }
}

void ProfileWriter::write(const std::string& message) {
    std::unique_lock<std::shared_mutex> lock(_mutex);
    if (!_writer) {
        _writer = new_file_writer();
    }

    if (_current_file_size + message.size() > _max_file_size) {
        close_file();
        _writer = new_file_writer();
    }

    auto st = _writer->append(Slice(message));
    if (!st.ok()) {
        LOG(WARNING) << "Failed to write log into file " << _writer->filename() << ", reason: " << st.message();
        return;
    }
    st = _writer->flush(WritableFile::FlushMode::FLUSH_ASYNC);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to flush log into file " << _writer->filename() << ", reason: " << st.message();
        return;
    }
    _current_file_size += message.size();
}

std::unique_ptr<WritableFile> ProfileWriter::new_file_writer() {
    const std::string& file_name = _profile_path + "." + get_log_suffix();
    auto file_writer = FileSystem::Default()->new_writable_file(file_name);
    if (!file_writer.ok()) {
        LOG(ERROR) << "Failed to create profile log writer " << file_name
                   << ", reason: " << file_writer.status().message();
        return nullptr;
    }
    _current_file_size = 0;

    rotate(file_name);
    return std::move(file_writer.value());
}

void ProfileWriter::rotate(const std::string& new_log_path) {
    const std::string& symbolic_path = _profile_path;
    auto st = FileSystem::Default()->path_exists(symbolic_path);
    if (st.ok()) {
        st = FileSystem::Default()->delete_file(symbolic_path);
        if (!st.ok()) {
            LOG(WARNING) << "Failed to delete symbolic path " << symbolic_path << ", reason: " << st.message();
        }
    }

    st = FileSystem::Default()->link_file(new_log_path, symbolic_path, FileSystem::LinkMode::SOFT);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to link path " << new_log_path << " to path " << symbolic_path
                     << ", reason: " << st.message();
    }

    if (_file_queue.size() + 1 > _max_file_num) {
        remove_first_expired_file();
    }
    _file_queue.push(new_log_path);
}

std::string ProfileWriter::get_log_suffix() {
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    std::tm tm = *std::localtime(&time);

    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y%m%d-%H%M%S");
    return oss.str();
}

} // namespace starrocks
