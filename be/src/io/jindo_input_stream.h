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

#include <fmt/format.h>

#include <memory>
#include <string>

#include "fs/fs_jindo.h"
#include "io/seekable_input_stream.h"
#include "jindo_utils.h"
#include "jindosdk/jdo_api.h"
#include "jindosdk/jdo_defines.h"
#include "util/uid_util.h"

namespace starrocks::io {

class JindoInputStream final : public SeekableInputStream {
public:
    explicit JindoInputStream(std::shared_ptr<JindoClient> client, std::string file_path)
            : _jindo_client(std::move(client)), _open_handle(nullptr), _file_path(std::move(file_path)) {
        _stream_uuid = generate_uuid_string();
    }

    ~JindoInputStream() override {
        auto status = get_numeric_statistics();
        if (status.ok()) {
            LOG(INFO) << fmt::format("JindoInputStream[{}] get_numeric_statistics: {}", _stream_uuid, status.value());
        }
        if (_open_handle != nullptr) {
            auto jdo_ctx = jdo_createHandleCtx2(*(_jindo_client->jdo_store), _open_handle);
            jdo_close(jdo_ctx, nullptr);
            Status init_status = io::check_jindo_status(jdo_ctx);
            jdo_freeHandleCtx(jdo_ctx);
            jdo_freeIOContext(_open_handle);
        }
        _open_handle = nullptr;
        _jindo_client.reset();
    };

    // Disallow copy and assignment
    JindoInputStream(const JindoInputStream&) = delete;
    void operator=(const JindoInputStream&) = delete;

    // Disallow move ctor and move assignment, because no usage now
    JindoInputStream(JindoInputStream&&) = delete;
    void operator=(JindoInputStream&&) = delete;

    StatusOr<int64_t> read(void* data, int64_t count) override;

    Status seek(int64_t offset) override;

    StatusOr<int64_t> position() override;

    StatusOr<int64_t> get_size() override;

    void set_size(int64_t size) override;

    StatusOr<std::unique_ptr<io::NumericStatistics>> get_numeric_statistics() override;

private:
    std::shared_ptr<JindoClient> _jindo_client;
    JdoIOContext_t _open_handle;
    std::string _file_path;
    int64_t _offset{0};
    int64_t _size{-1};
    std::string _stream_uuid;
};

} // namespace starrocks::io
