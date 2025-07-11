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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/string_util.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/string_util.h"

#include "gutil/strings/split.h"
#include "util/hash_util.hpp"

namespace starrocks {

size_t hash_of_path(const std::string& identifier, const std::string& path) {
    size_t hash = std::hash<std::string>()(identifier);
    std::vector<std::string> path_parts = strings::Split(path, "/", strings::SkipWhitespace());
    for (auto& part : path_parts) {
        HashUtil::hash_combine<std::string>(hash, part);
    }
    return hash;
}

// This function checks if a std::string is heap allocated, which means it doesn't use Small String Optimization
bool is_string_heap_allocated(const std::string& s) {
    uintptr_t data_ptr = reinterpret_cast<uintptr_t>(s.data());
    uintptr_t obj_ptr = reinterpret_cast<uintptr_t>(&s);
    return data_ptr < obj_ptr || data_ptr >= obj_ptr + sizeof(std::string);
}

} // namespace starrocks
