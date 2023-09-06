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

#include <gtest/gtest.h>

#include "fs/fs_util.h"
#include "storage/lake/tablet_manager.h"
#include "test_util.h"

namespace starrocks::lake {

using namespace starrocks;

class AlterTabletMetaTest : public TestBase {
public:
    AlterTabletMetaTest() : TestBase(kTestDirectory) {
        _tablet_metadata = std::make_unique<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
    }

    void SetUp() override {
        clear_and_init_test_dir();

        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

protected:
    constexpr static const char* const kTestDirectory = "test_alter_tablet_meta";

    std::unique_ptr<TabletMetadata> _tablet_metadata;
};

} // namespace starrocks::lake