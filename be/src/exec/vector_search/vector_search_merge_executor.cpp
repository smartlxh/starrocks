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

#include "exec/vector_search/vector_search_merge_executor.h"

#include <algorithm>
#include <utility>

namespace starrocks::vector_search {

VectorSearchMergeExecutor::VectorSearchMergeExecutor(VectorSearchExecutor* executor,
                                                     VectorSearchMergeExecutorOptions options,
                                                     std::unique_ptr<VectorSearchMergePolicy> merge_policy)
        : _executor(executor), _options(options), _merge_policy(std::move(merge_policy)) {
    _scheduler = std::thread([this] { _schedule_loop(); });
}

VectorSearchMergeExecutor::~VectorSearchMergeExecutor() {
    shutdown();
}

Status VectorSearchMergeExecutor::submit(std::shared_ptr<VectorSearchTask> task) {
    if (_executor == nullptr) {
        return Status::InvalidArgument("vector search executor is null");
    }
    if (task == nullptr || task->completion == nullptr) {
        return Status::InvalidArgument("vector search task or completion is null");
    }
    if (_options.max_inflight_tasks == 0) {
        return Status::InvalidArgument("vector search max inflight tasks must be positive");
    }

    std::lock_guard lock(_mutex);
    if (_stopping) {
        return Status::ServiceUnavailable("vector search merge executor is stopping");
    }
    if (_tasks.contains(task->id)) {
        return Status::AlreadyExist("duplicate vector search task id");
    }

    for (const auto& queued : _pending) {
        if (_merge_policy != nullptr && _merge_policy->try_merge(queued, task)) {
            return Status::OK();
        }
    }

    if (_pending.size() >= _options.max_pending_tasks) {
        return Status::ServiceUnavailable("vector search pending queue is full");
    }

    _tasks.emplace(task->id, ActiveTask{task, nullptr, false});
    _pending.emplace_back(std::move(task));
    _cv.notify_one();
    return Status::OK();
}

bool VectorSearchMergeExecutor::cancel(const VectorSearchTaskId& id) {
    std::shared_ptr<VectorSearchTask> pending_task;
    std::shared_ptr<VectorSearchExecutionHandle> handle;
    {
        std::lock_guard lock(_mutex);
        auto task_it = _tasks.find(id);
        if (task_it == _tasks.end()) {
            return false;
        }
        handle = task_it->second.handle;
        if (handle == nullptr) {
            auto pending_it =
                    std::find_if(_pending.begin(), _pending.end(), [&id](const auto& task) { return task->id == id; });
            if (pending_it != _pending.end()) {
                pending_task = std::move(*pending_it);
                _pending.erase(pending_it);
                _tasks.erase(task_it);
            } else {
                // The scheduler has admitted this task and is between submit() and publishing its
                // execution handle. Remember cancellation and apply it as soon as the handle exists.
                task_it->second.cancel_requested = true;
                return true;
            }
        }
    }

    if (pending_task != nullptr) {
        _complete(pending_task, Status::Cancelled("vector search cancelled while pending"), {});
        return true;
    }
    if (handle != nullptr) {
        handle->cancel();
        return true;
    }
    return false;
}

void VectorSearchMergeExecutor::shutdown() {
    std::vector<std::shared_ptr<VectorSearchTask>> pending;
    std::vector<std::shared_ptr<VectorSearchExecutionHandle>> handles;
    {
        std::lock_guard lock(_mutex);
        if (_stopping) {
            if (_scheduler.joinable()) {
                // Another caller initiated shutdown. The scheduler still needs to be joined below.
            } else {
                return;
            }
        }
        _stopping = true;
        pending.assign(_pending.begin(), _pending.end());
        for (const auto& task : pending) {
            _tasks.erase(task->id);
        }
        _pending.clear();
        for (const auto& [_, active] : _tasks) {
            if (active.handle != nullptr) {
                handles.emplace_back(active.handle);
            }
        }
        _cv.notify_all();
    }

    for (const auto& task : pending) {
        _complete(task, Status::Cancelled("vector search merge executor is shutting down"), {});
    }
    for (const auto& handle : handles) {
        handle->cancel();
    }
    if (_scheduler.joinable()) {
        _scheduler.join();
    }
}

size_t VectorSearchMergeExecutor::pending_tasks() const {
    std::lock_guard lock(_mutex);
    return _pending.size();
}

size_t VectorSearchMergeExecutor::inflight_tasks() const {
    std::lock_guard lock(_mutex);
    return _inflight;
}

void VectorSearchMergeExecutor::_schedule_loop() {
    while (true) {
        std::shared_ptr<VectorSearchTask> task;
        {
            std::unique_lock lock(_mutex);
            _cv.wait(lock, [this] {
                return (_stopping && _inflight == 0) || (!_pending.empty() && _inflight < _options.max_inflight_tasks);
            });
            if (_stopping && _inflight == 0) {
                return;
            }
            if (_pending.empty() || _inflight >= _options.max_inflight_tasks) {
                continue;
            }
            task = std::move(_pending.front());
            _pending.pop_front();
            ++_inflight;
        }
        _dispatch(std::move(task));
    }
}

void VectorSearchMergeExecutor::_dispatch(std::shared_ptr<VectorSearchTask> task) {
    auto handle_or =
            _executor->submit(task->options, task->workgroup, std::move(task->work_items),
                              [this, task](Status status, std::vector<VectorSearchCandidate> candidates) mutable {
                                  _finish(task, std::move(status), std::move(candidates));
                              });
    if (!handle_or.ok()) {
        _finish(task, handle_or.status(), {});
        return;
    }

    std::shared_ptr<VectorSearchExecutionHandle> handle = std::move(handle_or).value();
    bool cancel_requested = false;
    {
        std::lock_guard lock(_mutex);
        auto task_it = _tasks.find(task->id);
        if (task_it != _tasks.end()) {
            task_it->second.handle = handle;
            cancel_requested = task_it->second.cancel_requested;
        }
    }
    if (cancel_requested) {
        handle->cancel();
    }
}

void VectorSearchMergeExecutor::_finish(const std::shared_ptr<VectorSearchTask>& task, Status status,
                                        std::vector<VectorSearchCandidate> candidates) {
    {
        std::lock_guard lock(_mutex);
        auto task_it = _tasks.find(task->id);
        if (task_it == _tasks.end()) {
            return;
        }
        _tasks.erase(task_it);
        DCHECK_GT(_inflight, 0);
        --_inflight;
        _cv.notify_all();
    }
    _complete(task, std::move(status), std::move(candidates));
}

void VectorSearchMergeExecutor::_complete(const std::shared_ptr<VectorSearchTask>& task, Status status,
                                          std::vector<VectorSearchCandidate> candidates) {
    if (task->completion != nullptr) {
        task->completion(std::move(status), std::move(candidates));
    }
}

} // namespace starrocks::vector_search
