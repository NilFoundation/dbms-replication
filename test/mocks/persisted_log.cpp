//---------------------------------------------------------------------------//
// Copyright (c) 2018-2022 Mikhail Komarov <nemo@nil.foundation>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the Server Side Public License, version 1,
// as published by the author.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// Server Side Public License for more details.
//
// You should have received a copy of the Server Side Public License
// along with this program. If not, see
// <https://github.com/NilFoundation/dbms/blob/master/LICENSE_1_0.txt>.
//---------------------------------------------------------------------------//

#include "persisted_log.hpp"

using namespace nil::dbms;
using namespace nil::dbms::replication;
using namespace nil::dbms::replication::replicated_log;
using namespace nil::dbms::replication::test;

auto mock_log::insert(persisted_log_iterator &iter, write_options const &opts) -> nil::dbms::Result {
    auto lastIndex = log_index {0};

    while (auto entry = iter.next()) {
        auto const res = _storage.try_emplace(entry->logIndex(), entry.value());
        TRI_ASSERT(res.second);

        TRI_ASSERT(entry->logIndex() > lastIndex);
        lastIndex = entry->logIndex();
        if (opts.waitForSync) {
            _writtenWithWaitForSync.insert(entry->logIndex());
        }
    }

    return {};
}

template<typename I>
struct MockLogContainerIterator : persisted_log_iterator {
    MockLogContainerIterator(mock_log::storeType store, log_index start) :
        _store(std::move(store)), _current(_store.lower_bound(start)), _end(_store.end()) {
    }

    auto next() -> std::optional<persisting_log_entry> override {
        if (_current != _end) {
            auto it = _current;
            ++_current;
            return it->second;
        }
        return std::nullopt;
    }

    mock_log::storeType _store;
    I _current;
    I _end;
};

auto mock_log::read(replication::log_index start) -> std::unique_ptr<persisted_log_iterator> {
    return std::make_unique<MockLogContainerIterator<iteratorType>>(_storage, start);
}

auto mock_log::removeFront(replication::log_index stop) -> futures::Future<Result> {
    _storage.erase(_storage.begin(), _storage.lower_bound(stop));
    return Result {};
}

auto mock_log::removeBack(replication::log_index start) -> Result {
    _storage.erase(_storage.lower_bound(start), _storage.end());
    return {};
}

auto mock_log::drop() -> Result {
    _storage.clear();
    return {};
}

void mock_log::setEntry(replication::log_index idx, replication::log_term term,
                       replication::log_payload payload) {
    _storage.emplace(std::piecewise_construct, std::forward_as_tuple(idx),
                     std::forward_as_tuple(term, idx, std::move(payload)));
}

mock_log::mock_log(replication::LogId id) : mock_log(id, {}) {
}

mock_log::mock_log(replication::global_log_identifier gid) :
    replication::replicated_log::persisted_log(std::move(gid)) {
}

mock_log::mock_log(replication::LogId id, mock_log::storeType storage) :
    replication::replicated_log::persisted_log(global_log_identifier("", id)), _storage(std::move(storage)) {
}

AsyncMockLog::AsyncMockLog(replication::LogId id) : mock_log(id), _asyncWorker([this] { this->runWorker(); }) {
}

AsyncMockLog::~AsyncMockLog() noexcept {
    stop();
}

void mock_log::setEntry(replication::persisting_log_entry entry) {
    _storage.emplace(entry.logIndex(), std::move(entry));
}

auto mock_log::insertAsync(std::unique_ptr<persisted_log_iterator> iter, write_options const &opts)
    -> futures::Future<Result> {
    return insert(*iter, opts);
}

auto AsyncMockLog::insertAsync(std::unique_ptr<persisted_log_iterator> iter, write_options const &opts)
    -> futures::Future<Result> {
    auto entry = std::make_shared<QueueEntry>();
    entry->opts = opts;
    entry->iter = std::move(iter);

    {
        std::unique_lock guard(_mutex);
        TRI_ASSERT(!_stopped);
        TRI_ASSERT(!_stopping);
        _queue.emplace_back(entry);
        _cv.notify_all();
    }

    return entry->promise.getFuture();
}

void AsyncMockLog::runWorker() {
    bool cont = true;
    while (cont) {
        std::vector<std::shared_ptr<QueueEntry>> queue;
        {
            std::unique_lock guard(_mutex);
            if (_queue.empty()) {
                if (_stopping.load()) {
                    _stopped = true;
                    return;
                }
                _cv.wait(guard);
            } else {
                std::swap(queue, _queue);
            }
        }
        for (auto &entry : queue) {
            auto res = insert(*entry->iter, entry->opts);
            entry->promise.setValue(res);
        }
    }
}

auto DelayedMockLog::insertAsync(std::unique_ptr<replication::persisted_log_iterator> iter,
                                 persisted_log::write_options const &opts) -> futures::Future<Result> {
    TRI_ASSERT(!_pending.has_value());
    return _pending.emplace(std::move(iter), opts).promise.getFuture();
}

void DelayedMockLog::runAsyncInsert() {
    TRI_ASSERT(_pending.has_value());
    mock_log::insertAsync(std::move(_pending->iter), _pending->options).thenFinal([this](futures::Try<Result> &&res) {
        auto promise = std::move(_pending->promise);
        _pending.reset();
        promise.setTry(std::move(res));
    });
}

DelayedMockLog::PendingRequest::PendingRequest(std::unique_ptr<replication::persisted_log_iterator> iter,
                                               write_options options) :
    iter(std::move(iter)),
    options(options) {
}
