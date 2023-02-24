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

#pragma once

#include <nil/dbms/replication/replicated_log/ilog_interfaces.hpp>
#include <nil/dbms/replication/replicated_log/inmemory_log.hpp>
#include <nil/dbms/replication/replicated_log/log_core.hpp>
#include <nil/dbms/replication/replicated_log/log_follower.hpp>
#include <nil/dbms/replication/replicated_log/log_leader.hpp>
#include <nil/dbms/replication/replicated_log/log_status.hpp>
#include <nil/dbms/replication/replicated_log/persisted_log.hpp>
#include <nil/dbms/replication/replicated_log/replicated_log.hpp>
#include <nil/dbms/replication/replicated_log/types.hpp>

namespace nil::dbms::replication::test {

    using namespace replicated_log;

    struct mock_log : replication::replicated_log::persisted_log {
        using storeType = std::map<replication::log_index, replication::persisting_log_entry>;

        explicit mock_log(replication::LogId id);
        explicit mock_log(replication::global_log_identifier gid);
        mock_log(replication::LogId id, storeType storage);

        auto insert(replication::persisted_log_iterator &iter, write_options const &) -> Result override;
        auto insertAsync(std::unique_ptr<replication::persisted_log_iterator> iter, write_options const &)
            -> futures::Future<Result> override;
        auto read(replication::log_index start)
            -> std::unique_ptr<replication::persisted_log_iterator> override;
        auto removeFront(replication::log_index stop) -> futures::Future<Result> override;
        auto removeBack(replication::log_index start) -> Result override;
        auto drop() -> Result override;

        void setEntry(replication::log_index idx, replication::log_term term,
                      replication::log_payload payload);
        void setEntry(replication::persisting_log_entry);

        [[nodiscard]] storeType getStorage() const {
            return _storage;
        }

        auto checkEntryWaitedForSync(log_index idx) const noexcept -> bool {
            return _writtenWithWaitForSync.find(idx) != _writtenWithWaitForSync.end();
        }

    private:
        using iteratorType = storeType::iterator;
        storeType _storage;
        std::unordered_set<log_index> _writtenWithWaitForSync;
    };

    struct DelayedMockLog : mock_log {
        explicit DelayedMockLog(replication::LogId id) : mock_log(id) {
        }

        auto insertAsync(std::unique_ptr<replication::persisted_log_iterator> iter, write_options const &)
            -> futures::Future<Result> override;

        auto hasPendingInsert() const noexcept -> bool {
            return _pending.has_value();
        }
        void runAsyncInsert();

        struct PendingRequest {
            PendingRequest(std::unique_ptr<replication::persisted_log_iterator> iter, write_options options);
            std::unique_ptr<replication::persisted_log_iterator> iter;
            write_options options;
            futures::Promise<Result> promise;
        };

        std::optional<PendingRequest> _pending;
    };

    struct AsyncMockLog : mock_log {
        explicit AsyncMockLog(replication::LogId id);

        ~AsyncMockLog() noexcept;

        auto insertAsync(std::unique_ptr<replication::persisted_log_iterator> iter, write_options const &)
            -> futures::Future<Result> override;

        auto stop() noexcept -> void {
            if (!_stopping) {
                {
                    std::unique_lock guard(_mutex);
                    _stopping = true;
                    _cv.notify_all();
                }
                _asyncWorker.join();
            }
        }

    private:
        struct QueueEntry {
            write_options opts;
            std::unique_ptr<replication::persisted_log_iterator> iter;
            futures::Promise<Result> promise;
        };

        void runWorker();

        std::mutex _mutex;
        std::vector<std::shared_ptr<QueueEntry>> _queue;
        std::condition_variable _cv;
        std::atomic<bool> _stopping = false;
        bool _stopped = false;
        // _asyncWorker *must* be initialized last, otherwise starting the thread
        // races with initializing the coordination variables.
        std::thread _asyncWorker;
    };

}    // namespace nil::dbms::replication::test
