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
#include <deque>
#include <memory>

#include "../mocks/replicated_log_metrics_mock.hpp"
#include <nil/dbms/replication/replicated_log/ilog_interfaces.hpp>
#include <nil/dbms/replication/replicated_log/inmemory_log.hpp>
#include <nil/dbms/replication/replicated_log/log_common.hpp>
#include <nil/dbms/replication/replicated_log/log_core.hpp>
#include <nil/dbms/replication/replicated_log/log_follower.hpp>
#include <nil/dbms/replication/replicated_log/log_leader.hpp>
#include <nil/dbms/replication/replicated_log/log_status.hpp>
#include <nil/dbms/replication/replicated_log/persisted_log.hpp>
#include <nil/dbms/replication/replicated_log/replicated_log.hpp>
#include <nil/dbms/replication/replicated_log/types.hpp>

namespace nil::dbms::cluster {
    struct IFailureOracle;
}
namespace nil::dbms::replication::test {

    struct DelayedFollowerLog : replicated_log::abstract_follower, replicated_log::ilog_participant {
        explicit DelayedFollowerLog(std::shared_ptr<replicated_log::log_follower> follower) :
            _follower(std::move(follower)) {
        }

        DelayedFollowerLog(logger_context const &logContext, std::shared_ptr<replicated_log_metrics_mock> logMetricsMock,
                           std::shared_ptr<replicated_log_global_settings const> options, ParticipantId const &id,
                           std::unique_ptr<replicated_log::log_core> logCore, log_term term, ParticipantId leaderId) :
            DelayedFollowerLog([&] {
                return replicated_log::log_follower::construct(logContext, std::move(logMetricsMock), id,
                                                               std::move(logCore), term, std::move(leaderId));
            }()) {
        }

        auto appendEntries(replicated_log::append_entries_request req)
            -> nil::dbms::futures::Future<replicated_log::append_entries_result> override {
            auto future = _asyncQueue.doUnderLock([&](auto &queue) {
                return queue.emplace_back(std::make_shared<AsyncRequest>(std::move(req)))->promise.getFuture();
            });
            return std::move(future).thenValue([this](auto &&result) mutable {
                return _follower->appendEntries(std::forward<decltype(result)>(result));
            });
        }

        void runAsyncAppendEntries() {
            auto asyncQueue = _asyncQueue.doUnderLock([](auto &_queue) {
                auto queue = std::move(_queue);
                _queue.clear();
                return queue;
            });

            for (auto &p : asyncQueue) {
                p->promise.setValue(std::move(p->request));
            }
        }

        void runAllAsyncAppendEntries() {
            while (hasPendingAppendEntries()) {
                runAsyncAppendEntries();
            }
        }

        auto getCommitIndex() const noexcept -> log_index override {
            return _follower->getCommitIndex();
        }

        auto copyInMemoryLog() const -> replicated_log::inmemory_log override {
            return _follower->copyInMemoryLog();
        }

        using WaitForAsyncPromise = futures::Promise<replicated_log::append_entries_request>;

        struct AsyncRequest {
            explicit AsyncRequest(replicated_log::append_entries_request request) : request(std::move(request)) {
            }
            replicated_log::append_entries_request request;
            WaitForAsyncPromise promise;
        };
        [[nodiscard]] auto pendingAppendEntries() const -> std::deque<std::shared_ptr<AsyncRequest>> {
            return _asyncQueue.copy();
        }
        [[nodiscard]] auto hasPendingAppendEntries() const -> bool {
            return _asyncQueue.doUnderLock([](auto const &queue) { return !queue.empty(); });
        }

        auto getParticipantId() const noexcept -> ParticipantId const & override {
            return _follower->getParticipantId();
        }

        auto getStatus() const -> replicated_log::log_status override {
            return _follower->getStatus();
        }

        auto getQuickStatus() const -> replicated_log::quick_log_status override {
            return _follower->getQuickStatus();
        }

        [[nodiscard]] auto
            resign() && -> std::tuple<std::unique_ptr<replicated_log::log_core>, deferred_action> override {
            return std::move(*_follower).resign();
        }

        auto waitFor(log_index index) -> WaitForFuture override {
            return _follower->waitFor(index);
        }

        auto waitForIterator(log_index index) -> WaitForIteratorFuture override {
            return _follower->waitForIterator(index);
        }
        auto waitForResign() -> futures::Future<futures::Unit> override {
            return _follower->waitForResign();
        }
        auto release(log_index doneWithIdx) -> Result override {
            return _follower->release(doneWithIdx);
        }

    private:
        Guarded<std::deque<std::shared_ptr<AsyncRequest>>> _asyncQueue;
        std::shared_ptr<replicated_log::log_follower> _follower;
    };

    struct test_replicated_log : replicated_log::replicated_log_t {
        using replicated_log_t::become_leader;
        using replicated_log_t::replicated_log_t;
        auto become_follower(ParticipantId const &id, log_term term, ParticipantId leaderId)
            -> std::shared_ptr<DelayedFollowerLog>;

        auto become_leader(ParticipantId const &id, log_term term,
                           std::vector<std::shared_ptr<replicated_log::abstract_follower>> const &,
                           std::size_t writeConcern, bool waitForSync = false,
                           std::shared_ptr<cluster::IFailureOracle> failureOracle = nullptr)
            -> std::shared_ptr<replicated_log::log_leader>;
    };
}    // namespace nil::dbms::replication::test
