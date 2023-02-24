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

#include <nil/dbms/cluster/failure_oracle.hpp>
#include <nil/dbms/replication/replicated_log/agency_log_specification.hpp>
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

#include "test_helper.hpp"

#include <nil/dbms/replication/replicated_log/log_core.hpp>
#include <nil/dbms/replication/replicated_log/log_leader.hpp>
#include <nil/dbms/replication/replicated_log/replicated_log.hpp>
#include <nil/dbms/replication/replicated_log/types.hpp>

#include <utility>

#include <deque>
#include <memory>
#include <utility>

#include "../mocks/fake_replicated_log.hpp"
#include "../mocks/persisted_log.hpp"
#include "../mocks/fake_failure_oracle.hpp"
#include "../mocks/replicated_log_metrics_mock.hpp"


namespace nil::dbms::replication::test {

    using namespace replication::replicated_log;

    template<typename I>
    struct SimpleIterator : persisted_log_iterator {
        SimpleIterator(I begin, I end) : current(begin), end(end) {
        }
        ~SimpleIterator() override = default;

        auto next() -> std::optional<persisting_log_entry> override {
            if (current == end) {
                return std::nullopt;
            }

            return *(current++);
        }

        I current, end;
    };
    template<typename C, typename Iter = typename C::const_iterator>
    auto make_iterator(C const &c) -> std::shared_ptr<SimpleIterator<Iter>> {
        return std::make_shared<SimpleIterator<Iter>>(c.begin(), c.end());
    }

    struct replicated_log_test {
        template<typename MockLogT = mock_log>
        auto makeLogCore(LogId id) -> std::unique_ptr<log_core> {
            auto persisted = makePersistedLog<MockLogT>(id);
            return std::make_unique<log_core>(persisted);
        }

        template<typename MockLogT = mock_log>
        auto makeLogCore(global_log_identifier gid) -> std::unique_ptr<log_core> {
            auto persisted = makePersistedLog<MockLogT>(std::move(gid));
            return std::make_unique<log_core>(persisted);
        }

        template<typename MockLogT = mock_log>
        auto getPersistedLogById(LogId id) -> std::shared_ptr<MockLogT> {
            return std::dynamic_pointer_cast<MockLogT>(_persistedLogs.at(id));
        }

        template<typename MockLogT = mock_log>
        auto makePersistedLog(LogId id) -> std::shared_ptr<MockLogT> {
            auto persisted = std::make_shared<MockLogT>(id);
            _persistedLogs[id] = persisted;
            return persisted;
        }

        template<typename MockLogT = mock_log>
        auto makePersistedLog(global_log_identifier gid) -> std::shared_ptr<MockLogT> {
            auto persisted = std::make_shared<MockLogT>(gid);
            _persistedLogs[gid.id] = persisted;
            return persisted;
        }

        auto makeDelayedPersistedLog(LogId id) {
            return makePersistedLog<DelayedMockLog>(id);
        }

        template<typename MockLogT = mock_log>
        auto makeReplicatedLog(LogId id) -> std::shared_ptr<test_replicated_log> {
            auto core = makeLogCore<MockLogT>(id);
            return std::make_shared<test_replicated_log>(std::move(core), _logMetricsMock, _optionsMock,
                                                         logger_context(Logger::REPLICATION2));
        }

        template<typename MockLogT = mock_log>
        auto makeReplicatedLog(global_log_identifier gid) -> std::shared_ptr<test_replicated_log> {
            auto core = makeLogCore<MockLogT>(std::move(gid));
            return std::make_shared<test_replicated_log>(std::move(core), _logMetricsMock, _optionsMock,
                                                         logger_context(Logger::REPLICATION2));
        }

        auto makeReplicatedLogWithAsyncMockLog(LogId id) -> std::shared_ptr<test_replicated_log> {
            auto persisted = std::make_shared<AsyncMockLog>(id);
            _persistedLogs[id] = persisted;
            auto core = std::make_unique<log_core>(persisted);
            return std::make_shared<test_replicated_log>(std::move(core), _logMetricsMock, _optionsMock,
                                                         logger_context(Logger::REPLICATION2));
        }

        auto defaultLogger() {
            return logger_context(Logger::REPLICATION2);
        }

        auto createLeaderWithDefaultFlags(ParticipantId id, log_term term, std::unique_ptr<log_core> logCore,
                                          std::vector<std::shared_ptr<abstract_follower>> const &follower,
                                          std::size_t effectiveWriteConcern, bool waitForSync = false,
                                          std::shared_ptr<cluster::IFailureOracle> failureOracle = nullptr)
            -> std::shared_ptr<log_leader> {
            auto config = agency::log_plan_config {effectiveWriteConcern, waitForSync};
            auto participants = std::unordered_map<ParticipantId, participant_flags> {{id, {}}};
            for (auto const &participant : follower) {
                participants.emplace(participant->getParticipantId(), participant_flags {});
            }
            auto participantsConfig = std::make_shared<agency::participants_config>(agency::participants_config {
                .generation = 1, .participants = std::move(participants), .config = config});

            if (!failureOracle) {
                failureOracle = std::make_shared<fake_failure_oracle>();
            }

            return log_leader::construct(config, std::move(logCore), {follower}, std::move(participantsConfig), id,
                                         term, defaultLogger(), _logMetricsMock, _optionsMock, failureOracle);
        }

        auto stopAsyncMockLogs() -> void {
            for (auto const &it : _persistedLogs) {
                if (auto log = std::dynamic_pointer_cast<AsyncMockLog>(it.second); log != nullptr) {
                    log->stop();
                }
            }
        }

        std::unordered_map<LogId, std::shared_ptr<mock_log>> _persistedLogs;
          std::shared_ptr<replicated_log_metrics_mock> _logMetricsMock = std::make_shared<replicated_log_metrics_mock>();
        std::shared_ptr<replicated_log_global_settings> _optionsMock =
            std::make_shared<replicated_log_global_settings>();
    };

}    // namespace nil::dbms::replication::test
