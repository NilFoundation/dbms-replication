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

#include <nil/dbms/replication/deferred_execution.hpp>
#include "log_common.hpp"
#include "log_entries.hpp"
#include "types.hpp"

#include <futures/Future.h>
#include <futures/Promise.h>

#include <map>
#include <memory>

namespace nil::dbms {
    class Result;
    struct logger_context;
}    // namespace nil::dbms

namespace nil::dbms::replication::replicated_log {

    struct log_core;
    struct log_status;
    struct quick_log_status;
    struct inmemory_log;

    struct wait_for_result {
        /// @brief contains the _current_ commit index. (Not the index waited for)
        log_index currentCommitIndex;
        /// @brief Quorum information
        std::shared_ptr<quorum_data const> quorum;

        wait_for_result(log_index index, std::shared_ptr<quorum_data const> quorum);
        wait_for_result() = default;
        explicit wait_for_result(velocypack::Slice);

        void toVelocyPack(velocypack::Builder &) const;
    };

    /**
     * @brief Interface for a log participant: That is, usually either a leader or a
     * follower (LogLeader and LogFollower). Can also be a
     * LogUnconfiguredParticipant, e.g. during startup. The most prominent thing
     * this interface provides is that each instance is responsible for a singular
     * LogCore, which can be moved out with resign().
     */
    struct ilog_participant {
        [[nodiscard]] virtual auto getStatus() const -> log_status = 0;
        [[nodiscard]] virtual auto getQuickStatus() const -> quick_log_status = 0;
        virtual ~ilog_participant() = default;
        [[nodiscard]] virtual auto resign() && -> std::tuple<std::unique_ptr<log_core>, deferred_action> = 0;

        using WaitForPromise = futures::Promise<wait_for_result>;
        using WaitForFuture = futures::Future<wait_for_result>;
        using WaitForIteratorFuture = futures::Future<std::unique_ptr<LogRangeIterator>>;
        using WaitForQueue = std::multimap<log_index, WaitForPromise>;

        [[nodiscard]] virtual auto waitFor(log_index index) -> WaitForFuture = 0;
        [[nodiscard]] virtual auto waitForIterator(log_index index) -> WaitForIteratorFuture = 0;
        [[nodiscard]] virtual auto waitForResign() -> futures::Future<futures::Unit> = 0;
        [[nodiscard]] virtual auto getTerm() const noexcept -> std::optional<log_term>;
        [[nodiscard]] virtual auto getCommitIndex() const noexcept -> log_index = 0;

        [[nodiscard]] virtual auto copyInMemoryLog() const -> inmemory_log = 0;
        [[nodiscard]] virtual auto release(log_index doneWithIdx) -> Result = 0;
    };

    /**
     * Interface describing a LogFollower API. Components should use this interface
     * if they want to refer to a LogFollower instance.
     */
    struct ilog_follower : ilog_participant, abstract_follower {
        [[nodiscard]] virtual auto waitForLeaderAcked() -> WaitForFuture = 0;
        [[nodiscard]] virtual auto getLeader() const noexcept -> std::optional<ParticipantId> const & = 0;
    };

    /**
     * Interfaces describe a LogLeader API. Components should use this interface
     * if they want to refer to a LogLeader instance.
     */
    struct ilog_leader : ilog_participant {
        virtual auto insert(log_payload payload, bool waitForSync) -> log_index = 0;

        struct DoNotTriggerAsyncReplication { };
        constexpr static auto doNotTriggerAsyncReplication = DoNotTriggerAsyncReplication {};
        virtual auto insert(log_payload payload, bool waitForSync, DoNotTriggerAsyncReplication) -> log_index = 0;
        virtual void triggerAsyncReplication() = 0;

        [[nodiscard]] virtual auto isLeadershipEstablished() const noexcept -> bool = 0;
        [[nodiscard]] virtual auto waitForLeadership() -> WaitForFuture = 0;
    };

}    // namespace nil::dbms::replication::replicated_log
