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

#include <memory>
#include <mutex>

#include <basics/result.h>
#include <basics/unshackled_mutex.h>
#include <futures/Future.h>

#include <nil/replication_sdk/replicated_log/log_common.hpp>
#include <nil/replication_sdk/replicated_log/log_entries.hpp>

namespace nil::dbms::replication_sdk::replicated_log {
    struct persisted_log;

    /**
     * @brief The persistent core of a replicated log. There must only ever by one
     * instance of LogCore for a particular physical log. It is always held by the
     * single active ILogParticipant instance, which in turn lives in the
     * ReplicatedLog instance for this particular log. That is, usually by either a
     * LogLeader, or a LogFollower. If the term changes (and with that
     * leader/followers and/or configuration like writeConcern), a new participant
     * instance is created, and the core moved from the old to the new instance. If
     * the server is currently neither a leader nor follower for the log, e.g.
     * during startup, the LogCore is held by a LogUnconfiguredParticipant instance.
     */
    struct alignas(64) log_core {
        explicit log_core(std::shared_ptr<persisted_log> persistedLog);

        // There must only be one LogCore per physical log
        log_core() = delete;
        log_core(log_core const &) = delete;
        log_core(log_core &&) = delete;
        auto operator=(log_core const &) -> log_core & = delete;
        auto operator=(log_core &&) -> log_core & = delete;

        auto insert_async(std::unique_ptr<persisted_log_iterator> iter, bool waitForSync) -> futures::Future<Result>;
        auto insert(persisted_log_iterator &iter, bool waitForSync) -> Result;
        [[nodiscard]] auto read(log_index first) const -> std::unique_ptr<persisted_log_iterator>;
        auto remove_back(log_index first) -> Result;
        auto remove_front(log_index stop) -> futures::Future<Result>;

        auto release_persisted_log() && -> std::shared_ptr<persisted_log>;

        auto log_id() const noexcept -> LogId;
        auto gid() const noexcept -> global_log_identifier const &;

    private:
        std::shared_ptr<persisted_log> _persistedLog;
        mutable basics::UnshackledMutex _operationMutex;
    };

}    // namespace nil::dbms::replication_sdk::replicated_log
