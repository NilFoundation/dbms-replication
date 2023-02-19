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

#include <variant>
#include <unordered_map>
#include <set>

#include <nil/dbms/replication/cluster/cluster_types.hpp>
#include "log_common.hpp"
#include "replicated_log.hpp"
#include "agency_log_specification.hpp"

namespace nil::dbms::cluster {
    struct IFailureOracle;
}

namespace nil::dbms::replication::algorithms {

    struct participant_record {
        RebootId rebootId;
        bool isHealthy;

        participant_record(RebootId rebootId, bool isHealthy) : rebootId(rebootId), isHealthy(isHealthy) {
        }
    };

    using ParticipantInfo = std::unordered_map<ParticipantId, participant_record>;

    enum class conflict_reason {
        LOG_ENTRY_AFTER_END,
        LOG_ENTRY_BEFORE_BEGIN,
        LOG_EMPTY,
        LOG_ENTRY_NO_MATCH,
    };

    auto to_string(conflict_reason r) noexcept -> std::string_view;

    auto detectConflict(replicated_log::in_memory_log const &log, term_index_pair prevLog) noexcept
        -> std::optional<std::pair<conflict_reason, term_index_pair>>;

    struct log_action_context {
        virtual ~log_action_context() = default;
        virtual auto dropReplicatedLog(LogId) -> Result = 0;
        virtual auto ensureReplicatedLog(LogId) -> std::shared_ptr<replicated_log::replicated_log_t> = 0;
        virtual auto buildAbstractFollowerImpl(LogId, ParticipantId)
            -> std::shared_ptr<replication::replicated_log::abstract_follower> = 0;
    };

    auto update_replicated_log(log_action_context &ctx, ServerID const &myServerId, RebootId myRebootId, LogId logId,
                               agency::log_plan_specification const *spec,
                               std::shared_ptr<cluster::IFailureOracle const> failureOracle) noexcept
        -> futures::Future<nil::dbms::Result>;

    struct participant_state {
        term_index_pair lastAckedEntry;
        ParticipantId id;
        bool failed = false;
        participant_flags flags {};

        [[nodiscard]] auto isAllowedInQuorum() const noexcept -> bool;
        [[nodiscard]] auto isForced() const noexcept -> bool;
        [[nodiscard]] auto isFailed() const noexcept -> bool;

        [[nodiscard]] auto lastTerm() const noexcept -> log_term;
        [[nodiscard]] auto lastIndex() const noexcept -> log_index;

        bool operator==(const participant_state &rhs) const {
            return lastAckedEntry == rhs.lastAckedEntry && id == rhs.id && failed == rhs.failed && flags == rhs.flags;
        }
        bool operator!=(const participant_state &rhs) const {
            return !(rhs == *this);
        }

        bool operator<(const participant_state &rhs) const {
            if (lastAckedEntry < rhs.lastAckedEntry)
                return true;
            if (rhs.lastAckedEntry < lastAckedEntry)
                return false;
            if (id < rhs.id)
                return true;
            if (rhs.id < id)
                return false;
            if (failed < rhs.failed)
                return true;
            if (rhs.failed < failed)
                return false;
            return flags < rhs.flags;
        }
        bool operator>(const participant_state &rhs) const {
            return rhs < *this;
        }
        bool operator<=(const participant_state &rhs) const {
            return !(rhs < *this);
        }
        bool operator>=(const participant_state &rhs) const {
            return !(*this < rhs);
        }

        friend auto operator<<(std::ostream &os, participant_state const &p) noexcept -> std::ostream &;
    };

    auto operator<<(std::ostream &os, participant_state const &p) noexcept -> std::ostream &;

    auto calculate_commit_index(std::vector<participant_state> const &participants,
                                size_t effectiveWriteConcern,
                                log_index currentCommitIndex,
                                term_index_pair lastTermIndex)
        -> std::tuple<log_index, replicated_log::commit_fail_reason, std::vector<ParticipantId>>;

}    // namespace nil::dbms::replication::algorithms
