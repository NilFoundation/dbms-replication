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

#include <nil/dbms/cluster/cluster_types.hpp>
#include <nil/dbms/replication/log/log_common.hpp>
#include "log.hpp"
#include <nil/dbms/replication/log/agency_log_specification.hpp>

namespace nil {
    namespace dbms {
        namespace cluster {
            struct IFailureOracle;
        }
    }
}

namespace nil {
    namespace dbms {
        namespace replication {
            namespace algorithms {

                struct participant_record {
                    RebootId rebootId;
                    bool isHealthy;

                    participant_record(RebootId rebootId, bool isHealthy) : rebootId(rebootId), isHealthy(isHealthy) {
                    }
                };

                using ParticipantInfo = std::unordered_map<ParticipantId, participant_record>;

                enum class ConflictReason {
                    LOG_ENTRY_AFTER_END,
                    LOG_ENTRY_BEFORE_BEGIN,
                    LOG_EMPTY,
                    LOG_ENTRY_NO_MATCH,
                };

                auto to_string(ConflictReason r)

                noexcept ->
                std::string_view;

                auto detectConflict(log::in_memory_log const &log, term_index_pair prevLog)

                noexcept
                -> std::optional <std::pair<ConflictReason, term_index_pair>>;

                struct log_action_context {
                    virtual ~log_action_context() = default;

                    virtual auto drop_log(log_id) -> Result = 0;

                    virtual auto ensureReplicatedLog(log_id) -> std::shared_ptr <log::ReplicatedLog> = 0;

                    virtual auto buildAbstractFollowerImpl(log_id, ParticipantId)
                    -> std::shared_ptr <replication::log::AbstractFollower> = 0;
                };

                auto update_log(log_action_context &ctx, ServerID const &myServerId, RebootId myRebootId, log_id log_id,
                                agency::log_plan_specification const *spec,
                                std::shared_ptr<cluster::IFailureOracle const> failureOracle)

                noexcept
                ->
                futures::Future<nil::dbms::Result>;

                struct participant_state {
                    term_index_pair lastAckedEntry;
                    ParticipantId id;
                    bool failed = false;
                    ParticipantFlags flags{};

                    [[nodiscard]] auto isAllowedInQuorum() const noexcept -> bool;
                    [[nodiscard]] auto isForced() const noexcept -> bool;
                    [[nodiscard]] auto isFailed() const noexcept -> bool;

                    [[nodiscard]] auto lastTerm() const noexcept -> log_term;
                    [[nodiscard]] auto lastIndex() const noexcept -> log_index;

                    bool operator==(const participant_state &rhs) const {
                        return lastAckedEntry == rhs.lastAckedEntry && id == rhs.id && failed == rhs.failed &&
                               flags == rhs.flags;
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

                    friend auto operator<<(std::ostream &os, participant_state const &p)

                    noexcept -> std::ostream &;
                };

                auto operator<<(std::ostream &os, participant_state const &p)

                noexcept -> std::ostream &;

                auto calculateCommitIndex(std::vector < participant_state >
                const &participants,
                size_t effectiveWriteConcern,
                        log_index
                currentCommitIndex,
                term_index_pair lastTermIndex
                )
                -> std::tuple <log_index, log::CommitFailReason, std::vector<ParticipantId>>;

            }
        }
    }
}    // namespace nil::dbms::replication::algorithms
