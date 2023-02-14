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

#include <nil/replication_sdk/replicated_log/algorithms.hpp>

#include "basics/exceptions.h"
#include "basics/string_utils.h"
#include "basics/application_exit.h"
#include "logger/LogMacros.h"
#include "random/RandomGenerator.h"

#include <algorithm>
#include <random>
#include <tuple>
#include <type_traits>

using namespace nil::dbms;
using namespace nil::dbms::replication_sdk;
using namespace nil::dbms::replication_sdk::agency;
using namespace nil::dbms::replication_sdk::algorithms;
using namespace nil::dbms::replication_sdk::replicated_log;

auto algorithms::to_string(conflict_reason r) noexcept -> std::string_view {
    switch (r) {
        case conflict_reason::LOG_ENTRY_AFTER_END:
            return "prev log is located after the last log entry";
        case conflict_reason::LOG_ENTRY_BEFORE_BEGIN:
            return "prev log is located before the first entry";
        case conflict_reason::LOG_EMPTY:
            return "the replicated log is empty";
        case conflict_reason::LOG_ENTRY_NO_MATCH:
            return "term mismatch";
    }
    LOG_TOPIC("03e11", FATAL, nil::dbms::Logger::REPLICATION2)
        << "Invalid ConflictReason " << static_cast<std::underlying_type_t<decltype(r)>>(r);
    FATAL_ERROR_ABORT();
}

auto algorithms::detectConflict(replicated_log::in_memory_log const &log, term_index_pair prevLog) noexcept
    -> std::optional<std::pair<conflict_reason, term_index_pair>> {
    /*
     * There are three situations to handle here:
     *  - We don't have that log entry
     *    - It is behind our last entry
     *    - It is before our first entry
     *  - The term does not match.
     */
    auto entry = log.get_entry_by_index(prevLog.index);
    if (entry.has_value()) {
        // check if the term matches
        if (entry->entry().logTerm() != prevLog.term) {
            auto conflict = std::invoke([&] {
                if (auto idx = log.get_first_index_of_term(entry->entry().logTerm()); idx.has_value()) {
                    return term_index_pair {entry->entry().logTerm(), *idx};
                }
                return term_index_pair {};
            });

            return std::make_pair(conflict_reason::LOG_ENTRY_NO_MATCH, conflict);
        } else {
            // No conflict
            return std::nullopt;
        }
    } else {
        auto lastEntry = log.get_last_entry();
        if (!lastEntry.has_value()) {
            // The log is empty, reset to (0, 0)
            return std::make_pair(conflict_reason::LOG_EMPTY, term_index_pair {});
        } else if (prevLog.index > lastEntry->entry().logIndex()) {
            // the given entry is too far ahead
            return std::make_pair(conflict_reason::LOG_ENTRY_AFTER_END,
                                  term_index_pair {lastEntry->entry().logTerm(), lastEntry->entry().logIndex() + 1});
        } else {
            TRI_ASSERT(prevLog.index < lastEntry->entry().logIndex());
            TRI_ASSERT(prevLog.index < log.get_first_entry()->entry().logIndex());
            // the given index too old, reset to (0, 0)
            return std::make_pair(conflict_reason::LOG_ENTRY_BEFORE_BEGIN, term_index_pair {});
        }
    }
}

auto algorithms::update_replicated_log(log_action_context &ctx, ServerID const &myServerId, RebootId myRebootId,
                                     LogId logId, agency::log_plan_specification const *spec,
                                     std::shared_ptr<cluster::IFailureOracle const> failureOracle) noexcept
    -> futures::Future<nil::dbms::Result> {
    auto result = basics::catchToResultT([&]() -> futures::Future<nil::dbms::Result> {
        if (spec == nullptr) {
            return ctx.dropReplicatedLog(logId);
        }

        TRI_ASSERT(logId == spec->id);
        TRI_ASSERT(spec->currentTerm.has_value());
        auto &plannedLeader = spec->currentTerm->leader;
        auto log = ctx.ensureReplicatedLog(logId);

        if (log->get_participant()->getTerm() == spec->currentTerm->term) {
            // something has changed in the term volatile configuration
            auto leader = log->get_leader();
            TRI_ASSERT(leader != nullptr);
            // Provide the leader with a way to build a follower
            auto const buildFollower = [&ctx, &logId](ParticipantId const &participantId) {
                return ctx.buildAbstractFollowerImpl(logId, participantId);
            };
            auto index = leader->updateParticipantsConfig(
                std::make_shared<participants_config const>(spec->participantsConfig), buildFollower);
            return leader->waitFor(index).thenValue(
                [](auto &&quorum) -> Result { return Result {TRI_ERROR_NO_ERROR}; });
        } else if (plannedLeader.has_value() && plannedLeader->serverId == myServerId &&
                   plannedLeader->rebootId == myRebootId) {
            auto followers = std::vector<std::shared_ptr<replication_sdk::replicated_log::abstract_follower>> {};
            for (auto const &[participant, data] : spec->participantsConfig.participants) {
                if (participant != myServerId) {
                    followers.emplace_back(ctx.buildAbstractFollowerImpl(logId, participant));
                }
            }

            TRI_ASSERT(spec->participantsConfig.generation > 0);
            auto newLeader = log->become_leader(
                spec->participantsConfig.config, myServerId, spec->currentTerm->term, followers,
                std::make_shared<participants_config>(spec->participantsConfig), std::move(failureOracle));
            newLeader->triggerAsyncReplication();    // TODO move this call into
                                                     // becomeLeader?
            return newLeader->waitForLeadership().thenValue(
                [](auto &&quorum) -> Result { return Result {TRI_ERROR_NO_ERROR}; });
        } else {
            auto leaderString = std::optional<ParticipantId> {};
            if (spec->currentTerm->leader) {
                leaderString = spec->currentTerm->leader->serverId;
            }

            std::ignore = log->become_follower(myServerId, spec->currentTerm->term, leaderString);
        }

        return futures::Future<nil::dbms::Result> {std::in_place};
    });

    if (result.ok()) {
        return *std::move(result);
    } else {
        return futures::Future<nil::dbms::Result> {std::in_place, result.result()};
    }
}

auto algorithms::operator<<(std::ostream &os, participant_state const &p) noexcept -> std::ostream & {
    os << '{' << p.id << ':' << p.lastAckedEntry << ", ";
    os << "failed = " << std::boolalpha << p.failed;
    os << ", flags = " << p.flags;
    os << '}';
    return os;
}

auto participant_state::isAllowedInQuorum() const noexcept -> bool {
    return flags.allowedInQuorum;
};

auto participant_state::isForced() const noexcept -> bool {
    return flags.forced;
};

auto participant_state::isFailed() const noexcept -> bool {
    return failed;
};

auto participant_state::lastTerm() const noexcept -> log_term {
    return lastAckedEntry.term;
}

auto participant_state::lastIndex() const noexcept -> log_index {
    return lastAckedEntry.index;
}

auto algorithms::calculate_commit_index(std::vector<participant_state> const &participants,
                                        size_t effectiveWriteConcern, log_index currentCommitIndex,
                                        term_index_pair lastTermIndex)
    -> std::tuple<log_index, commit_fail_reason, std::vector<ParticipantId>> {
    // We keep a vector of eligible participants.
    // To be eligible, a participant
    //  - must not be excluded, and
    //  - must be in the same term as the leader.
    // This is because we must not include an excluded server in any quorum, and
    // must never commit log entries from older terms.
    auto eligible = std::vector<participant_state> {};
    eligible.reserve(participants.size());
    std::copy_if(std::begin(participants), std::end(participants), std::back_inserter(eligible),
                 [&](auto const &p) { return p.isAllowedInQuorum() && p.lastTerm() == lastTermIndex.term; });

    if (effectiveWriteConcern > participants.size() && participants.size() == eligible.size()) {
        // With WC greater than the number of participants, we cannot commit
        // anything, even if all participants are eligible.
        TRI_ASSERT(!participants.empty());
        return {currentCommitIndex,
                commit_fail_reason::withFewerParticipantsThanWriteConcern({
                    .effectiveWriteConcern = effectiveWriteConcern,
                    .numParticipants = participants.size(),
                }),
                {}};
    }

    auto const spearhead = lastTermIndex.index;

    // The minimal commit index caused by forced participants.
    // If there are no forced participants, this component is just
    // the spearhead (the furthest we could commit to).
    auto minForcedCommitIndex = spearhead;
    auto minForcedParticipantId = std::optional<ParticipantId> {};
    for (auto const &pt : participants) {
        if (pt.isForced()) {
            if (pt.lastTerm() != lastTermIndex.term) {
                // A forced participant has entries from a previous term. We can't use
                // this participant, hence it becomes impossible to commit anything.
                return {currentCommitIndex, commit_fail_reason::with_forced_participant_not_in_quorum(pt.id), {}};
            }
            if (pt.lastIndex() < minForcedCommitIndex) {
                minForcedCommitIndex = pt.lastIndex();
                minForcedParticipantId = pt.id;
            }
        }
    }

    // While effectiveWriteConcern == 0 is silly we still allow it.
    if (effectiveWriteConcern == 0) {
        return {minForcedCommitIndex, commit_fail_reason::with_nothing_to_commit(), {}};
    }

    if (effectiveWriteConcern <= eligible.size()) {
        auto nth = std::begin(eligible);

        TRI_ASSERT(effectiveWriteConcern > 0);
        std::advance(nth, effectiveWriteConcern - 1);

        std::nth_element(std::begin(eligible), nth, std::end(eligible),
                         [](auto &left, auto &right) { return left.lastIndex() > right.lastIndex(); });
        auto const minNonExcludedCommitIndex = nth->lastIndex();

        auto commitIndex = std::min(minForcedCommitIndex, minNonExcludedCommitIndex);

        auto quorum = std::vector<ParticipantId> {};
        quorum.reserve(effectiveWriteConcern);

        std::transform(std::begin(eligible), std::next(nth), std::back_inserter(quorum), [](auto &p) { return p.id; });

        if (spearhead == commitIndex) {
            // The quorum has been reached and any uncommitted entries can now be
            // committed.
            return {commitIndex, commit_fail_reason::with_nothing_to_commit(), std::move(quorum)};
        } else if (minForcedCommitIndex < minNonExcludedCommitIndex) {
            // The forced participant didn't make the quorum because its index
            // is too low. Return its index, but report that it is dragging down the
            // commit index.
            TRI_ASSERT(minForcedParticipantId.has_value());
            return {
                commitIndex,
                    commit_fail_reason::with_forced_participant_not_in_quorum(minForcedParticipantId.value()), {}};
        } else {
            // We commit as far away as we can get, but report all participants who
            // can't be part of a quorum for the spearhead.
            auto who = commit_fail_reason::quorum_size_not_reached::who_type();
            for (auto const &participant : participants) {
                if (participant.lastAckedEntry < lastTermIndex || !participant.isAllowedInQuorum()) {
                    who.try_emplace(participant.id,
                                    commit_fail_reason::quorum_size_not_reached::participant_info {
                                        .isFailed = participant.isFailed(),
                                        .isAllowedInQuorum = participant.isAllowedInQuorum(),
                                        .lastAcknowledged = participant.lastAckedEntry,
                                    });
                }
            }
            return {commitIndex, commit_fail_reason::with_quorum_size_not_reached(std::move(who), lastTermIndex),
                    std::move(quorum)};
        }
    }

    // This happens when too many servers are either excluded or failed;
    // this certainly means we could not reach a quorum;
    // indexes cannot be empty because this particular case would've been handled
    // above by comparing actualWriteConcern to 0;
    commit_fail_reason::non_eligible_server_required_for_quorum::CandidateMap candidates;
    for (auto const &p : participants) {
        if (!p.isAllowedInQuorum()) {
            candidates.emplace(p.id, commit_fail_reason::non_eligible_server_required_for_quorum::kNotAllowedInQuorum);
        } else if (p.lastTerm() != lastTermIndex.term) {
            candidates.emplace(p.id, commit_fail_reason::non_eligible_server_required_for_quorum::kWrongTerm);
        }
    }

    TRI_ASSERT(!participants.empty());
    return {currentCommitIndex, commit_fail_reason::withNonEligibleServerRequiredForQuorum(std::move(candidates)), {}};
}
