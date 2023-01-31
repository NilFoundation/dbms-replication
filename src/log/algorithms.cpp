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

#include <nil/dbms/replication/log/algorithms.hpp>

#include "basics/exceptions.h"
#include "basics/string_utils.h"
#include "basics/application_exit.h"
#include <nil/dbms/cluster/failure_oracle.hpp>
#include "logger/LogMacros.h"
#include "random/RandomGenerator.h"

#include <algorithm>
#include <random>
#include <tuple>
#include <type_traits>

using namespace nil::dbms;
using namespace nil::dbms::replication;
using namespace nil::dbms::replication::agency;
using namespace nil::dbms::replication::algorithms;
using namespace nil::dbms::replication::log;

auto algorithms::to_string(ConflictReason r) noexcept -> std::string_view {
    switch (r) {
        case ConflictReason::LOG_ENTRY_AFTER_END:
            return "prev log is located after the last log entry";
        case ConflictReason::LOG_ENTRY_BEFORE_BEGIN:
            return "prev log is located before the first entry";
        case ConflictReason::LOG_EMPTY:
            return "the replicated log is empty";
        case ConflictReason::LOG_ENTRY_NO_MATCH:
            return "term mismatch";
    }
    LOG_TOPIC("03e11", FATAL, nil::dbms::Logger::replication)
        << "Invalid ConflictReason " << static_cast<std::underlying_type_t<decltype(r)>>(r);
    FATAL_ERROR_ABORT();
}

auto algorithms::detectConflict(log::InMemoryLog const &log, term_index_pair prevLog) noexcept
    -> std::optional<std::pair<ConflictReason, term_index_pair>> {
    /*
     * There are three situations to handle here:
     *  - We don't have that log entry
     *    - It is behind our last entry
     *    - It is before our first entry
     *  - The term does not match.
     */
    auto entry = log.getEntryByIndex(prevLog.index);
    if (entry.has_value()) {
        // check if the term matches
        if (entry->entry().log_term() != prevLog.term) {
            auto conflict = std::invoke([&] {
                if (auto idx = log.getFirstIndexOfTerm(entry->entry().log_term()); idx.has_value()) {
                    return term_index_pair {entry->entry().log_term(), *idx};
                }
                return term_index_pair {};
            });

            return std::make_pair(ConflictReason::LOG_ENTRY_NO_MATCH, conflict);
        } else {
            // No conflict
            return std::nullopt;
        }
    } else {
        auto lastEntry = log.getLastEntry();
        if (!lastEntry.has_value()) {
            // The log is empty, reset to (0, 0)
            return std::make_pair(ConflictReason::LOG_EMPTY, term_index_pair {});
        } else if (prevLog.index > lastEntry->entry().log_index()) {
            // the given entry is too far ahead
            return std::make_pair(ConflictReason::LOG_ENTRY_AFTER_END,
                                  term_index_pair {lastEntry->entry().log_term(), lastEntry->entry().log_index() + 1});
        } else {
            TRI_ASSERT(prevLog.index < lastEntry->entry().log_index());
            TRI_ASSERT(prevLog.index < log.getFirstEntry()->entry().log_index());
            // the given index too old, reset to (0, 0)
            return std::make_pair(ConflictReason::LOG_ENTRY_BEFORE_BEGIN, term_index_pair {});
        }
    }
}

auto algorithms::update_log(log_action_context &ctx, ServerID const &myServerId, RebootId myRebootId,
                                     log_id log_id, agency::LogPlanSpecification const *spec,
                                     std::shared_ptr<cluster::IFailureOracle const> failureOracle) noexcept
    -> futures::Future<nil::dbms::Result> {
    auto result = basics::catchToResultT([&]() -> futures::Future<nil::dbms::Result> {
        if (spec == nullptr) {
            return ctx.drop_log(log_id);
        }

        TRI_ASSERT(log_id == spec->id);
        TRI_ASSERT(spec->currentTerm.has_value());
        auto &plannedLeader = spec->currentTerm->leader;
        auto log = ctx.ensureReplicatedLog(log_id);

        if (log->getParticipant()->getTerm() == spec->currentTerm->term) {
            // something has changed in the term volatile configuration
            auto leader = log->getLeader();
            TRI_ASSERT(leader != nullptr);
            // Provide the leader with a way to build a follower
            auto const buildFollower = [&ctx, &log_id](ParticipantId const &participantId) {
                return ctx.buildAbstractFollowerImpl(log_id, participantId);
            };
            auto index = leader->updateparticipants_config(
                std::make_shared<participants_config const>(spec->participants_config), buildFollower);
            return leader->waitFor(index).thenValue(
                [](auto &&quorum) -> Result { return Result {TRI_ERROR_NO_ERROR}; });
        } else if (plannedLeader.has_value() && plannedLeader->serverId == myServerId &&
                   plannedLeader->rebootId == myRebootId) {
            auto followers = std::vector<std::shared_ptr<replication::log::AbstractFollower>> {};
            for (auto const &[participant, data] : spec->participants_config.participants) {
                if (participant != myServerId) {
                    followers.emplace_back(ctx.buildAbstractFollowerImpl(log_id, participant));
                }
            }

            TRI_ASSERT(spec->participants_config.generation > 0);
            auto newLeader = log->becomeLeader(
                spec->participants_config.config, myServerId, spec->currentTerm->term, followers,
                std::make_shared<participants_config>(spec->participants_config), std::move(failureOracle));
            newLeader->triggerAsyncReplication();    // TODO move this call into
                                                     // becomeLeader?
            return newLeader->wait_for_leadership().thenValue(
                [](auto &&quorum) -> Result { return Result {TRI_ERROR_NO_ERROR}; });
        } else {
            auto leaderString = std::optional<ParticipantId> {};
            if (spec->currentTerm->leader) {
                leaderString = spec->currentTerm->leader->serverId;
            }

            std::ignore = log->becomeFollower(myServerId, spec->currentTerm->term, leaderString);
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

auto algorithms::calculateCommitIndex(std::vector<participant_state> const &participants,
                                      size_t const effectiveWriteConcern, log_index const currentCommitIndex,
                                      term_index_pair const lastTermIndex)
    -> std::tuple<log_index, CommitFailReason, std::vector<ParticipantId>> {
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
                CommitFailReason::withFewerParticipantsThanWriteConcern({
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
                return {currentCommitIndex, CommitFailReason::withForcedParticipantNotInQuorum(pt.id), {}};
            }
            if (pt.lastIndex() < minForcedCommitIndex) {
                minForcedCommitIndex = pt.lastIndex();
                minForcedParticipantId = pt.id;
            }
        }
    }

    // While effectiveWriteConcern == 0 is silly we still allow it.
    if (effectiveWriteConcern == 0) {
        return {minForcedCommitIndex, CommitFailReason::withNothingToCommit(), {}};
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
            return {commitIndex, CommitFailReason::withNothingToCommit(), std::move(quorum)};
        } else if (minForcedCommitIndex < minNonExcludedCommitIndex) {
            // The forced participant didn't make the quorum because its index
            // is too low. Return its index, but report that it is dragging down the
            // commit index.
            TRI_ASSERT(minForcedParticipantId.has_value());
            return {
                commitIndex, CommitFailReason::withForcedParticipantNotInQuorum(minForcedParticipantId.value()), {}};
        } else {
            // We commit as far away as we can get, but report all participants who
            // can't be part of a quorum for the spearhead.
            auto who = CommitFailReason::QuorumSizeNotReached::who_type();
            for (auto const &participant : participants) {
                if (participant.lastAckedEntry < lastTermIndex || !participant.isAllowedInQuorum()) {
                    who.try_emplace(participant.id,
                                    CommitFailReason::QuorumSizeNotReached::ParticipantInfo {
                                        .isFailed = participant.isFailed(),
                                        .isAllowedInQuorum = participant.isAllowedInQuorum(),
                                        .lastAcknowledged = participant.lastAckedEntry,
                                    });
                }
            }
            return {commitIndex, CommitFailReason::withQuorumSizeNotReached(std::move(who), lastTermIndex),
                    std::move(quorum)};
        }
    }

    // This happens when too many servers are either excluded or failed;
    // this certainly means we could not reach a quorum;
    // indexes cannot be empty because this particular case would've been handled
    // above by comparing actualWriteConcern to 0;
    CommitFailReason::NonEligibleServerRequiredForQuorum::CandidateMap candidates;
    for (auto const &p : participants) {
        if (!p.isAllowedInQuorum()) {
            candidates.emplace(p.id, CommitFailReason::NonEligibleServerRequiredForQuorum::kNotAllowedInQuorum);
        } else if (p.lastTerm() != lastTermIndex.term) {
            candidates.emplace(p.id, CommitFailReason::NonEligibleServerRequiredForQuorum::kWrongTerm);
        }
    }

    TRI_ASSERT(!participants.empty());
    return {currentCommitIndex, CommitFailReason::withNonEligibleServerRequiredForQuorum(std::move(candidates)), {}};
}
