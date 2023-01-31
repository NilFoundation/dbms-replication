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

#include <memory>

#include <nil/dbms/agency/agency_paths.hpp>
#include "basics/exceptions.h"
#include "basics/string_utils.h"
#include "basics/time_string.h"
#include <nil/dbms/cluster/cluster_types.hpp>
#include "inspection/vpack.h"
#include "random/RandomGenerator.h"
#include <nil/dbms/replication/log/agency_log_specification.hpp>
#include <nil/dbms/replication/log/agency_specification_inspectors.hpp>
#include <nil/dbms/replication/log/log_common.hpp>
#include <nil/dbms/replication/log/supervision.hpp>
#include <nil/dbms/replication/log/supervision_action.hpp>
#include <nil/dbms/replication/log/supervision_context.hpp>

#include "logger/LogMacros.h"

namespace paths = nil::dbms::cluster::paths::aliases;
using namespace nil::dbms::replication::agency;

namespace nil::dbms::replication::log {

    auto isConfigurationCommitted(Log const &log) -> bool {
        if (!log.plan) {
            return false;
        }
        auto const &plan = *log.plan;

        if (!log.current) {
            return false;
        }
        auto const &current = *log.current;

        return current.leader && current.leader->committedparticipants_config &&
               current.leader->committedparticipants_config->generation == plan.participants_config.generation;
    }

    auto hasCurrentTermWithLeader(Log const &log) -> bool {
        if (!log.plan) {
            return false;
        }
        auto const &plan = *log.plan;

        return plan.currentTerm and plan.currentTerm->leader;
    }

    // Leader has failed if it is marked as failed or its rebootId is
    // different from what is expected
    auto isLeaderFailed(log_plan_term_specification::Leader const &leader, ParticipantsHealth const &health) -> bool {
        // TODO: less obscure with fewer negations
        // TODO: write test first
        if (health.notIsFailed(leader.serverId) and health.validRebootId(leader.serverId, leader.rebootId)) {
            return false;
        } else {
            return true;
        }
    }

    // If the currentleader is not present in target, this means
    // that the user removed that leader (rather forcefully)
    //
    // This in turn means we have to gracefully remove the leader
    // from its position;
    //
    // To not end up in a state where we have a) no leader and b)
    // not even a way to elect a new one we want to replace the leader
    // with a new one (gracefully); this is as opposed to just
    // rip out the old leader and waiting for failover to occur
    //
    // TODO: should this have some kind of preference?
    //       consider the case where all participants are replaced; ideally
    //       leadership should be handed to a participant that is in target
    //       Yet, is there a case where it is necessary to hand leadership to
    //       an otherwise healthy participant that is not in target anymore?
    auto getParticipantsAcceptableAsLeaders(ParticipantId const &currentLeader,
                                            ParticipantsFlagsMap const &participants) -> std::vector<ParticipantId> {
        // A participant is acceptable if it is neither excluded nor
        // already the leader
        auto acceptableLeaderSet = std::vector<ParticipantId> {};
        for (auto const &[participant, flags] : participants) {
            if (participant != currentLeader and flags.allowedAsLeader) {
                acceptableLeaderSet.emplace_back(participant);
            }
        }

        return acceptableLeaderSet;
    }

    // Check whether Target contains an entry for a leader, which means
    // that the user would like a particular participant to be leader;

    auto computeReason(std::optional<log_current_local_state> const &maybeStatus, bool healthy, bool excluded,
                       log_term term) -> log_current_supervision_election::ErrorCode {
        if (!healthy) {
            return log_current_supervision_election::ErrorCode::SERVER_NOT_GOOD;
        } else if (excluded) {
            return log_current_supervision_election::ErrorCode::SERVER_EXCLUDED;
        } else if (!maybeStatus or term != maybeStatus->term) {
            return log_current_supervision_election::ErrorCode::TERM_NOT_CONFIRMED;
        } else {
            return log_current_supervision_election::ErrorCode::OK;
        }
    }

    auto runElectionCampaign(log_current_local_states const &states, participants_config const &participants_config,
                             ParticipantsHealth const &health, log_term term) -> log_current_supervision_election {
        auto election = log_current_supervision_election();
        election.term = term;

        for (auto const &[participant, flags] : participants_config.participants) {
            auto const excluded = not flags.allowedAsLeader;
            auto const healthy = health.notIsFailed(participant);

            auto maybeStatus = std::invoke(
                [&states](ParticipantId const &participant) -> std::optional<log_current_local_state> {
                    auto status = states.find(participant);
                    if (status != states.end()) {
                        return status->second;
                    } else {
                        return std::nullopt;
                    }
                },
                participant);

            auto reason = computeReason(maybeStatus, healthy, excluded, term);
            election.detail.emplace(participant, reason);

            if (reason == log_current_supervision_election::ErrorCode::OK) {
                election.participantsAvailable += 1;

                if (maybeStatus->spearhead >= election.bestTermIndex) {
                    if (maybeStatus->spearhead != election.bestTermIndex) {
                        election.electibleLeaderSet.clear();
                    }
                    election.electibleLeaderSet.push_back(participant);
                    election.bestTermIndex = maybeStatus->spearhead;
                }
            }
        }
        return election;
    }

    // If the currentTerm does not have a leader, we have to select one
    // participant to become the leader. For this we have to
    //
    //  * have enough participants (one participant more than
    //    writeConcern)
    //  * have to have enough participants that have not failed or
    //    rebooted
    //
    // The subset of electable participants is determined. A participant is
    // electable if it is
    //  * allowedAsLeader
    //  * not marked as failed
    //  * amongst the participant with the most recent TermIndex.
    //
    auto checkLeaderPresent(SupervisionContext &ctx, Log const &log, ParticipantsHealth const &health) -> void {
        if (!log.plan.has_value()) {
            return;
        }
        TRI_ASSERT(log.plan.has_value());
        auto const &plan = *log.plan;

        if (!plan.currentTerm.has_value()) {
            return;
        }
        TRI_ASSERT(plan.currentTerm.has_value());
        auto const &currentTerm = *plan.currentTerm;

        if (!log.current.has_value()) {
            return;
        }
        TRI_ASSERT(log.current.has_value());
        auto const &current = *log.current;

        if (currentTerm.leader) {
            return;
        }

        // Check whether there are enough participants to reach a quorum
        if (plan.participants_config.participants.size() + 1 <= plan.participants_config.config.effectiveWriteConcern) {
            ctx.reportStatus<log_current_supervision::LeaderElectionImpossible>();
            ctx.createAction<NoActionPossibleAction>();
            return;
        }

        TRI_ASSERT(plan.participants_config.participants.size() + 1 >
                   plan.participants_config.config.effectiveWriteConcern);

        auto const requiredNumberOfOKParticipants =
            plan.participants_config.participants.size() + 1 - plan.participants_config.config.effectiveWriteConcern;

        // Find the participants that are healthy and that have the best log_term
        auto election = runElectionCampaign(current.localState, plan.participants_config, health, currentTerm.term);
        election.participantsRequired = requiredNumberOfOKParticipants;

        auto const numElectible = election.electibleLeaderSet.size();

        if (numElectible == 0 || numElectible > std::numeric_limits<uint16_t>::max()) {
            // TODO: enter some detail about numElectible
            ctx.reportStatus<log_current_supervision::LeaderElectionOutOfBounds>();
            ctx.createAction<NoActionPossibleAction>();
            return;
        }

        if (election.participantsAvailable >= requiredNumberOfOKParticipants) {
            // We randomly elect on of the electible leaders
            auto const maxIdx = static_cast<uint16_t>(numElectible - 1);
            auto const &newLeader = election.electibleLeaderSet.at(RandomGenerator::interval(maxIdx));
            auto const &newLeaderRebootId = health.getRebootId(newLeader);

            if (newLeaderRebootId.has_value()) {
                ctx.reportStatus<log_current_supervision::LeaderElectionSuccess>(election);
                ctx.createAction<LeaderElectionAction>(log_plan_term_specification::Leader(newLeader, *newLeaderRebootId),
                                                       election);
                return;
            } else {
                // TODO: better error
                //       return LeaderElectionImpossibleAction();
                //      ctx.reportStatus
                return;
            }
        } else {
            // Not enough participants were available to form a quorum, so
            // we can't elect a leader
            ctx.reportStatus<log_current_supervision::LeaderElectionQuorumNotReached>(election);
            ctx.createAction<NoActionPossibleAction>();
            return;
        }
    }

    auto checkLeaderHealthy(SupervisionContext &ctx, Log const &log, ParticipantsHealth const &health) -> void {
        if (!log.plan.has_value()) {
            return;
        }
        TRI_ASSERT(log.plan.has_value());
        auto const plan = *log.plan;

        if (!log.current.has_value()) {
            return;
        }
        TRI_ASSERT(log.current.has_value());
        auto const current = *log.current;

        if (!plan.currentTerm.has_value()) {
            return;
        }
        TRI_ASSERT(plan.currentTerm.has_value());
        auto const &currentTerm = *plan.currentTerm;

        if (!currentTerm.leader.has_value()) {
            return;
        }

        // If the leader is unhealthy, write a new term that
        // does not have a leader.
        if (isLeaderFailed(*currentTerm.leader, health)) {
            // Make sure the new term is bigger than any
            // term seen by participants in current
            auto minTerm = currentTerm.term;
            for (auto [participant, state] : current.localState) {
                if (state.spearhead.term > minTerm) {
                    minTerm = state.spearhead.term;
                }
            }
            ctx.createAction<WriteEmptyTermAction>(minTerm);
        }
    }

    auto checkLeaderRemovedFromTargetParticipants(SupervisionContext &ctx,
                                                  Log const &log,
                                                  ParticipantsHealth const &health) -> void {
        auto const &target = log.target;

        if (!log.plan.has_value()) {
            return;
        }
        TRI_ASSERT(log.plan.has_value());
        auto const &plan = *log.plan;

        if (!plan.currentTerm.has_value()) {
            return;
        }
        TRI_ASSERT(plan.currentTerm.has_value());
        auto const &currentTerm = *plan.currentTerm;

        if (!currentTerm.leader.has_value()) {
            return;
        }
        TRI_ASSERT(currentTerm.leader.has_value());
        auto const &leader = *currentTerm.leader;

        if (!log.current.has_value()) {
            return;
        }
        TRI_ASSERT(log.current.has_value());
        auto const &current = *log.current;

        if (!isConfigurationCommitted(log)) {
            ctx.reportStatus<log_current_supervision::WaitingForConfigCommitted>();
            return;
        }
        auto const &committedParticipants = current.leader->committedparticipants_config->participants;

        if (target.participants.find(leader.serverId) == target.participants.end()) {
            auto const acceptableLeaderSet = getParticipantsAcceptableAsLeaders(
                current.leader->serverId, current.leader->committedparticipants_config->participants);

            //  Check whether we already have a participant that is
            //  acceptable and forced
            //
            //  if so, make them leader
            for (auto const &participant : acceptableLeaderSet) {
                TRI_ASSERT(committedParticipants.find(participant) != committedParticipants.end());
                auto const &flags = committedParticipants.at(participant);

                if (participant != current.leader->serverId and flags.forced) {
                    auto const &rebootId = health.getRebootId(participant);
                    if (rebootId.has_value()) {
                        ctx.createAction<SwitchLeaderAction>(log_plan_term_specification::Leader {participant, *rebootId});
                        return;
                    } else {
                        // TODO: this should get the participant
                        ctx.reportStatus<log_current_supervision::SwitchLeaderFailed>();
                    }
                }
            }

            // Did not find a participant above, so pick one at random
            // and force it.
            auto const numElectible = acceptableLeaderSet.size();
            if (numElectible > 0) {
                auto const maxIdx = static_cast<uint16_t>(numElectible - 1);
                auto const &chosenOne = acceptableLeaderSet.at(RandomGenerator::interval(maxIdx));

                TRI_ASSERT(committedParticipants.find(chosenOne) != committedParticipants.end());
                auto flags = committedParticipants.at(chosenOne);

                flags.forced = true;

                ctx.createAction<UpdateParticipantFlagsAction>(chosenOne, flags);
            } else {
                // We did not have a selectable leader
                ctx.reportStatus<log_current_supervision::SwitchLeaderFailed>();
            }
        }
    }

    auto check_leader_setInTarget(SupervisionContext &ctx, Log const &log, ParticipantsHealth const &health) -> void {
        auto const &target = log.target;

        if (!log.plan.has_value()) {
            return;
        }
        TRI_ASSERT(log.plan.has_value());
        auto const &plan = *log.plan;

        if (target.leader.has_value()) {
            // The leader set in target is not valid
            if (plan.participants_config.participants.find(*target.leader) ==
                plan.participants_config.participants.end()) {
                // TODO: Add detail which leader we find invalid (or even rename this
                // status code to leader not a participant)
                ctx.reportStatus<log_current_supervision::TargetLeaderInvalid>();
                return;
            }

            if (!health.notIsFailed(*target.leader)) {
                ctx.reportStatus<log_current_supervision::TargetLeaderFailed>();
                return;
            };

            if (hasCurrentTermWithLeader(log) and target.leader != plan.currentTerm->leader->serverId) {
                TRI_ASSERT(plan.participants_config.participants.find(*target.leader) !=
                           plan.participants_config.participants.end());
                auto const &planLeaderConfig = plan.participants_config.participants.at(*target.leader);

                if (!planLeaderConfig.forced) {
                    auto desiredFlags = planLeaderConfig;
                    desiredFlags.forced = true;
                    ctx.createAction<UpdateParticipantFlagsAction>(*target.leader, desiredFlags);
                    return;
                }

                if (!planLeaderConfig.allowedAsLeader) {
                    ctx.reportStatus<log_current_supervision::TargetLeaderExcluded>();
                    return;
                }

                if (!isConfigurationCommitted(log)) {
                    ctx.reportStatus<log_current_supervision::WaitingForConfigCommitted>();
                    ctx.createAction<NoActionPossibleAction>();
                    return;
                }

                auto const &rebootId = health.getRebootId(*target.leader);
                if (rebootId.has_value()) {
                    ctx.createAction<SwitchLeaderAction>(log_plan_term_specification::Leader {*target.leader, *rebootId});
                } else {
                    ctx.reportStatus<log_current_supervision::TargetLeaderInvalid>();
                }
            }
        }
    }

    // Pick leader at random from participants
    auto pickRandomParticipantToBeLeader(ParticipantsFlagsMap const &participants,
                                         ParticipantsHealth const &health,
                                         uint64_t log_id) -> std::optional<ParticipantId> {
        auto acceptableParticipants = std::vector<ParticipantId> {};

        for (auto [part, flags] : participants) {
            if (flags.allowedAsLeader && health.contains(part)) {
                acceptableParticipants.emplace_back(part);
            }
        }

        if (!acceptableParticipants.empty()) {
            auto maxIdx = static_cast<uint16_t>(acceptableParticipants.size());
            auto p = acceptableParticipants.begin();

            std::advance(p, log_id % maxIdx);

            return *p;
        }

        return std::nullopt;
    }

    auto pickLeader(std::optional<ParticipantId> targetLeader, ParticipantsFlagsMap const &participants,
                    ParticipantsHealth const &health, uint64_t log_id)
        -> std::optional<log_plan_term_specification::Leader> {
        auto leaderId = targetLeader;

        if (!leaderId) {
            leaderId = pickRandomParticipantToBeLeader(participants, health, log_id);
        }

        if (leaderId.has_value()) {
            auto const &rebootId = health.getRebootId(*leaderId);
            if (rebootId.has_value()) {
                return log_plan_term_specification::Leader {*leaderId, *rebootId};
            }
        };

        return std::nullopt;
    }

    auto checkLogExists(SupervisionContext &ctx, Log const &log, ParticipantsHealth const &health) -> void {
        auto const &target = log.target;
        if (!log.plan) {
            // The log is not planned right now, so we create it
            // provided we have enough participants
            if (target.participants.size() + 1 < target.config.writeConcern) {
                ctx.reportStatus<log_current_supervision::TargetNotEnoughParticipants>();
                ctx.createAction<NoActionPossibleAction>();
            } else {
                auto leader = pickLeader(target.leader, target.participants, health, log.target.id.id());
                auto config = log_plan_config(target.config.writeConcern, target.config.softWriteConcern,
                                            target.config.waitForSync);
                ctx.createAction<AddLogToPlanAction>(target.id, target.participants, config, leader);
            }
        }
    }

    auto checkCurrentExists(SupervisionContext &ctx, Log const &log) -> void {
        // If the Current subtree does not exist yet, create it by writing
        // a message into it.
        if (!log.current || !log.current->supervision) {
            ctx.createAction<CurrentNotAvailableAction>();
        }
    }

    auto checkParticipantToAdd(SupervisionContext &ctx, Log const &log, ParticipantsHealth const &health) -> void {
        auto const &target = log.target;

        if (!log.plan.has_value()) {
            return;
        }
        TRI_ASSERT(log.plan.has_value());
        auto const &plan = *log.plan;

        for (auto const &[targetParticipant, targetFlags] : target.participants) {
            if (auto const &planParticipant = plan.participants_config.participants.find(targetParticipant);
                planParticipant == plan.participants_config.participants.end()) {
                ctx.createAction<AddParticipantToPlanAction>(targetParticipant, targetFlags);
            }
        }
    }

    auto checkParticipantToRemove(SupervisionContext &ctx, Log const &log, ParticipantsHealth const &health) -> void {
        auto const &target = log.target;

        if (!log.plan.has_value()) {
            return;
        }
        TRI_ASSERT(log.plan.has_value());
        auto const &plan = *log.plan;

        if (!log.current.has_value() || !log.current->leader.has_value()) {
            return;
        };
        TRI_ASSERT(log.current.has_value());
        TRI_ASSERT(log.current->leader.has_value());
        auto const &leader = *log.current->leader;

        if (!leader.committedparticipants_config.has_value()) {
            return;
        }
        TRI_ASSERT(leader.committedparticipants_config.has_value());
        auto const &committedparticipants_config = *leader.committedparticipants_config;

        auto const &targetParticipants = target.participants;
        auto const &planParticipants = plan.participants_config.participants;

        for (auto const &[maybeRemovedParticipant, maybeRemovedParticipantFlags] : planParticipants) {
            // never remove a leader
            if (targetParticipants.find(maybeRemovedParticipant) == targetParticipants.end() &&
                maybeRemovedParticipant != leader.serverId) {
                // If the participant is not allowed in Quorum it is safe to remove it
                if (!maybeRemovedParticipantFlags.allowedInQuorum &&
                    committedparticipants_config.generation == plan.participants_config.generation) {
                    ctx.createAction<RemoveParticipantFromPlanAction>(maybeRemovedParticipant);
                } else if (maybeRemovedParticipantFlags.allowedInQuorum) {
                    // A participant can only be removed without risk,
                    // if it is not member of any quorum
                    auto newFlags = maybeRemovedParticipantFlags;
                    newFlags.allowedInQuorum = false;
                    ctx.createAction<UpdateParticipantFlagsAction>(maybeRemovedParticipant, newFlags);
                } else {
                    // still waiting
                    ctx.reportStatus<log_current_supervision::WaitingForConfigCommitted>();
                }
            }
        }
    }

    auto checkParticipantWithFlagsToUpdate(SupervisionContext &ctx, Log const &log, ParticipantsHealth const &health)
        -> void {
        auto const &target = log.target;

        if (!log.plan.has_value()) {
            return;
        }
        TRI_ASSERT(log.plan.has_value());
        auto const &plan = *log.plan;

        for (auto const &[targetParticipant, targetFlags] : target.participants) {
            if (auto const &planParticipant = plan.participants_config.participants.find(targetParticipant);
                planParticipant != std::end(plan.participants_config.participants)) {
                // participant is in plan, check whether flags are the same
                if (targetFlags != planParticipant->second) {
                    // Flags changed, so we need to commit new flags for this participant
                    ctx.createAction<UpdateParticipantFlagsAction>(targetParticipant, targetFlags);
                }
            }
        }
    }

    auto checkConfigUpdated(SupervisionContext &ctx, Log const &log, ParticipantsHealth const &health) -> void {
        ctx.reportStatus<log_current_supervision::ConfigChangeNotImplemented>();
    }

    auto check_converged(SupervisionContext &ctx, Log const &log) {
        auto const &target = log.target;

        if (!log.current.has_value()) {
            return;
        }
        TRI_ASSERT(log.current.has_value());
        auto const &current = *log.current;

        if (target.version.has_value() &&
            (!current.supervision.has_value() || target.version != current.supervision->targetVersion)) {
            ctx.createAction<ConvergedToTargetAction>(target.version);
        }
    }
    //
    // This function is called from Agency/Supervision.cpp every k seconds for
    // every replicated log in every database.
    //
    // This means that this function is always going to deal with exactly *one*
    // replicated log.
    //
    // A ReplicatedLog has a Target, a Plan, and a Current subtree in the agency,
    // and these three subtrees are passed into checkReplicatedLog in the form
    // of C++ structs.
    //
    // The return value of this function is an Action, where the type Action is a
    // std::variant of all possible actions that we can perform as a result of
    // checkReplicatedLog.
    //
    // These actions are executes by using std::visit via an Executor struct that
    // contains the necessary context.
    auto checkReplicatedLog(SupervisionContext &ctx, Log const &log, ParticipantsHealth const &health) -> void {
        // Check whether the log (that exists in target by virtue of
        // checkReplicatedLog being called here) is planned. If not, then create it,
        // provided we have enough participants If there are not enough participants
        // we can only report back that this log cannot be created.
        checkLogExists(ctx, log, health);

        // FIXME: This is probably not necessary, as all other check functions should
        //        check whether current is available (and not do anything if it is
        //        not)
        checkCurrentExists(ctx, log);

        // If currentTerm's leader entry does not have a value,
        // make sure a leader is elected.
        checkLeaderPresent(ctx, log, health);

        // If the leader is unhealthy, write a new term that
        // does not have a leader.
        // In the next round this will lead to a leadership election.
        checkLeaderHealthy(ctx, log, health);

        // Check whether a participant was added in Target that is not in Plan.
        // If so, add it to Plan.
        //
        // This has to happen before checkLeaderRemovedFromTargetParticipants,
        // because we don't want to make anyone leader who is not in participants
        // anymore
        checkParticipantToAdd(ctx, log, health);

        // If a participant is in Plan but not in Target, gracefully
        // remove it
        checkParticipantToRemove(ctx, log, health);

        // If the participant who is leader has been removed from target,
        // gracefully remove it by selecting a different eligible participant
        // as leader
        //
        // At this point there should only ever be precisely one participant to
        // remove (the current leader); Once it is not the leader anymore it will be
        // disallowed from any quorum above.
        checkLeaderRemovedFromTargetParticipants(ctx, log, health);

        // Check whether a specific participant is configured in Target to become
        // the leader. This requires that participant to be flagged to always be
        // part of a quorum; once that change is committed, the leader can be
        // switched if the target.leader participant is healty.
        //
        // This operation can fail and
        // TODO: Report if leaderInTarget fails.
        check_leader_setInTarget(ctx, log, health);

        // If the user has updated flags for a participant, which is detected by
        // comparing Target to Plan, write that change to Plan.
        // If the configuration differs between Target and Plan,
        // apply the new configuration.
        checkParticipantWithFlagsToUpdate(ctx, log, health);

        // TODO: This has not been implemented yet!
        checkConfigUpdated(ctx, log, health);

        // Check whether we have converged, and if so, report and set version
        // to target version
        check_converged(ctx, log);
    }

    auto executeCheckReplicatedLog(DatabaseID const &dbName, std::string const &log_idString, Log log,
                                   ParticipantsHealth const &health, nil::dbms::agency::envelope envelope) noexcept
        -> nil::dbms::agency::envelope {
        SupervisionContext sctx;
        auto const now = std::chrono::system_clock::now();
        auto const log_id = log.target.id;
        auto const hasStatusReport = log.current && log.current->supervision && log.current->supervision->statusReport;

        // check if error reporting is enabled
        if (log.current && log.current->supervision && log.current->supervision->lastTimeModified.has_value()) {
            auto const lastMod = *log.current->supervision->lastTimeModified;
            if ((now - lastMod) > std::chrono::seconds {15}) {
                sctx.enableErrorReporting();
            }
        }

        auto maxActionsTraceLength = std::invoke([&log]() {
            if (log.target.supervision.has_value()) {
                return log.target.supervision->maxActionsTraceLength;
            } else {
                return static_cast<size_t>(0);
            }
        });

        checkReplicatedLog(sctx, log, health);

        bool const hasNoExecutableAction = std::holds_alternative<EmptyAction>(sctx.getAction()) ||
                                           std::holds_alternative<NoActionPossibleAction>(sctx.getAction());
        // now check if there is status update
        if (hasNoExecutableAction) {
            // there is only a status update
            if (sctx.isErrorReportingEnabled()) {
                // now compare the new status with the old status
                if (log.current && log.current->supervision) {
                    if (log.current->supervision->statusReport == sctx.getReport()) {
                        // report did not change, do not create a transaction
                        return envelope;
                    }
                }
            }
        }

        auto actionCtx = nil::dbms::replication::log::executeAction(std::move(log), sctx.getAction());

        if (sctx.isErrorReportingEnabled()) {
            if (sctx.getReport().empty()) {
                if (hasStatusReport) {
                    actionCtx.modify<log_current_supervision>(
                        [&](auto &supervision) { supervision.statusReport.reset(); });
                }
            } else {
                actionCtx.modify<log_current_supervision>(
                    [&](auto &supervision) { supervision.statusReport = std::move(sctx.getReport()); });
            }
        } else if (std::holds_alternative<ConvergedToTargetAction>(sctx.getAction())) {
            actionCtx.modify<log_current_supervision>([&](auto &supervision) { supervision.statusReport.reset(); });
        }

        // update last time modified
        if (!hasNoExecutableAction) {
            actionCtx.modify<log_current_supervision>([&](auto &supervision) { supervision.lastTimeModified = now; });
        }

        if (!actionCtx.has_modification()) {
            return envelope;
        }

        return build_agency_transaction(dbName, log_id, sctx, actionCtx, maxActionsTraceLength, std::move(envelope));
    }

    auto build_agency_transaction(DatabaseID const &dbName, log_id const &log_id, SupervisionContext &sctx,
                                ActionContext &actx, size_t maxActionsTraceLength, nil::dbms::agency::envelope envelope)
        -> nil::dbms::agency::envelope {
        auto planPath = paths::plan()->replicatedLogs()->database(dbName)->log(log_id)->str();

        auto currentSupervisionPath =
            paths::current()->replicatedLogs()->database(dbName)->log(log_id)->supervision()->str();

        // If we want to keep a trace of actions, then only record actions
        // that actually modify the data structure. This excludes the EmptyAction
        // and the NoActionPossibleAction.
        if (sctx.hasModifyingAction() && maxActionsTraceLength > 0) {
            envelope = envelope.write()
                           .push_queue_emplace(
                               nil::dbms::cluster::paths::aliases::current()
                                   ->replicatedLogs()
                                   ->database(dbName)
                                   ->log(log_id)
                                   ->actions()
                                   ->str(),
                               // TODO: struct + inspect + transformWith
                               [&](velocypack::Builder &b) {
                                   VPackObjectBuilder ob(&b);
                                   b.add("time", VPackValue(timepointToString(std::chrono::system_clock::now())));
                                   b.add(VPackValue("desc"));
                                   std::visit([&b](auto &&arg) { serialize(b, arg); }, sctx.getAction());
                               },
                               maxActionsTraceLength)
                           .precs()
                           .isNotEmpty(paths::target()->replicatedLogs()->database(dbName)->log(log_id)->str())
                           .end();
        }

        return envelope
            .write()
            // this is here to trigger all waitForPlan, even if we only
            // update current.
            .inc(paths::plan()->version()->str())
            .cond(actx.has_modification_for<log_plan_specification>(),
                  [&](nil::dbms::agency::envelope::write_trx &&trx) {
                      return std::move(trx).emplace_object(planPath, [&](VPackBuilder &builder) {
                          velocypack::serialize(builder, actx.getValue<log_plan_specification>());
                      });
                  })
            .cond(actx.has_modification_for<log_current_supervision>(),
                  [&](nil::dbms::agency::envelope::write_trx &&trx) {
                      return std::move(trx)
                          .emplace_object(currentSupervisionPath,
                                          [&](VPackBuilder &builder) {
                                              velocypack::serialize(builder, actx.getValue<log_current_supervision>());
                                          })
                          .inc(paths::current()->version()->str());
                  })
            .precs()
            .isNotEmpty(paths::target()->replicatedLogs()->database(dbName)->log(log_id)->str())
            .end();
    }

}    // namespace nil::dbms::replication::log
