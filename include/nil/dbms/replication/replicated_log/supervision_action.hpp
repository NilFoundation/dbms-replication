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

#include "velocypack/Builder.h"
#include "velocypack/velocypack-common.h"
#include <memory>
#include <utility>

#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include "agency_log_specification.hpp"
#include "log_common.hpp"
#include "nil/dbms/replication/supervision/modify_context.hpp"

using namespace nil::dbms::replication::agency;

namespace nil::dbms::agency {
    struct envelope;

}

namespace nil::dbms::replication::replicated_log {

    using ActionContext = modify_context<log_plan_specification, log_current_supervision>;

    /* The empty action signifies that no action has been put
     * into an action context yet; we use a seprarte action
     * instead of a std::optional<Action>, because it is less
     * prone to crashes and undefined behaviour
     */
    struct empty_action {
        constexpr static const char *name = "EmptyAction";

        explicit empty_action() {};

        auto execute(ActionContext &ctx) const -> void {
        }
    };
    template<typename Inspector>
    auto inspect(Inspector &f, empty_action &x) {
        auto hack = std::string {x.name};
        return f.object(x).fields(f.field("type", hack));
    }

    /*
     * This action is placed into the supervision action to prevent
     * any other action from taking place.
     *
     * This is *different* from no action having been put into
     * the context as sometimes we will report a problem through
     * the reporting but do not want to continue;
     *
     * This action does not modify the agency state.
     */
    struct no_action_possible_action {
        constexpr static const char *name = "NoActionPossibleAction";

        explicit no_action_possible_action() {};

        auto execute(ActionContext &ctx) const -> void {
        }
    };
    template<typename Inspector>
    auto inspect(Inspector &f, no_action_possible_action &x) {
        auto hack = std::string {x.name};
        return f.object(x).fields(f.field("type", hack));
    }

    struct add_log_to_plan_action {
        constexpr static const char *name = "AddLogToPlanAction";

        add_log_to_plan_action(LogId const id, ParticipantsFlagsMap participants, log_plan_config config,
                               std::optional<log_plan_term_specification::Leader> leader) :
            _id(id),
            _participants(std::move(participants)), _config(std::move(config)), _leader(std::move(leader)) {};
        LogId _id;
        ParticipantsFlagsMap _participants;
        log_plan_config _config;
        std::optional<log_plan_term_specification::Leader> _leader;

        auto execute(ActionContext &ctx) const -> void {
            auto newPlan = log_plan_specification(
                _id, log_plan_term_specification(log_term {1}, _leader),
                participants_config {.generation = 1, .participants = _participants, .config = _config});
            newPlan.owner = "target";
            ctx.setValue<log_plan_specification>(std::move(newPlan));
        }
    };

    template<typename Inspector>
    auto inspect(Inspector &f, add_log_to_plan_action &x) {
        auto hack = std::string {x.name};
        return f.object(x).fields(f.field("type", hack), f.field("id", x._id), f.field("participants", x._participants),
                                  f.field("leader", x._leader), f.field("config", x._config));
    }

    struct current_not_available_action {
        constexpr static const char *name = "CurrentNotAvailableAction";

        auto execute(ActionContext &ctx) const -> void {
            ctx.setValue<log_current_supervision>();
        }
    };
    template<typename Inspector>
    auto inspect(Inspector &f, current_not_available_action &x) {
        auto hack = std::string {x.name};
        return f.object(x).fields(f.field("type", hack));
    }

    struct switch_leader_action {
        constexpr static const char *name = "SwitchLeaderAction";

        switch_leader_action(log_plan_term_specification::Leader const &leader) : _leader {leader} {};

        log_plan_term_specification::Leader _leader;

        auto execute(ActionContext &ctx) const -> void {
            ctx.modify<log_plan_specification>([&](log_plan_specification &plan) {
                plan.currentTerm->term = log_term {plan.currentTerm->term.value + 1};
                plan.currentTerm->leader = _leader;
            });
        }
    };
    template<typename Inspector>
    auto inspect(Inspector &f, switch_leader_action &x) {
        auto hack = std::string {x.name};
        return f.object(x).fields(f.field("type", hack), f.field("leader", x._leader));
    }

    struct write_empty_term_action {
        constexpr static const char *name = "WriteEmptyTermAction";
        log_term minTerm;

        explicit write_empty_term_action(log_term minTerm) : minTerm {minTerm} {};

        auto execute(ActionContext &ctx) const -> void {
            ctx.modify<log_plan_specification>([&](log_plan_specification &plan) {
                // TODO: what to do if currentTerm does not have a value?
                //       this shouldn't happen, but what if it does?
                plan.currentTerm->term = log_term {minTerm.value + 1};
                plan.currentTerm->leader.reset();
            });
        }
    };
    template<typename Inspector>
    auto inspect(Inspector &f, write_empty_term_action &x) {
        auto hack = std::string {x.name};
        return f.object(x).fields(f.field("type", hack), f.field("minTerm", x.minTerm));
    }

    struct leader_election_action {
        constexpr static const char *name = "LeaderElectionAction";

        leader_election_action(log_plan_term_specification::Leader electedLeader,
                               log_current_supervision_election const &electionReport) :
            _electedLeader {electedLeader},
            _electionReport(electionReport) {};

        log_plan_term_specification::Leader _electedLeader;
        log_current_supervision_election _electionReport;

        auto execute(ActionContext &ctx) const -> void {
            ctx.modify<log_plan_specification>([&](log_plan_specification &plan) {
                plan.currentTerm->term = log_term {plan.currentTerm->term.value + 1};
                plan.currentTerm->leader = _electedLeader;
            });
        }
    };
    template<typename Inspector>
    auto inspect(Inspector &f, leader_election_action &x) {
        auto hack = std::string {x.name};
        return f.object(x).fields(f.field("type", hack), f.field("election", x._electionReport),
                                  f.field("electedLeader", x._electedLeader));
    }

    struct update_participant_flags_action {
        constexpr static const char *name = "UpdateParticipantFlagsAction";

        update_participant_flags_action(ParticipantId const &participant, participant_flags const &flags) :
            _participant(participant), _flags(flags) {};

        ParticipantId _participant;
        participant_flags _flags;

        auto execute(ActionContext &ctx) const -> void {
            ctx.modify<log_plan_specification>([&](log_plan_specification &plan) {
                TRI_ASSERT(plan.participantsConfig.participants.find(_participant) !=
                           plan.participantsConfig.participants.end());
                plan.participantsConfig.participants.at(_participant) = _flags;
                plan.participantsConfig.generation += 1;
            });
        }
    };
    template<typename Inspector>
    auto inspect(Inspector &f, update_participant_flags_action &x) {
        auto hack = std::string {x.name};
        return f.object(x).fields(f.field("type", hack), f.field("participant", x._participant),
                                  f.field("flags", x._flags));
    }

    struct add_participant_to_plan_action {
        constexpr static const char *name = "AddParticipantToPlanAction";

        add_participant_to_plan_action(ParticipantId const &participant, participant_flags const &flags) :
            _participant(participant), _flags(flags) {
        }

        ParticipantId _participant;
        participant_flags _flags;

        auto execute(ActionContext &ctx) const -> void {
            ctx.modify<log_plan_specification>([&](log_plan_specification &plan) {
                plan.participantsConfig.generation += 1;
                plan.participantsConfig.participants.emplace(_participant, _flags);
            });
        }
    };
    template<typename Inspector>
    auto inspect(Inspector &f, add_participant_to_plan_action &x) {
        auto hack = std::string {x.name};
        return f.object(x).fields(f.field("type", hack), f.field("participant", x._participant),
                                  f.field("flags", x._flags));
    }

    struct remove_participant_from_plan_action {
        constexpr static const char *name = "RemoveParticipantFromPlanAction";

        remove_participant_from_plan_action(ParticipantId const &participant) : _participant(participant) {};

        ParticipantId _participant;

        auto execute(ActionContext &ctx) const -> void {
            ctx.modify<log_plan_specification>([&](log_plan_specification &plan) {
                plan.participantsConfig.participants.erase(_participant);
                plan.participantsConfig.generation += 1;
            });
        }
    };
    template<typename Inspector>
    auto inspect(Inspector &f, remove_participant_from_plan_action &x) {
        auto hack = std::string {x.name};
        return f.object(x).fields(f.field("type", hack), f.field("participant", x._participant));
    }

    struct update_log_config_action {
        constexpr static const char *name = "UpdateLogConfigAction";

        update_log_config_action(log_plan_config const &config) : _config(config) {};

        log_plan_config _config;

        auto execute(ActionContext &ctx) const -> void {
            // TODO: updating log config is not implemented yet
        }
    };
    template<typename Inspector>
    auto inspect(Inspector &f, update_log_config_action &x) {
        auto hack = std::string {x.name};
        return f.object(x).fields(f.field("type", hack));
    }

    struct converged_to_target_action {
        constexpr static const char *name = "ConvergedToTargetAction";
        std::optional<std::uint64_t> version {std::nullopt};

        auto execute(ActionContext &ctx) const -> void {
            ctx.modify_or_create<log_current_supervision>(
                [&](log_current_supervision &currentSupervision) { currentSupervision.targetVersion = version; });
        }
    };

    template<typename Inspector>
    auto inspect(Inspector &f, converged_to_target_action &x) {
        auto hack = std::string {x.name};
        return f.object(x).fields(f.field("type", hack), f.field("version", x.version));
    }

    /* NOTE!
     *
     * EmptyAction *has* *to* *be* first, as the default constructor
     * of a variant constructs the variant with index 0
     *
     */
    using Action =
        std::variant<empty_action, no_action_possible_action, add_log_to_plan_action, current_not_available_action,
                     switch_leader_action, write_empty_term_action, leader_election_action,
                     update_participant_flags_action, add_participant_to_plan_action,
                     remove_participant_from_plan_action, update_log_config_action, converged_to_target_action>;

    auto executeAction(Log log, Action &action) -> ActionContext;
}    // namespace nil::dbms::replication::replicated_log

#include "inspection/vpack.h"
#include "agency_specification_inspectors.hpp"

template<>
struct fmt::formatter<nil::dbms::replication::replicated_log::Action> : formatter<string_view> {
    // parse is inherited from formatter<string_view>.
    template<typename FormatContext>
    auto format(nil::dbms::replication::replicated_log::Action a, FormatContext &ctx) const {
        VPackBuilder builder;

        std::visit([&builder](auto &&arg) { nil::dbms::velocypack::serialize(builder, arg); }, a);
        return formatter<string_view>::format(builder.slice().toJson(), ctx);
    }
};
