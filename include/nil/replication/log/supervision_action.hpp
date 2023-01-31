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

#include <nil/dbms/agency/transaction_builder.hpp>
#include <nil/dbms/replication/log/agency_log_specification.hpp>
#include <nil/dbms/replication/log/log_common.hpp>
#include <nil/dbms/replication/supervision/modify_context.hpp>

using namespace nil::dbms::replication::agency;

namespace nil::dbms::replication::log {

    using ActionContext = ModifyContext<LogPlanSpecification, LogCurrentSupervision>;

    /* The empty action signifies that no action has been put
     * into an action context yet; we use a seprarte action
     * instead of a std::optional<Action>, because it is less
     * prone to crashes and undefined behaviour
     */
    struct EmptyAction {
        constexpr static const char *name = "EmptyAction";

        explicit EmptyAction() {};

        auto execute(ActionContext &ctx) const -> void {
        }
    };
    template<typename Inspector>
    auto inspect(Inspector &f, EmptyAction &x) {
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
    struct NoActionPossibleAction {
        constexpr static const char *name = "NoActionPossibleAction";

        explicit NoActionPossibleAction() {};

        auto execute(ActionContext &ctx) const -> void {
        }
    };
    template<typename Inspector>
    auto inspect(Inspector &f, NoActionPossibleAction &x) {
        auto hack = std::string {x.name};
        return f.object(x).fields(f.field("type", hack));
    }

    struct AddLogToPlanAction {
        constexpr static const char *name = "AddLogToPlanAction";

        AddLogToPlanAction(log_id const id, ParticipantsFlagsMap participants, log_plan_config config,
                           std::optional<LogPlanTermSpecification::Leader> leader) :
            _id(id),
            _participants(std::move(participants)), _config(std::move(config)), _leader(std::move(leader)) {};
        log_id _id;
        ParticipantsFlagsMap _participants;
        log_plan_config _config;
        std::optional<LogPlanTermSpecification::Leader> _leader;

        auto execute(ActionContext &ctx) const -> void {
            auto newPlan = LogPlanSpecification(
                _id, LogPlanTermSpecification(log_term {1}, _leader),
                participants_config {.generation = 1, .participants = _participants, .config = _config});
            newPlan.owner = "target";
            ctx.setValue<LogPlanSpecification>(std::move(newPlan));
        }
    };

    template<typename Inspector>
    auto inspect(Inspector &f, AddLogToPlanAction &x) {
        auto hack = std::string {x.name};
        return f.object(x).fields(f.field("type", hack), f.field("id", x._id), f.field("participants", x._participants),
                                  f.field("leader", x._leader), f.field("config", x._config));
    }

    struct CurrentNotAvailableAction {
        constexpr static const char *name = "CurrentNotAvailableAction";

        auto execute(ActionContext &ctx) const -> void {
            ctx.setValue<LogCurrentSupervision>();
        }
    };
    template<typename Inspector>
    auto inspect(Inspector &f, CurrentNotAvailableAction &x) {
        auto hack = std::string {x.name};
        return f.object(x).fields(f.field("type", hack));
    }

    struct SwitchLeaderAction {
        constexpr static const char *name = "SwitchLeaderAction";

        SwitchLeaderAction(LogPlanTermSpecification::Leader const &leader) : _leader {leader} {};

        LogPlanTermSpecification::Leader _leader;

        auto execute(ActionContext &ctx) const -> void {
            ctx.modify<LogPlanSpecification>([&](LogPlanSpecification &plan) {
                plan.currentTerm->term = log_term {plan.currentTerm->term.value + 1};
                plan.currentTerm->leader = _leader;
            });
        }
    };
    template<typename Inspector>
    auto inspect(Inspector &f, SwitchLeaderAction &x) {
        auto hack = std::string {x.name};
        return f.object(x).fields(f.field("type", hack), f.field("leader", x._leader));
    }

    struct WriteEmptyTermAction {
        constexpr static const char *name = "WriteEmptyTermAction";
        log_term minTerm;

        explicit WriteEmptyTermAction(log_term minTerm) : minTerm {minTerm} {};

        auto execute(ActionContext &ctx) const -> void {
            ctx.modify<LogPlanSpecification>([&](LogPlanSpecification &plan) {
                // TODO: what to do if currentTerm does not have a value?
                //       this shouldn't happen, but what if it does?
                plan.currentTerm->term = log_term {minTerm.value + 1};
                plan.currentTerm->leader.reset();
            });
        }
    };
    template<typename Inspector>
    auto inspect(Inspector &f, WriteEmptyTermAction &x) {
        auto hack = std::string {x.name};
        return f.object(x).fields(f.field("type", hack), f.field("minTerm", x.minTerm));
    }

    struct LeaderElectionAction {
        constexpr static const char *name = "LeaderElectionAction";

        LeaderElectionAction(LogPlanTermSpecification::Leader electedLeader,
                             LogCurrentSupervisionElection const &electionReport) :
            _electedLeader {electedLeader},
            _electionReport(electionReport) {};

        LogPlanTermSpecification::Leader _electedLeader;
        LogCurrentSupervisionElection _electionReport;

        auto execute(ActionContext &ctx) const -> void {
            ctx.modify<LogPlanSpecification>([&](LogPlanSpecification &plan) {
                plan.currentTerm->term = log_term {plan.currentTerm->term.value + 1};
                plan.currentTerm->leader = _electedLeader;
            });
        }
    };
    template<typename Inspector>
    auto inspect(Inspector &f, LeaderElectionAction &x) {
        auto hack = std::string {x.name};
        return f.object(x).fields(f.field("type", hack), f.field("election", x._electionReport),
                                  f.field("electedLeader", x._electedLeader));
    }

    struct UpdateParticipantFlagsAction {
        constexpr static const char *name = "UpdateParticipantFlagsAction";

        UpdateParticipantFlagsAction(ParticipantId const &participant, ParticipantFlags const &flags) :
            _participant(participant), _flags(flags) {};

        ParticipantId _participant;
        ParticipantFlags _flags;

        auto execute(ActionContext &ctx) const -> void {
            ctx.modify<LogPlanSpecification>([&](LogPlanSpecification &plan) {
                TRI_ASSERT(plan.participants_config.participants.find(_participant) !=
                           plan.participants_config.participants.end());
                plan.participants_config.participants.at(_participant) = _flags;
                plan.participants_config.generation += 1;
            });
        }
    };
    template<typename Inspector>
    auto inspect(Inspector &f, UpdateParticipantFlagsAction &x) {
        auto hack = std::string {x.name};
        return f.object(x).fields(f.field("type", hack), f.field("participant", x._participant),
                                  f.field("flags", x._flags));
    }

    struct AddParticipantToPlanAction {
        constexpr static const char *name = "AddParticipantToPlanAction";

        AddParticipantToPlanAction(ParticipantId const &participant, ParticipantFlags const &flags) :
            _participant(participant), _flags(flags) {
        }

        ParticipantId _participant;
        ParticipantFlags _flags;

        auto execute(ActionContext &ctx) const -> void {
            ctx.modify<LogPlanSpecification>([&](LogPlanSpecification &plan) {
                plan.participants_config.generation += 1;
                plan.participants_config.participants.emplace(_participant, _flags);
            });
        }
    };
    template<typename Inspector>
    auto inspect(Inspector &f, AddParticipantToPlanAction &x) {
        auto hack = std::string {x.name};
        return f.object(x).fields(f.field("type", hack), f.field("participant", x._participant),
                                  f.field("flags", x._flags));
    }

    struct RemoveParticipantFromPlanAction {
        constexpr static const char *name = "RemoveParticipantFromPlanAction";

        RemoveParticipantFromPlanAction(ParticipantId const &participant) : _participant(participant) {};

        ParticipantId _participant;

        auto execute(ActionContext &ctx) const -> void {
            ctx.modify<LogPlanSpecification>([&](LogPlanSpecification &plan) {
                plan.participants_config.participants.erase(_participant);
                plan.participants_config.generation += 1;
            });
        }
    };
    template<typename Inspector>
    auto inspect(Inspector &f, RemoveParticipantFromPlanAction &x) {
        auto hack = std::string {x.name};
        return f.object(x).fields(f.field("type", hack), f.field("participant", x._participant));
    }

    struct UpdateLogConfigAction {
        constexpr static const char *name = "UpdateLogConfigAction";

        UpdateLogConfigAction(log_plan_config const &config) : _config(config) {};

        log_plan_config _config;

        auto execute(ActionContext &ctx) const -> void {
            // TODO: updating log config is not implemented yet
        }
    };
    template<typename Inspector>
    auto inspect(Inspector &f, UpdateLogConfigAction &x) {
        auto hack = std::string {x.name};
        return f.object(x).fields(f.field("type", hack));
    }

    struct ConvergedToTargetAction {
        constexpr static const char *name = "ConvergedToTargetAction";
        std::optional<std::uint64_t> version {std::nullopt};

        auto execute(ActionContext &ctx) const -> void {
            ctx.modify_or_create<LogCurrentSupervision>(
                [&](LogCurrentSupervision &currentSupervision) { currentSupervision.targetVersion = version; });
        }
    };

    template<typename Inspector>
    auto inspect(Inspector &f, ConvergedToTargetAction &x) {
        auto hack = std::string {x.name};
        return f.object(x).fields(f.field("type", hack), f.field("version", x.version));
    }

    /* NOTE!
     *
     * EmptyAction *has* *to* *be* first, as the default constructor
     * of a variant constructs the variant with index 0
     *
     */
    using Action = std::variant<EmptyAction, NoActionPossibleAction, AddLogToPlanAction, CurrentNotAvailableAction,
                                SwitchLeaderAction, WriteEmptyTermAction, LeaderElectionAction,
                                UpdateParticipantFlagsAction, AddParticipantToPlanAction,
                                RemoveParticipantFromPlanAction, UpdateLogConfigAction, ConvergedToTargetAction>;

    auto executeAction(Log log, Action &action) -> ActionContext;
}    // namespace nil::dbms::replication::log

#include "inspection/vpack.h"
#include <nil/dbms/replication/log/agency_specification_inspectors.hpp>

template<>
struct fmt::formatter<nil::dbms::replication::log::Action> : formatter<string_view> {
    // parse is inherited from formatter<string_view>.
    template<typename FormatContext>
    auto format(nil::dbms::replication::log::Action a, FormatContext &ctx) const {
        VPackBuilder builder;

        std::visit([&builder](auto &&arg) { nil::dbms::velocypack::serialize(builder, arg); }, a);
        return formatter<string_view>::format(builder.slice().toJson(), ctx);
    }
};
