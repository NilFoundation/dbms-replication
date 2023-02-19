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

#include "agency_log_specification.hpp"

#include "inspection/vpack.h"
#include "inspection/transformers.h"

namespace nil::dbms::replication::agency {

    namespace static_strings {
        auto constexpr CommittedParticipantsConfig = std::string_view {"committedParticipantsConfig"};
        auto constexpr ParticipantsConfig = std::string_view {"participantsConfig"};
        auto constexpr BestTermIndex = std::string_view {"bestTermIndex"};
        auto constexpr ParticipantsRequired = std::string_view {"participantsRequired"};
        auto constexpr ParticipantsAvailable = std::string_view {"participantsAvailable"};
        auto constexpr Details = std::string_view {"details"};
        auto constexpr ElectibleLeaderSet = std::string_view {"electibleLeaderSet"};
        auto constexpr Election = std::string_view {"election"};
        auto constexpr Error = std::string_view {"error"};
        auto constexpr StatusMessage = std::string_view {"StatusMessage"};
        auto constexpr StatusReport = std::string_view {"StatusReport"};
        auto constexpr LeadershipEstablished = std::string_view {"leadershipEstablished"};
        auto constexpr CommitStatus = std::string_view {"commitStatus"};
        auto constexpr Supervision = std::string_view {"supervision"};
        auto constexpr Leader = std::string_view {"leader"};
        auto constexpr TargetVersion = std::string_view {"targetVersion"};
        auto constexpr Version = std::string_view {"version"};
        auto constexpr Actions = std::string_view {"actions"};
        auto constexpr MaxActionsTraceLength = std::string_view {"maxActionsTraceLength"};
        auto constexpr Code = std::string_view {"code"};
        auto constexpr Message = std::string_view {"message"};
        auto constexpr LastTimeModified = std::string_view {"lastTimeModified"};
        auto constexpr Participant = std::string_view {"participant"};
        auto constexpr Owner = std::string_view {"owner"};
    }    // namespace static_strings

    template<class Inspector>
    auto inspect(Inspector &f, log_plan_term_specification::Leader &x) {
        return f.object(x).fields(f.field(StaticStrings::ServerId, x.serverId),
                                  f.field(StaticStrings::RebootId, x.rebootId));
    }

    template<class Inspector>
    auto inspect(Inspector &f, log_plan_term_specification &x) {
        return f.object(x).fields(f.field(StaticStrings::Term, x.term), f.field(StaticStrings::Leader, x.leader));
    }

    template<class Inspector>
    auto inspect(Inspector &f, log_plan_specification &x) {
        return f.object(x).fields(f.field(StaticStrings::Id, x.id),
                                  f.field(StaticStrings::CurrentTerm, x.currentTerm),
                                  f.field(static_strings::Owner, x.owner),
                                  f.field(static_strings::ParticipantsConfig, x.participantsConfig));
    };

    template<class Inspector>
    auto inspect(Inspector &f, log_current_local_state &x) {
        return f.object(x).fields(f.field(StaticStrings::Term, x.term), f.field(StaticStrings::Spearhead, x.spearhead));
    }

    template<typename Enum>
    struct enum_struct {
        static_assert(std::is_enum_v<Enum>);

        enum_struct() : code {0}, message {} {};
        enum_struct(Enum e) : code(static_cast<std::underlying_type_t<Enum>>(e)), message(to_string(e)) {};

        [[nodiscard]] auto get() const -> Enum {
            return static_cast<Enum>(code);
        }

        std::underlying_type_t<Enum> code;
        std::string message;
    };

    template<class Inspector, typename Enum>
    auto inspect(Inspector &f, enum_struct<Enum> &x) {
        return f.object(x).fields(f.field(static_strings::Code, x.code), f.field(static_strings::Message, x.message));
    };

    template<class Inspector>
    auto inspect(Inspector &f, log_current_supervision_election::ErrorCode &x) {
        if constexpr (Inspector::isLoading) {
            auto v = enum_struct<log_current_supervision_election::ErrorCode>();
            auto res = f.apply(v);
            if (res.ok()) {
                x = v.get();
            }
            return res;
        } else {
            auto v = enum_struct<log_current_supervision_election::ErrorCode>(x);
            return f.apply(v);
        }
    }

    template<class Inspector>
    auto inspect(Inspector &f, log_current_supervision_election &x) {
        return f.object(x).fields(f.field(StaticStrings::Term, x.term),
                                  f.field(static_strings::BestTermIndex, x.bestTermIndex),
                                  f.field(static_strings::ParticipantsRequired, x.participantsRequired),
                                  f.field(static_strings::ParticipantsAvailable, x.participantsAvailable),
                                  f.field(static_strings::Details, x.detail),
                                  f.field(static_strings::ElectibleLeaderSet, x.electibleLeaderSet));
    }

    template<class Inspector>
    auto inspect(Inspector &f, log_current_supervision::target_leader_invalid &x) {
        return f.object(x).fields();
    }

    template<class Inspector>
    auto inspect(Inspector &f, log_current_supervision::target_leader_excluded &x) {
        return f.object(x).fields();
    }

    template<class Inspector>
    auto inspect(Inspector &f, log_current_supervision::target_leader_failed &x) {
        return f.object(x).fields();
    }

    template<class Inspector>
    auto inspect(Inspector &f, log_current_supervision::target_not_enough_participants &x) {
        return f.object(x).fields();
    }

    template<class Inspector>
    auto inspect(Inspector &f, log_current_supervision::waiting_for_config_committed &x) {
        return f.object(x).fields();
    }

    template<class Inspector>
    auto inspect(Inspector &f, log_current_supervision::config_change_not_implemented &x) {
        return f.object(x).fields();
    }

    template<class Inspector>
    auto inspect(Inspector &f, log_current_supervision::leader_election_impossible &x) {
        return f.object(x).fields();
    }

    template<class Inspector>
    auto inspect(Inspector &f, log_current_supervision::leader_election_out_of_bounds &x) {
        return f.object(x).fields();
    }

    template<class Inspector>
    auto inspect(Inspector &f, log_current_supervision::leader_election_quorum_not_reached &x) {
        return f.object(x).fields(f.field("election", x.election));
    }

    template<class Inspector>
    auto inspect(Inspector &f, log_current_supervision::leader_election_success &x) {
        return f.object(x).fields(f.field("election", x.election));
    }

    template<class Inspector>
    auto inspect(Inspector &f, log_current_supervision::switch_leader_failed &x) {
        return f.object(x).fields();
    }

    template<class Inspector>
    auto inspect(Inspector &f, log_current_supervision::plan_not_available &x) {
        return f.object(x).fields();
    }

    template<class Inspector>
    auto inspect(Inspector &f, log_current_supervision::current_not_available &x) {
        return f.object(x).fields();
    }

    template<class Inspector>
    auto inspect(Inspector &f, log_current_supervision::StatusMessage &x) {
        namespace insp = nil::dbms::inspection;
        return f.variant(x)
            .qualified("type", "detail")
            .alternatives(insp::type<log_current_supervision::target_leader_invalid>(
                              log_current_supervision::target_leader_invalid::code),
                          insp::type<log_current_supervision::target_leader_excluded>(
                              log_current_supervision::target_leader_excluded::code),
                          insp::type<log_current_supervision::target_leader_failed>(
                              log_current_supervision::target_leader_failed::code),
                          insp::type<log_current_supervision::target_not_enough_participants>(
                              log_current_supervision::target_not_enough_participants::code),
                          insp::type<log_current_supervision::waiting_for_config_committed>(
                              log_current_supervision::waiting_for_config_committed::code),
                          insp::type<log_current_supervision::config_change_not_implemented>(
                              log_current_supervision::config_change_not_implemented::code),
                          insp::type<log_current_supervision::leader_election_impossible>(
                              log_current_supervision::leader_election_impossible::code),
                          insp::type<log_current_supervision::leader_election_out_of_bounds>(
                              log_current_supervision::leader_election_out_of_bounds::code),
                          insp::type<log_current_supervision::leader_election_quorum_not_reached>(
                              log_current_supervision::leader_election_quorum_not_reached::code),
                          insp::type<log_current_supervision::leader_election_success>(
                              log_current_supervision::leader_election_success::code),
                          insp::type<log_current_supervision::switch_leader_failed>(
                              log_current_supervision::switch_leader_failed::code),
                          insp::type<log_current_supervision::plan_not_available>(
                              log_current_supervision::plan_not_available::code),
                          insp::type<log_current_supervision::current_not_available>(
                              log_current_supervision::current_not_available::code));
    }

    template<class Inspector>
    auto inspect(Inspector &f, log_current_supervision &x) {
        return f.object(x).fields(f.field(static_strings::TargetVersion, x.targetVersion),
                                  f.field(static_strings::StatusReport, x.statusReport),
                                  f.field(static_strings::LastTimeModified, x.lastTimeModified)
                                      .transformWith(inspection::TimeStampTransformer {}));
    }

    template<class Inspector>
    auto inspect(Inspector &f, log_current::Leader &x) {
        return f.object(x).fields(
            f.field(StaticStrings::ServerId, x.serverId),
            f.field(StaticStrings::Term, x.term),
            f.field(static_strings::CommittedParticipantsConfig, x.committedParticipantsConfig),
            f.field(static_strings::LeadershipEstablished, x.leadershipEstablished).fallback(false),
            f.field(static_strings::CommitStatus, x.commitStatus));
    }

    template<class Inspector>
    auto inspect(Inspector &f, log_current::ActionDummy &x) {
        if constexpr (Inspector::isLoading) {
            x = log_current::ActionDummy {};
        } else {
        }
        return nil::dbms::inspection::Status::Success {};
    }

    template<class Inspector>
    auto inspect(Inspector &f, log_current &x) {
        return f.object(x).fields(
            f.field(StaticStrings::LocalStatus, x.localState)
                .fallback(std::unordered_map<ParticipantId, log_current_local_state> {}),
            f.field(static_strings::Supervision, x.supervision),
            f.field(static_strings::Leader, x.leader),
            f.field(static_strings::Actions, x.actions).fallback(std::vector<log_current::ActionDummy> {}));
    }

    template<class Inspector>
    auto inspect(Inspector &f, log_target::Supervision &x) {
        return f.object(x).fields(f.field(static_strings::MaxActionsTraceLength, x.maxActionsTraceLength));
    }

    template<class Inspector>
    auto inspect(Inspector &f, log_target &x) {
        return f.object(x).fields(
            f.field(StaticStrings::Id, x.id),
            f.field(StaticStrings::Participants, x.participants).fallback(ParticipantsFlagsMap {}),
            f.field(StaticStrings::Config, x.config),
            f.field(StaticStrings::Leader, x.leader),
            f.field(static_strings::Version, x.version),
            f.field(static_strings::Owner, x.owner),
            f.field(static_strings::Supervision, x.supervision));
    }

}    // namespace nil::dbms::replication::agency
