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

namespace nil {
    namespace dbms {
        namespace replication {
            namespace agency {

                namespace static_strings {
                    auto constexpr
                    Committedparticipants_config = std::string_view{"committedparticipants_config"};
                    auto constexpr
                    participants_config = std::string_view{"participants_config"};
                    auto constexpr
                    BestTermIndex = std::string_view{"bestTermIndex"};
                    auto constexpr
                    ParticipantsRequired = std::string_view{"participantsRequired"};
                    auto constexpr
                    ParticipantsAvailable = std::string_view{"participantsAvailable"};
                    auto constexpr
                    Details = std::string_view{"details"};
                    auto constexpr
                    ElectibleLeaderSet = std::string_view{"electibleLeaderSet"};
                    auto constexpr
                    Election = std::string_view{"election"};
                    auto constexpr
                    Error = std::string_view{"error"};
                    auto constexpr
                    StatusMessage = std::string_view{"StatusMessage"};
                    auto constexpr
                    StatusReport = std::string_view{"StatusReport"};
                    auto constexpr
                    LeadershipEstablished = std::string_view{"leadershipEstablished"};
                    auto constexpr
                    CommitStatus = std::string_view{"commitStatus"};
                    auto constexpr
                    Supervision = std::string_view{"supervision"};
                    auto constexpr
                    Leader = std::string_view{"leader"};
                    auto constexpr
                    TargetVersion = std::string_view{"targetVersion"};
                    auto constexpr
                    Version = std::string_view{"version"};
                    auto constexpr
                    Actions = std::string_view{"actions"};
                    auto constexpr
                    MaxActionsTraceLength = std::string_view{"maxActionsTraceLength"};
                    auto constexpr
                    Code = std::string_view{"code"};
                    auto constexpr
                    Message = std::string_view{"message"};
                    auto constexpr
                    LastTimeModified = std::string_view{"lastTimeModified"};
                    auto constexpr
                    Participant = std::string_view{"participant"};
                    auto constexpr
                    Owner = std::string_view{"owner"};
                }    // namespace static_strings

                template<class Inspector>
                auto inspect(Inspector &f, log_plan_term_specification::Leader &x) {
                    return f.object(x).fields(f.field(StaticStrings::ServerId, x.serverId),
                                              f.field(StaticStrings::RebootId, x.rebootId));
                }

                template<class Inspector>
                auto inspect(Inspector &f, log_plan_term_specification &x) {
                    return f.object(x).fields(f.field(StaticStrings::Term, x.term),
                                              f.field(StaticStrings::Leader, x.leader));
                }

                template<class Inspector>
                auto inspect(Inspector &f, log_plan_specification &x) {
                    return f.object(x).fields(f.field(StaticStrings::Id, x.id),
                                              f.field(StaticStrings::CurrentTerm, x.currentTerm),
                                              f.field(static_strings::Owner, x.owner),
                                              f.field(static_strings::participants_config, x.participants_config));
                };

                template<class Inspector>
                auto inspect(Inspector &f, log_current_local_state &x) {
                    return f.object(x).fields(f.field(StaticStrings::Term, x.term),
                                              f.field(StaticStrings::Spearhead, x.spearhead));
                }

                template<typename Enum>
                struct EnumStruct {
                    static_assert(std::is_enum_v<Enum>);

                    EnumStruct() : code{0}, message{} {};

                    EnumStruct(Enum e) : code(static_cast<std::underlying_type_t <Enum>>(e)), message(to_string(e)) {};

                    [[nodiscard]] auto get() const -> Enum {
                        return static_cast<Enum>(code);
                    }

                    std::underlying_type_t <Enum> code;
                    std::string message;
                };

                template<class Inspector, typename Enum>
                auto inspect(Inspector &f, EnumStruct<Enum> &x) {
                    return f.object(x).fields(f.field(static_strings::Code, x.code),
                                              f.field(static_strings::Message, x.message));
                };

                template<class Inspector>
                auto inspect(Inspector &f, log_current_supervision_election::ErrorCode &x) {
                    if constexpr(Inspector::isLoading)
                    {
                        auto v = EnumStruct<log_current_supervision_election::ErrorCode>();
                        auto res = f.apply(v);
                        if (res.ok()) {
                            x = v.get();
                        }
                        return res;
                    } else {
                        auto v = EnumStruct<log_current_supervision_election::ErrorCode>(x);
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
                auto inspect(Inspector &f, log_current_supervision::TargetLeaderInvalid &x) {
                    return f.object(x).fields();
                }

                template<class Inspector>
                auto inspect(Inspector &f, log_current_supervision::TargetLeaderExcluded &x) {
                    return f.object(x).fields();
                }

                template<class Inspector>
                auto inspect(Inspector &f, log_current_supervision::TargetLeaderFailed &x) {
                    return f.object(x).fields();
                }

                template<class Inspector>
                auto inspect(Inspector &f, log_current_supervision::TargetNotEnoughParticipants &x) {
                    return f.object(x).fields();
                }

                template<class Inspector>
                auto inspect(Inspector &f, log_current_supervision::WaitingForConfigCommitted &x) {
                    return f.object(x).fields();
                }

                template<class Inspector>
                auto inspect(Inspector &f, log_current_supervision::ConfigChangeNotImplemented &x) {
                    return f.object(x).fields();
                }

                template<class Inspector>
                auto inspect(Inspector &f, log_current_supervision::LeaderElectionImpossible &x) {
                    return f.object(x).fields();
                }

                template<class Inspector>
                auto inspect(Inspector &f, log_current_supervision::LeaderElectionOutOfBounds &x) {
                    return f.object(x).fields();
                }

                template<class Inspector>
                auto inspect(Inspector &f, log_current_supervision::LeaderElectionQuorumNotReached &x) {
                    return f.object(x).fields(f.field("election", x.election));
                }

                template<class Inspector>
                auto inspect(Inspector &f, log_current_supervision::LeaderElectionSuccess &x) {
                    return f.object(x).fields(f.field("election", x.election));
                }

                template<class Inspector>
                auto inspect(Inspector &f, log_current_supervision::SwitchLeaderFailed &x) {
                    return f.object(x).fields();
                }

                template<class Inspector>
                auto inspect(Inspector &f, log_current_supervision::PlanNotAvailable &x) {
                    return f.object(x).fields();
                }

                template<class Inspector>
                auto inspect(Inspector &f, log_current_supervision::CurrentNotAvailable &x) {
                    return f.object(x).fields();
                }

                template<class Inspector>
                auto inspect(Inspector &f, log_current_supervision::StatusMessage &x) {
                    namespace insp = nil::dbms::inspection;
                    return f.variant(x)
                            .qualified("type", "detail")
                            .alternatives(
                                    insp::type<log_current_supervision::TargetLeaderInvalid>(
                                            log_current_supervision::TargetLeaderInvalid::code),
                                    insp::type<log_current_supervision::TargetLeaderExcluded>(
                                            log_current_supervision::TargetLeaderExcluded::code),
                                    insp::type<log_current_supervision::TargetLeaderFailed>(
                                            log_current_supervision::TargetLeaderFailed::code),
                                    insp::type<log_current_supervision::TargetNotEnoughParticipants>(
                                            log_current_supervision::TargetNotEnoughParticipants::code),
                                    insp::type<log_current_supervision::WaitingForConfigCommitted>(
                                            log_current_supervision::WaitingForConfigCommitted::code),
                                    insp::type<log_current_supervision::ConfigChangeNotImplemented>(
                                            log_current_supervision::ConfigChangeNotImplemented::code),
                                    insp::type<log_current_supervision::LeaderElectionImpossible>(
                                            log_current_supervision::LeaderElectionImpossible::code),
                                    insp::type<log_current_supervision::LeaderElectionOutOfBounds>(
                                            log_current_supervision::LeaderElectionOutOfBounds::code),
                                    insp::type<log_current_supervision::LeaderElectionQuorumNotReached>(
                                            log_current_supervision::LeaderElectionQuorumNotReached::code),
                                    insp::type<log_current_supervision::LeaderElectionSuccess>(
                                            log_current_supervision::LeaderElectionSuccess::code),
                                    insp::type<log_current_supervision::SwitchLeaderFailed>(
                                            log_current_supervision::SwitchLeaderFailed::code),
                                    insp::type<log_current_supervision::PlanNotAvailable>(
                                            log_current_supervision::PlanNotAvailable::code),
                                    insp::type<log_current_supervision::CurrentNotAvailable>(
                                            log_current_supervision::CurrentNotAvailable::code));
                }

                template<class Inspector>
                auto inspect(Inspector &f, log_current_supervision &x) {
                    return f.object(x).fields(f.field(static_strings::TargetVersion, x.targetVersion),
                                              f.field(static_strings::StatusReport, x.statusReport),
                                              f.field(static_strings::LastTimeModified, x.lastTimeModified)
                                                      .transformWith(inspection::TimeStampTransformer{}));
                }

                template<class Inspector>
                auto inspect(Inspector &f, LogCurrent::Leader &x) {
                    return f.object(x).fields(
                            f.field(StaticStrings::ServerId, x.serverId),
                            f.field(StaticStrings::Term, x.term),
                            f.field(static_strings::Committedparticipants_config, x.committedparticipants_config),
                            f.field(static_strings::LeadershipEstablished, x.leadershipEstablished).fallback(false),
                            f.field(static_strings::CommitStatus, x.commitStatus));
                }

                template<class Inspector>
                auto inspect(Inspector &f, LogCurrent::ActionDummy &x) {
                    if constexpr(Inspector::isLoading)
                    {
                        x = LogCurrent::ActionDummy{};
                    } else {
                    }
                    return nil::dbms::inspection::Status::Success{};
                }

                template<class Inspector>
                auto inspect(Inspector &f, LogCurrent &x) {
                    return f.object(x).fields(
                            f.field(StaticStrings::LocalStatus, x.localState)
                                    .fallback(std::unordered_map < ParticipantId, log_current_local_state > {}),
                            f.field(static_strings::Supervision, x.supervision),
                            f.field(static_strings::Leader, x.leader),
                            f.field(static_strings::Actions, x.actions).fallback(
                                    std::vector < LogCurrent::ActionDummy > {}));
                }

                template<class Inspector>
                auto inspect(Inspector &f, LogTarget::Supervision &x) {
                    return f.object(x).fields(f.field(static_strings::MaxActionsTraceLength, x.maxActionsTraceLength));
                }

                template<class Inspector>
                auto inspect(Inspector &f, LogTarget &x) {
                    return f.object(x).fields(
                            f.field(StaticStrings::Id, x.id),
                            f.field(StaticStrings::Participants, x.participants).fallback(ParticipantsFlagsMap{}),
                            f.field(StaticStrings::Config, x.config),
                            f.field(StaticStrings::Leader, x.leader),
                            f.field(static_strings::Version, x.version),
                            f.field(static_strings::Owner, x.owner),
                            f.field(static_strings::Supervision, x.supervision));
                }

            }
        }
    }
}    // namespace nil::dbms::replication::agency
