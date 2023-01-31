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

#include "basics/error_code.h"
#include "basics/static_strings.h"
#include "inspection/vpack.h"
#include "inspection/transformers.h"
#include <nil/dbms/replication/log/agency_log_specification.hpp>
#include <nil/dbms/replication/log/log_common.hpp>
#include <nil/dbms/replication/state/state_common.hpp>

#include <chrono>
#include <optional>
#include <string>
#include <unordered_map>

namespace nil::dbms::velocypack {
    class Builder;
    class Slice;
}    // namespace nil::dbms::velocypack

namespace nil::dbms::replication::state::agency {

    namespace static_strings {
        auto const String_Snapshot = velocypack::StringRef {"snapshot"};
        auto const String_Generation = velocypack::StringRef {"generation"};
    }    // namespace static_strings

    struct ImplementationSpec {
        std::string type;

        bool operator==(const ImplementationSpec &rhs) const {
            return type == rhs.type;
        }
        bool operator!=(const ImplementationSpec &rhs) const {
            return !(rhs == *this);
        }
    };

    template<class Inspector>
    auto inspect(Inspector &f, ImplementationSpec &x) {
        return f.object(x).fields(f.field(StaticStrings::IndexType, x.type));
    }

    struct Properties {
        ImplementationSpec implementation;

        bool operator==(const Properties &rhs) const {
            return implementation == rhs.implementation;
        }
        bool operator!=(const Properties &rhs) const {
            return !(rhs == *this);
        }
    };

    template<class Inspector>
    auto inspect(Inspector &f, Properties &x) {
        return f.object(x).fields(f.field("implementation", x.implementation));
    }

    struct Plan {
        log_id id;
        StateGeneration generation;
        Properties properties;
        std::optional<std::string> owner;

        struct Participant {
            StateGeneration generation;

            bool operator==(const Participant &rhs) const {
                return generation == rhs.generation;
            }
            bool operator!=(const Participant &rhs) const {
                return !(rhs == *this);
            }
        };

        std::unordered_map<ParticipantId, Participant> participants;

        bool operator==(const Plan &rhs) const {
            return id == rhs.id && generation == rhs.generation && properties == rhs.properties && owner == rhs.owner &&
                   participants == rhs.participants;
        }
        bool operator!=(const Plan &rhs) const {
            return !(rhs == *this);
        }
    };

    template<class Inspector>
    auto inspect(Inspector &f, Plan &x) {
        return f.object(x).fields(f.field(StaticStrings::Id, x.id),
                                  f.field(static_strings::String_Generation, x.generation),
                                  f.field(StaticStrings::Properties, x.properties),
                                  f.field("owner", x.owner),
                                  f.field(StaticStrings::Participants, x.participants)
                                      .fallback(std::unordered_map<ParticipantId, Plan::Participant> {}));
    }

    template<class Inspector>
    auto inspect(Inspector &f, Plan::Participant &x) {
        return f.object(x).fields(f.field(static_strings::String_Generation, x.generation));
    }

    struct Current {
        struct participant_status {
            StateGeneration generation;
            SnapshotInfo snapshot;    // TODO might become an array later?

            bool operator==(const participant_status &rhs) const {
                return generation == rhs.generation && snapshot == rhs.snapshot;
            }
            bool operator!=(const participant_status &rhs) const {
                return !(rhs == *this);
            }
        };

        std::unordered_map<ParticipantId, participant_status> participants;

        struct Supervision {
            using clock = std::chrono::system_clock;

            std::optional<std::uint64_t> version;

            enum StatusCode {
                kLogNotCreated,
                kLogPlanNotAvailable,
                kLogCurrentNotAvailable,
                kServerSnapshotMissing,
                kInsufficientSnapshotCoverage,
                kLogParticipantNotYetGone,
            };

            struct StatusMessage {
                std::optional<std::string> message;
                StatusCode code;
                std::optional<ParticipantId> participant;

                StatusMessage() = default;
                StatusMessage(StatusCode code, std::optional<ParticipantId> participant) :
                    code(code), participant(std::move(participant)) {
                }

                bool operator==(const StatusMessage &rhs) const {
                    return message == rhs.message && code == rhs.code && participant == rhs.participant;
                }
                bool operator!=(const StatusMessage &rhs) const {
                    return !(rhs == *this);
                }
            };

            using StatusReport = std::vector<StatusMessage>;

            std::optional<StatusReport> statusReport;
            std::optional<clock::time_point> lastTimeModified;

            bool operator==(const Supervision &rhs) const {
                return version == rhs.version && statusReport == rhs.statusReport &&
                       lastTimeModified == rhs.lastTimeModified;
            }
            bool operator!=(const Supervision &rhs) const {
                return !(rhs == *this);
            }
        };

        std::optional<Supervision> supervision;

        bool operator==(const Current &rhs) const {
            return participants == rhs.participants && supervision == rhs.supervision;
        }
        bool operator!=(const Current &rhs) const {
            return !(rhs == *this);
        }
    };

    template<class Inspector>
    auto inspect(Inspector &f, Current &x) {
        return f.object(x).fields(f.field(StaticStrings::Participants, x.participants)
                                      .fallback(std::unordered_map<ParticipantId, Current::participant_status> {}),
                                  f.field("supervision", x.supervision));
    }

    template<class Inspector>
    auto inspect(Inspector &f, Current::participant_status &x) {
        return f.object(x).fields(f.field(static_strings::String_Generation, x.generation),
                                  f.field(static_strings::String_Snapshot, x.snapshot));
    }

    template<class Inspector>
    auto inspect(Inspector &f, Current::Supervision &x) {
        return f.object(x).fields(
            f.field(StaticStrings::Version, x.version),
            f.field("statusReport", x.statusReport),
            f.field("lastTimeModified", x.lastTimeModified).transformWith(inspection::TimeStampTransformer {}));
    }

    using StatusCode = Current::Supervision::StatusCode;
    using StatusMessage = Current::Supervision::StatusMessage;
    using StatusReport = Current::Supervision::StatusReport;

    auto to_string(StatusCode) noexcept -> std::string_view;

    struct StatusCodeStringTransformer {
        using SerializedType = std::string;
        auto toSerialized(StatusCode source, std::string &target) const -> inspection::Status;
        auto fromSerialized(std::string const &source, StatusCode &target) const -> inspection::Status;
    };

    struct Target {
        log_id id;

        Properties properties;

        std::optional<ParticipantId> leader;

        struct Participant {
            bool operator==(Participant const &s2) const noexcept {
                return true;
            }
        };

        std::unordered_map<ParticipantId, Participant> participants;
        nil::dbms::replication::agency::LogTargetConfig config;
        std::optional<std::uint64_t> version;

        struct Supervision { };

        bool operator==(const Target &rhs) const {
            return id == rhs.id && properties == rhs.properties && leader == rhs.leader &&
                   participants == rhs.participants && config == rhs.config && version == rhs.version;
        }
        bool operator!=(const Target &rhs) const {
            return !(rhs == *this);
        }
    };

    template<class Inspector>
    auto inspect(Inspector &f, Target &x) {
        return f.object(x).fields(f.field(StaticStrings::Id, x.id),
                                  f.field(StaticStrings::Properties, x.properties),
                                  f.field(StaticStrings::Leader, x.leader),
                                  f.field(StaticStrings::Participants, x.participants)
                                      .fallback(std::unordered_map<ParticipantId, Target::Participant> {}),
                                  f.field(StaticStrings::Config, x.config),
                                  f.field(StaticStrings::Version, x.version));
    }

    template<class Inspector>
    auto inspect(Inspector &f, Target::Participant &x) {
        return f.object(x).fields();
    }

    struct State {
        Target target;
        std::optional<Plan> plan;
        std::optional<Current> current;

        bool operator==(const State &rhs) const {
            return target == rhs.target && plan == rhs.plan && current == rhs.current;
        }
        bool operator!=(const State &rhs) const {
            return !(rhs == *this);
        }
    };

    template<class Inspector>
    auto inspect(Inspector &f, Current::Supervision::StatusMessage &x) {
        return f.object(x).fields(f.field("message", x.message),
                                  f.field("code", x.code).transformWith(StatusCodeStringTransformer {}),
                                  f.field("participant", x.participant));
    }

}    // namespace nil::dbms::replication::state::agency
