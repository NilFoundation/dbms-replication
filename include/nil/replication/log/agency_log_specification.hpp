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

#include <nil/dbms/agency/agency_paths.hpp>
#include "basics/static_strings.h"
#include <nil/dbms/cluster/cluster_types.hpp>
#include <nil/dbms/replication/log/log_common.hpp>
#include <nil/dbms/replication/log/types.hpp>

#include <velocypack/Builder.h>
#include <velocypack/Slice.h>

#include <optional>
#include <type_traits>
#include <utility>

namespace nil::dbms::replication::agency {

    using ParticipantsFlagsMap = std::unordered_map<ParticipantId, ParticipantFlags>;

    struct log_plan_config {
        std::size_t effectiveWriteConcern = 1;
        bool waitForSync = false;

        log_plan_config() noexcept = default;
        log_plan_config(std::size_t effectiveWriteConcern, bool waitForSync) noexcept;
        log_plan_config(std::size_t writeConcern, std::size_t softWriteConcern, bool waitForSync) noexcept;

        bool operator==(const log_plan_config &rhs) const {
            return effectiveWriteConcern == rhs.effectiveWriteConcern && waitForSync == rhs.waitForSync;
        }
        bool operator!=(const log_plan_config &rhs) const {
            return !(rhs == *this);
        }
    };

    template<class Inspector>
    auto inspect(Inspector &f, log_plan_config &x) {
        return f.object(x).fields(f.field("effectiveWriteConcern", x.effectiveWriteConcern),
                                  f.field("waitForSync", x.waitForSync));
    }

    struct participants_config {
        std::size_t generation = 0;
        ParticipantsFlagsMap participants;
        log_plan_config config;

        bool operator==(const participants_config &rhs) const {
            return generation == rhs.generation && participants == rhs.participants && config == rhs.config;
        }
        bool operator!=(const participants_config &rhs) const {
            return !(rhs == *this);
        }
    };

    template<class Inspector>
    auto inspect(Inspector &f, participants_config &x) {
        return f.object(x).fields(f.field("generation", x.generation), f.field("config", x.config),
                                  f.field("participants", x.participants));
    }

    struct LogPlanTermSpecification {
        log_term term;
        struct Leader {
            ParticipantId serverId;
            RebootId rebootId;

            Leader(ParticipantId participant, RebootId rebootId) :
                serverId {std::move(participant)}, rebootId {rebootId} {
            }
            Leader() : rebootId {RebootId {0}} {};

            bool operator==(const Leader &rhs) const {
                return serverId == rhs.serverId && rebootId == rhs.rebootId;
            }
            bool operator!=(const Leader &rhs) const {
                return !(rhs == *this);
            }
        };
        std::optional<Leader> leader;

        LogPlanTermSpecification() = default;

        LogPlanTermSpecification(log_term term, std::optional<Leader>);

        bool operator==(const LogPlanTermSpecification &rhs) const {
            return term == rhs.term && leader == rhs.leader;
        }
        bool operator!=(const LogPlanTermSpecification &rhs) const {
            return !(rhs == *this);
        }
    };

    struct LogPlanSpecification {
        log_id id;
        std::optional<LogPlanTermSpecification> currentTerm;

        participants_config participants_config;

        std::optional<std::string> owner;

        LogPlanSpecification() = default;

        LogPlanSpecification(log_id id, std::optional<LogPlanTermSpecification> term);
        LogPlanSpecification(log_id id, std::optional<LogPlanTermSpecification> term,
                             participants_config participants_config);

        bool operator==(const LogPlanSpecification &rhs) const {
            return id == rhs.id && currentTerm == rhs.currentTerm && participants_config == rhs.participants_config &&
                   owner == rhs.owner;
        }
        bool operator!=(const LogPlanSpecification &rhs) const {
            return !(rhs == *this);
        }
    };

    struct LogCurrentLocalState {
        log_term term {};
        term_index_pair spearhead {};

        LogCurrentLocalState() = default;
        LogCurrentLocalState(log_term, term_index_pair) noexcept;

        bool operator==(const LogCurrentLocalState &rhs) const {
            return term == rhs.term && spearhead == rhs.spearhead;
        }
        bool operator!=(const LogCurrentLocalState &rhs) const {
            return !(rhs == *this);
        }
    };

    struct LogCurrentSupervisionElection {
        // This error code applies to participants, not to
        // the election itself
        enum class ErrorCode { OK = 0, SERVER_NOT_GOOD = 1, TERM_NOT_CONFIRMED = 2, SERVER_EXCLUDED = 3 };

        log_term term;

        term_index_pair bestTermIndex;

        std::size_t participantsRequired {};
        std::size_t participantsAvailable {};
        std::unordered_map<ParticipantId, ErrorCode> detail;
        std::vector<ParticipantId> electibleLeaderSet;

        bool operator==(const LogCurrentSupervisionElection &rhs) const {
            return term == rhs.term && bestTermIndex == rhs.bestTermIndex &&
                   participantsRequired == rhs.participantsRequired &&
                   participantsAvailable == rhs.participantsAvailable && detail == rhs.detail &&
                   electibleLeaderSet == rhs.electibleLeaderSet;
        }
        bool operator!=(const LogCurrentSupervisionElection &rhs) const {
            return !(rhs == *this);
        }

        LogCurrentSupervisionElection() = default;
    };

    auto to_string(LogCurrentSupervisionElection::ErrorCode) noexcept -> std::string_view;

    struct LogCurrentSupervision {
        using clock = std::chrono::system_clock;

        struct TargetLeaderInvalid {
            constexpr static const char *code = "TargetLeaderInvalid";

            bool operator==(const TargetLeaderInvalid &rhs) const {
                return true;
            }
        };
        struct TargetLeaderExcluded {
            constexpr static const char *code = "TargetLeaderExcluded";

            bool operator==(const TargetLeaderExcluded &rhs) const {
                return true;
            }
        };
        struct TargetLeaderFailed {
            constexpr static const char *code = "TargetLeaderFailed";

            bool operator==(const TargetLeaderFailed &rhs) const {
                return true;
            }
        };
        struct TargetNotEnoughParticipants {
            constexpr static const char *code = "TargetNotEnoughParticipants";

            bool operator==(const TargetNotEnoughParticipants &rhs) const {
                return true;
            }
        };
        struct WaitingForConfigCommitted {
            constexpr static const char *code = "WaitingForConfigCommitted";

            bool operator==(const WaitingForConfigCommitted &rhs) const {
                return true;
            }
        };
        struct ConfigChangeNotImplemented {
            constexpr static const char *code = "ConfigChangeNotImplemented";

            bool operator==(const ConfigChangeNotImplemented &rhs) const {
                return true;
            }
        };
        struct LeaderElectionImpossible {
            constexpr static const char *code = "LeaderElectionImpossible";

            bool operator==(const LeaderElectionImpossible &rhs) const {
                return true;
            }
        };
        struct LeaderElectionOutOfBounds {
            constexpr static const char *code = "LeaderElectionOutOfBounds";

            bool operator==(const LeaderElectionOutOfBounds &rhs) const {
                return true;
            }
        };
        struct LeaderElectionQuorumNotReached {
            constexpr static const char *code = "LeaderElectionQuorumNotReached";
            LogCurrentSupervisionElection election;

            bool operator==(const LeaderElectionQuorumNotReached &rhs) const {
                return election == rhs.election;
            }
            bool operator!=(const LeaderElectionQuorumNotReached &rhs) const {
                return !(rhs == *this);
            }
        };
        struct LeaderElectionSuccess {
            constexpr static const char *code = "LeaderElectionSuccess";
            LogCurrentSupervisionElection election;

            bool operator==(const LeaderElectionSuccess &rhs) const {
                return election == rhs.election;
            }
            bool operator!=(const LeaderElectionSuccess &rhs) const {
                return !(rhs == *this);
            }
        };
        struct SwitchLeaderFailed {
            constexpr static const char *code = "SwitchLeaderFailed";

            bool operator==(const SwitchLeaderFailed &rhs) const {
                return true;
            }
        };
        struct PlanNotAvailable {
            constexpr static const char *code = "PlanNotAvailable";

            bool operator==(const PlanNotAvailable &rhs) const {
                return true;
            }
        };
        struct CurrentNotAvailable {
            constexpr static const char *code = "CurrentNotAvailable";

            bool operator==(const CurrentNotAvailable &rhs) const {
                return true;
            }
        };

        using StatusMessage =
            std::variant<TargetLeaderInvalid, TargetLeaderExcluded, TargetLeaderFailed, TargetNotEnoughParticipants,
                         WaitingForConfigCommitted, ConfigChangeNotImplemented, LeaderElectionImpossible,
                         LeaderElectionOutOfBounds, LeaderElectionQuorumNotReached, LeaderElectionSuccess,
                         SwitchLeaderFailed, PlanNotAvailable, CurrentNotAvailable>;

        using StatusReport = std::vector<StatusMessage>;

        std::optional<uint64_t> targetVersion;
        std::optional<StatusReport> statusReport;
        std::optional<clock::time_point> lastTimeModified;

        LogCurrentSupervision() = default;

        bool operator==(const LogCurrentSupervision &rhs) const {
            return targetVersion == rhs.targetVersion && statusReport == rhs.statusReport &&
                   lastTimeModified == rhs.lastTimeModified;
        }
        bool operator!=(const LogCurrentSupervision &rhs) const {
            return !(rhs == *this);
        }
    };

    struct LogCurrent {
        std::unordered_map<ParticipantId, LogCurrentLocalState> localState;
        std::optional<LogCurrentSupervision> supervision;

        struct Leader {
            ParticipantId serverId;
            log_term term;
            // optional because the leader might not have committed anything
            std::optional<participants_config> committedparticipants_config;
            bool leadershipEstablished;
            // will be set after 5s if leader is unable to establish leadership
            std::optional<log::CommitFailReason> commitStatus;

            bool operator==(const Leader &rhs) const {
                return serverId == rhs.serverId && term == rhs.term &&
                       committedparticipants_config == rhs.committedparticipants_config &&
                       leadershipEstablished == rhs.leadershipEstablished && commitStatus == rhs.commitStatus;
            }
            bool operator!=(const Leader &rhs) const {
                return !(rhs == *this);
            }
        };

        // Will be nullopt until a leader has been assumed leadership
        std::optional<Leader> leader;
        std::optional<std::uint64_t> targetVersion;

        // Temporary hack until Actions are de-serializable.
        struct ActionDummy {
            std::string timestamp;

            bool operator==(const ActionDummy &rhs) const {
                return timestamp == rhs.timestamp;
            }
            bool operator!=(const ActionDummy &rhs) const {
                return !(rhs == *this);
            }
        };
        std::vector<ActionDummy> actions;

        LogCurrent() = default;

        bool operator==(const LogCurrent &rhs) const {
            return localState == rhs.localState && supervision == rhs.supervision && leader == rhs.leader &&
                   targetVersion == rhs.targetVersion && actions == rhs.actions;
        }
        bool operator!=(const LogCurrent &rhs) const {
            return !(rhs == *this);
        }
    };

    struct LogTargetConfig {
        std::size_t writeConcern = 1;
        std::size_t softWriteConcern = 1;
        bool waitForSync = false;

        LogTargetConfig() noexcept = default;
        LogTargetConfig(std::size_t writeConcern, std::size_t softWriteConcern, bool waitForSync) noexcept;

        bool operator==(const LogTargetConfig &rhs) const {
            return writeConcern == rhs.writeConcern && softWriteConcern == rhs.softWriteConcern &&
                   waitForSync == rhs.waitForSync;
        }
        bool operator!=(const LogTargetConfig &rhs) const {
            return !(rhs == *this);
        }
    };

    template<class Inspector>
    auto inspect(Inspector &f, LogTargetConfig &x) {
        return f.object(x).fields(f.field("writeConcern", x.writeConcern),
                                  f.field("softWriteConcern", x.softWriteConcern).fallback(std::ref(x.writeConcern)),
                                  f.field("waitForSync", x.waitForSync));
    }

    struct LogTarget {
        log_id id;
        ParticipantsFlagsMap participants;
        LogTargetConfig config;

        std::optional<ParticipantId> leader;
        std::optional<uint64_t> version;

        struct Supervision {
            std::size_t maxActionsTraceLength {0};

            bool operator==(const Supervision &rhs) const {
                return maxActionsTraceLength == rhs.maxActionsTraceLength;
            }
            bool operator!=(const Supervision &rhs) const {
                return !(rhs == *this);
            }
        };

        std::optional<Supervision> supervision;
        std::optional<std::string> owner;

        LogTarget() = default;

        LogTarget(log_id id, ParticipantsFlagsMap const &participants, LogTargetConfig const &config);

        bool operator==(const LogTarget &rhs) const {
            return id == rhs.id && participants == rhs.participants && config == rhs.config && leader == rhs.leader &&
                   version == rhs.version && supervision == rhs.supervision && owner == rhs.owner;
        }
        bool operator!=(const LogTarget &rhs) const {
            return !(rhs == *this);
        }
    };

    /* Convenience Wrapper */
    struct Log {
        LogTarget target;

        // These two do not necessarily exist in the Agency
        // so when we're called for a Log these might not
        // exist
        std::optional<LogPlanSpecification> plan;
        std::optional<LogCurrent> current;

        bool operator==(const Log &rhs) const {
            return target == rhs.target && plan == rhs.plan && current == rhs.current;
        }
        bool operator!=(const Log &rhs) const {
            return !(rhs == *this);
        }
    };
}    // namespace nil::dbms::replication::agency
