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

#include "basics/static_strings.h"
#include <nil/replication_sdk/cluster/cluster_types.hpp>
#include <nil/replication_sdk/replicated_log/log_common.hpp>
#include <nil/replication_sdk/replicated_log/types.hpp>

#include <velocypack/Builder.h>
#include <velocypack/Slice.h>

#include <optional>
#include <type_traits>
#include <utility>

namespace nil::dbms::replication_sdk::agency {

    using ParticipantsFlagsMap = std::unordered_map<ParticipantId, participant_flags>;

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

    struct log_plan_term_specification {
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

        log_plan_term_specification() = default;

        log_plan_term_specification(log_term term, std::optional<Leader>);

        bool operator==(const log_plan_term_specification &rhs) const {
            return term == rhs.term && leader == rhs.leader;
        }
        bool operator!=(const log_plan_term_specification &rhs) const {
            return !(rhs == *this);
        }
    };

    struct log_plan_specification {
        LogId id;
        std::optional<log_plan_term_specification> currentTerm;

        participants_config participantsConfig;

        std::optional<std::string> owner;

        log_plan_specification() = default;

        log_plan_specification(LogId id, std::optional<log_plan_term_specification> term);
        log_plan_specification(LogId id, std::optional<log_plan_term_specification> term,
                             participants_config participantsConfig);

        bool operator==(const log_plan_specification &rhs) const {
            return id == rhs.id && currentTerm == rhs.currentTerm && participantsConfig == rhs.participantsConfig &&
                   owner == rhs.owner;
        }
        bool operator!=(const log_plan_specification &rhs) const {
            return !(rhs == *this);
        }
    };

    struct log_current_local_state {
        log_term term {};
        term_index_pair spearhead {};

        log_current_local_state() = default;
        log_current_local_state(log_term, term_index_pair) noexcept;

        bool operator==(const log_current_local_state &rhs) const {
            return term == rhs.term && spearhead == rhs.spearhead;
        }
        bool operator!=(const log_current_local_state &rhs) const {
            return !(rhs == *this);
        }
    };

    struct log_current_supervision_election {
        // This error code applies to participants, not to
        // the election itself
        enum class ErrorCode { OK = 0, SERVER_NOT_GOOD = 1, TERM_NOT_CONFIRMED = 2, SERVER_EXCLUDED = 3 };

        log_term term;

        term_index_pair bestTermIndex;

        std::size_t participantsRequired {};
        std::size_t participantsAvailable {};
        std::unordered_map<ParticipantId, ErrorCode> detail;
        std::vector<ParticipantId> electibleLeaderSet;

        bool operator==(const log_current_supervision_election &rhs) const {
            return term == rhs.term && bestTermIndex == rhs.bestTermIndex &&
                   participantsRequired == rhs.participantsRequired &&
                   participantsAvailable == rhs.participantsAvailable && detail == rhs.detail &&
                   electibleLeaderSet == rhs.electibleLeaderSet;
        }
        bool operator!=(const log_current_supervision_election &rhs) const {
            return !(rhs == *this);
        }

        log_current_supervision_election() = default;
    };

    auto to_string(log_current_supervision_election::ErrorCode) noexcept -> std::string_view;

    struct log_current_supervision {
        using clock = std::chrono::system_clock;

        struct target_leader_invalid {
            constexpr static const char *code = "TargetLeaderInvalid";

            bool operator==(const target_leader_invalid &rhs) const {
                return true;
            }
        };
        struct target_leader_excluded {
            constexpr static const char *code = "TargetLeaderExcluded";

            bool operator==(const target_leader_excluded &rhs) const {
                return true;
            }
        };
        struct target_leader_failed {
            constexpr static const char *code = "TargetLeaderFailed";

            bool operator==(const target_leader_failed &rhs) const {
                return true;
            }
        };
        struct target_not_enough_participants {
            constexpr static const char *code = "TargetNotEnoughParticipants";

            bool operator==(const target_not_enough_participants &rhs) const {
                return true;
            }
        };
        struct waiting_for_config_committed {
            constexpr static const char *code = "WaitingForConfigCommitted";

            bool operator==(const waiting_for_config_committed &rhs) const {
                return true;
            }
        };
        struct config_change_not_implemented {
            constexpr static const char *code = "ConfigChangeNotImplemented";

            bool operator==(const config_change_not_implemented &rhs) const {
                return true;
            }
        };
        struct leader_election_impossible {
            constexpr static const char *code = "LeaderElectionImpossible";

            bool operator==(const leader_election_impossible &rhs) const {
                return true;
            }
        };
        struct leader_election_out_of_bounds {
            constexpr static const char *code = "LeaderElectionOutOfBounds";

            bool operator==(const leader_election_out_of_bounds &rhs) const {
                return true;
            }
        };
        struct leader_election_quorum_not_reached {
            constexpr static const char *code = "LeaderElectionQuorumNotReached";
            log_current_supervision_election election;

            bool operator==(const leader_election_quorum_not_reached &rhs) const {
                return election == rhs.election;
            }
            bool operator!=(const leader_election_quorum_not_reached &rhs) const {
                return !(rhs == *this);
            }
        };
        struct leader_election_success {
            constexpr static const char *code = "LeaderElectionSuccess";
            log_current_supervision_election election;

            bool operator==(const leader_election_success &rhs) const {
                return election == rhs.election;
            }
            bool operator!=(const leader_election_success &rhs) const {
                return !(rhs == *this);
            }
        };
        struct switch_leader_failed {
            constexpr static const char *code = "SwitchLeaderFailed";

            bool operator==(const switch_leader_failed &rhs) const {
                return true;
            }
        };
        struct plan_not_available {
            constexpr static const char *code = "PlanNotAvailable";

            bool operator==(const plan_not_available &rhs) const {
                return true;
            }
        };
        struct current_not_available {
            constexpr static const char *code = "CurrentNotAvailable";

            bool operator==(const current_not_available &rhs) const {
                return true;
            }
        };

        using StatusMessage =
            std::variant<target_leader_invalid, target_leader_excluded, target_leader_failed,
                         target_not_enough_participants, waiting_for_config_committed, config_change_not_implemented,
                         leader_election_impossible, leader_election_out_of_bounds, leader_election_quorum_not_reached,
                         leader_election_success, switch_leader_failed, plan_not_available, current_not_available>;

        using StatusReport = std::vector<StatusMessage>;

        std::optional<uint64_t> targetVersion;
        std::optional<StatusReport> statusReport;
        std::optional<clock::time_point> lastTimeModified;

        log_current_supervision() = default;

        bool operator==(const log_current_supervision &rhs) const {
            return targetVersion == rhs.targetVersion && statusReport == rhs.statusReport &&
                   lastTimeModified == rhs.lastTimeModified;
        }
        bool operator!=(const log_current_supervision &rhs) const {
            return !(rhs == *this);
        }
    };

    struct log_current {
        std::unordered_map<ParticipantId, log_current_local_state> localState;
        std::optional<log_current_supervision> supervision;

        struct Leader {
            ParticipantId serverId;
            log_term term;
            // optional because the leader might not have committed anything
            std::optional<participants_config> committedParticipantsConfig;
            bool leadershipEstablished;
            // will be set after 5s if leader is unable to establish leadership
            std::optional<replicated_log::commit_fail_reason> commitStatus;

            bool operator==(const Leader &rhs) const {
                return serverId == rhs.serverId && term == rhs.term &&
                       committedParticipantsConfig == rhs.committedParticipantsConfig &&
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

        log_current() = default;

        bool operator==(const log_current &rhs) const {
            return localState == rhs.localState && supervision == rhs.supervision && leader == rhs.leader &&
                   targetVersion == rhs.targetVersion && actions == rhs.actions;
        }
        bool operator!=(const log_current &rhs) const {
            return !(rhs == *this);
        }
    };

    struct log_target_config {
        std::size_t writeConcern = 1;
        std::size_t softWriteConcern = 1;
        bool waitForSync = false;

        log_target_config() noexcept = default;
        log_target_config(std::size_t writeConcern, std::size_t softWriteConcern, bool waitForSync) noexcept;

        bool operator==(const log_target_config &rhs) const {
            return writeConcern == rhs.writeConcern && softWriteConcern == rhs.softWriteConcern &&
                   waitForSync == rhs.waitForSync;
        }
        bool operator!=(const log_target_config &rhs) const {
            return !(rhs == *this);
        }
    };

    template<class Inspector>
    auto inspect(Inspector &f, log_target_config &x) {
        return f.object(x).fields(f.field("writeConcern", x.writeConcern),
                                  f.field("softWriteConcern", x.softWriteConcern).fallback(std::ref(x.writeConcern)),
                                  f.field("waitForSync", x.waitForSync));
    }

    struct log_target {
        LogId id;
        ParticipantsFlagsMap participants;
        log_target_config config;

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

        log_target() = default;

        log_target(LogId id, ParticipantsFlagsMap const &participants, log_target_config const &config);

        bool operator==(const log_target &rhs) const {
            return id == rhs.id && participants == rhs.participants && config == rhs.config && leader == rhs.leader &&
                   version == rhs.version && supervision == rhs.supervision && owner == rhs.owner;
        }
        bool operator!=(const log_target &rhs) const {
            return !(rhs == *this);
        }
    };

    /* Convenience Wrapper */
    struct Log {
        log_target target;

        // These two do not necessarily exist in the Agency
        // so when we're called for a Log these might not
        // exist
        std::optional<log_plan_specification> plan;
        std::optional<log_current> current;

        bool operator==(const Log &rhs) const {
            return target == rhs.target && plan == rhs.plan && current == rhs.current;
        }
        bool operator!=(const Log &rhs) const {
            return !(rhs == *this);
        }
    };
}    // namespace nil::dbms::replication_sdk::agency
