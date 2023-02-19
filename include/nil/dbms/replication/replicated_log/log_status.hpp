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

#include <optional>
#include <unordered_map>
#include <variant>

#include <inspection/vpack.h>
#include <inspection/transformers.h>
#include <velocypack/Builder.h>
#include <velocypack/Slice.h>

#include "types.hpp"
#include "agency_log_specification.hpp"

namespace nil::dbms::replication::replicated_log {

    enum class ParticipantRole { kUnconfigured, kLeader, kFollower };

    /**
     * @brief A minimalist variant of LogStatus, designed to replace FollowerStatus
     * and LeaderStatus where only basic information is needed.
     */
    struct quick_log_status {
        ParticipantRole role {ParticipantRole::kUnconfigured};
        std::optional<log_term> term {};
        std::optional<log_statistics> local {};
        bool leadershipEstablished {false};
        std::optional<commit_fail_reason> commitFailReason {};

        // The following make sense only for a leader.
        std::shared_ptr<agency::participants_config const> activeParticipantsConfig {};
        // Note that committedParticipantsConfig will be nullptr until leadership has
        // been established!
        std::shared_ptr<agency::participants_config const> committedParticipantsConfig {};

        [[nodiscard]] auto getCurrentTerm() const noexcept -> std::optional<log_term>;
        [[nodiscard]] auto getLocalStatistics() const noexcept -> std::optional<log_statistics>;
    };

    auto to_string(ParticipantRole) noexcept -> std::string_view;

    struct participant_role_string_transformer {
        using SerializedType = std::string;
        auto toSerialized(ParticipantRole source, std::string &target) const -> inspection::Status;
        auto fromSerialized(std::string const &source, ParticipantRole &target) const -> inspection::Status;
    };

    template<typename Inspector>
    auto inspect(Inspector &f, quick_log_status &x) {
        auto activeParticipantsConfig = std::shared_ptr<agency::participants_config>();
        auto committedParticipantsConfig = std::shared_ptr<agency::participants_config>();
        if constexpr (!Inspector::isLoading) {
            activeParticipantsConfig = std::make_shared<agency::participants_config>();
            committedParticipantsConfig = std::make_shared<agency::participants_config>();
        }
        auto res = f.object(x).fields(f.field("role", x.role).transformWith(participant_role_string_transformer {}),
                                      f.field("term", x.term), f.field("local", x.local),
                                      f.field("leadershipEstablished", x.leadershipEstablished),
                                      f.field("commitFailReason", x.commitFailReason),
                                      f.field("activeParticipantsConfig", activeParticipantsConfig),
                                      f.field("committedParticipantsConfig", committedParticipantsConfig));
        if constexpr (Inspector::isLoading) {
            x.activeParticipantsConfig = activeParticipantsConfig;
            x.committedParticipantsConfig = committedParticipantsConfig;
        }
        return res;
    }

    struct follower_statistics : log_statistics {
        append_entries_error_reason lastErrorReason;
        std::chrono::duration<double, std::milli> lastRequestLatencyMS;
        follower_state internalState;

        friend auto operator==(follower_statistics const &left, follower_statistics const &right) noexcept -> bool;
        friend auto operator!=(follower_statistics const &left, follower_statistics const &right) noexcept -> bool;
    };

    template<class Inspector>
    auto inspect(Inspector &f, follower_statistics &x) {
        using namespace nil::dbms;
        return f.object(x).fields(
            f.field(StaticStrings::Spearhead, x.spearHead),
            f.field(StaticStrings::CommitIndex, x.commitIndex),
            f.field(StaticStrings::FirstIndex, x.firstIndex),
            f.field(StaticStrings::ReleaseIndex, x.releaseIndex),
            f.field("lastErrorReason", x.lastErrorReason),
            f.field("lastRequestLatencyMS", x.lastRequestLatencyMS)
                .transformWith(inspection::DurationTransformer<std::chrono::duration<double, std::milli>> {}),
            f.field("state", x.internalState));
    }

    [[nodiscard]] auto operator==(follower_statistics const &left, follower_statistics const &right) noexcept -> bool;
    [[nodiscard]] auto operator!=(follower_statistics const &left, follower_statistics const &right) noexcept -> bool;

    struct leader_status {
        log_statistics local;
        log_term term;
        log_index lowestIndexToKeep;
        bool leadershipEstablished {false};
        std::unordered_map<ParticipantId, follower_statistics> follower;
        // now() - insertTP of last uncommitted entry
        std::chrono::duration<double, std::milli> commitLagMS;
        commit_fail_reason lastCommitStatus;
        agency::participants_config activeParticipantsConfig;
        std::optional<agency::participants_config> committedParticipantsConfig;

        bool operator==(const leader_status &rhs) const {
            return local == rhs.local && term == rhs.term && lowestIndexToKeep == rhs.lowestIndexToKeep &&
                   leadershipEstablished == rhs.leadershipEstablished && follower == rhs.follower &&
                   commitLagMS == rhs.commitLagMS && lastCommitStatus == rhs.lastCommitStatus &&
                   activeParticipantsConfig == rhs.activeParticipantsConfig &&
                   committedParticipantsConfig == rhs.committedParticipantsConfig;
        }
        bool operator!=(const leader_status &rhs) const {
            return !(rhs == *this);
        }
    };

    template<class Inspector>
    auto inspect(Inspector &f, leader_status &x) {
        auto role = StaticStrings::Leader;
        return f.object(x).fields(
            f.field("role", role), f.field("local", x.local), f.field("term", x.term),
            f.field("lowestIndexToKeep", x.lowestIndexToKeep),
            f.field("leadershipEstablished", x.leadershipEstablished), f.field("follower", x.follower),
            f.field("commitLagMS", x.commitLagMS)
                .transformWith(inspection::DurationTransformer<std::chrono::duration<double, std::milli>> {}),
            f.field("lastCommitStatus", x.lastCommitStatus),
            f.field("activeParticipantsConfig", x.activeParticipantsConfig),
            f.field("committedParticipantsConfig", x.committedParticipantsConfig));
    }

    struct follower_status {
        log_statistics local;
        std::optional<ParticipantId> leader;
        log_term term;
        log_index lowestIndexToKeep;
    };

    template<class Inspector>
    auto inspect(Inspector &f, follower_status &x) {
        auto role = StaticStrings::Follower;
        return f.object(x).fields(f.field("role", role), f.field("local", x.local), f.field("term", x.term),
                                  f.field("lowestIndexToKeep", x.lowestIndexToKeep), f.field("leader", x.leader));
    }

    struct UnconfiguredStatus { };

    template<class Inspector>
    auto inspect(Inspector &f, UnconfiguredStatus &x) {
        auto role = std::string {"Unconfigured"};
        return f.object(x).fields(f.field("role", role));
    }

    struct log_status {
        using VariantType = std::variant<UnconfiguredStatus, leader_status, follower_status>;

        // default constructs as unconfigured status
        log_status() = default;
        explicit log_status(UnconfiguredStatus) noexcept;
        explicit log_status(leader_status) noexcept;
        explicit log_status(follower_status) noexcept;

        [[nodiscard]] auto getVariant() const noexcept -> VariantType const &;

        [[nodiscard]] auto getCurrentTerm() const noexcept -> std::optional<log_term>;
        [[nodiscard]] auto getLocalStatistics() const noexcept -> std::optional<log_statistics>;

        [[nodiscard]] auto asLeaderStatus() const noexcept -> leader_status const *;
        [[nodiscard]] auto asFollowerStatus() const noexcept -> follower_status const *;

        static auto fromVelocyPack(velocypack::Slice slice) -> log_status;
        void toVelocyPack(velocypack::Builder &builder) const;

    private:
        VariantType _variant;
    };

    /**
     * @brief Provides a more general view of what's currently going on, without
     * completely relying on the leader.
     */
    struct global_status_connection {
        ErrorCode error {0};
        std::string errorMessage;

        void toVelocyPack(velocypack::Builder &) const;
        static auto fromVelocyPack(velocypack::Slice) -> global_status_connection;
    };

    struct global_status {
        enum class SpecificationSource {
            kLocalCache,
            kRemoteAgency,
        };

        struct ParticipantStatus {
            struct Response {

                using VariantType = std::variant<log_status, velocypack::UInt8Buffer>;
                VariantType value;

                void toVelocyPack(velocypack::Builder &) const;
                static auto fromVelocyPack(velocypack::Slice) -> Response;
            };

            global_status_connection connection;
            std::optional<Response> response;

            void toVelocyPack(velocypack::Builder &) const;
            static auto fromVelocyPack(velocypack::Slice) -> ParticipantStatus;
        };

        struct SupervisionStatus {
            global_status_connection connection;
            std::optional<agency::log_current_supervision> response;

            void toVelocyPack(velocypack::Builder &) const;
            static auto fromVelocyPack(velocypack::Slice) -> SupervisionStatus;
        };

        struct Specification {
            SpecificationSource source {SpecificationSource::kLocalCache};
            agency::log_plan_specification plan;

            void toVelocyPack(velocypack::Builder &) const;
            static auto fromVelocyPack(velocypack::Slice) -> Specification;
        };

        SupervisionStatus supervision;
        std::unordered_map<ParticipantId, ParticipantStatus> participants;
        Specification specification;
        std::optional<ParticipantId> leaderId;
        static auto fromVelocyPack(velocypack::Slice slice) -> global_status;
        void toVelocyPack(velocypack::Builder &builder) const;
    };

    auto to_string(global_status::SpecificationSource source) -> std::string_view;

}    // namespace nil::dbms::replication::replicated_log
