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

#include <nil/dbms/replication/log/types.hpp>
#include <nil/dbms/replication/log/agency_log_specification.hpp>

namespace nil::dbms::replication::log {

    enum class participant_role { kUnconfigured, kLeader, kFollower };

    /**
     * @brief A minimalist variant of log_status, designed to replace FollowerStatus
     * and LeaderStatus where only basic information is needed.
     */
    struct quick_log_status {
        participant_role role {participant_role::kUnconfigured};
        std::optional<log_term> term {};
        std::optional<LogStatistics> local {};
        bool leadershipEstablished {false};
        std::optional<CommitFailReason> commitFailReason {};

        // The following make sense only for a leader.
        std::shared_ptr<agency::participants_config const> activeparticipants_config {};
        // Note that committedparticipants_config will be nullptr until leadership has
        // been established!
        std::shared_ptr<agency::participants_config const> committedparticipants_config {};

        [[nodiscard]] auto get_current_term() const noexcept -> std::optional<log_term>;
        [[nodiscard]] auto get_local_statistics() const noexcept -> std::optional<LogStatistics>;
    };

    auto to_string(participant_role) noexcept -> std::string_view;

    struct participant_roleStringTransformer {
        using SerializedType = std::string;
        auto toSerialized(participant_role source, std::string &target) const -> inspection::Status;
        auto fromSerialized(std::string const &source, participant_role &target) const -> inspection::Status;
    };

    template<typename Inspector>
    auto inspect(Inspector &f, quick_log_status &x) {
        auto activeparticipants_config = std::shared_ptr<agency::participants_config>();
        auto committedparticipants_config = std::shared_ptr<agency::participants_config>();
        if constexpr (!Inspector::isLoading) {
            activeparticipants_config = std::make_shared<agency::participants_config>();
            committedparticipants_config = std::make_shared<agency::participants_config>();
        }
        auto res = f.object(x).fields(f.field("role", x.role).transformWith(participant_roleStringTransformer {}),
                                      f.field("term", x.term), f.field("local", x.local),
                                      f.field("leadershipEstablished", x.leadershipEstablished),
                                      f.field("commitFailReason", x.commitFailReason),
                                      f.field("activeparticipants_config", activeparticipants_config),
                                      f.field("committedparticipants_config", committedparticipants_config));
        if constexpr (Inspector::isLoading) {
            x.activeparticipants_config = activeparticipants_config;
            x.committedparticipants_config = committedparticipants_config;
        }
        return res;
    }

    struct FollowerStatistics : LogStatistics {
        AppendEntriesErrorReason lastErrorReason;
        std::chrono::duration<double, std::milli> lastRequestLatencyMS;
        FollowerState internalState;

        friend auto operator==(FollowerStatistics const &left, FollowerStatistics const &right) noexcept -> bool;
        friend auto operator!=(FollowerStatistics const &left, FollowerStatistics const &right) noexcept -> bool;
    };

    template<class Inspector>
    auto inspect(Inspector &f, FollowerStatistics &x) {
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

    [[nodiscard]] auto operator==(FollowerStatistics const &left, FollowerStatistics const &right) noexcept -> bool;
    [[nodiscard]] auto operator!=(FollowerStatistics const &left, FollowerStatistics const &right) noexcept -> bool;

    struct LeaderStatus {
        LogStatistics local;
        log_term term;
        log_index lowestIndexToKeep;
        bool leadershipEstablished {false};
        std::unordered_map<ParticipantId, FollowerStatistics> follower;
        // now() - insertTP of last uncommitted entry
        std::chrono::duration<double, std::milli> commitLagMS;
        CommitFailReason lastCommitStatus;
        agency::participants_config activeparticipants_config;
        std::optional<agency::participants_config> committedparticipants_config;

        bool operator==(const LeaderStatus &rhs) const {
            return local == rhs.local && term == rhs.term && lowestIndexToKeep == rhs.lowestIndexToKeep &&
                   leadershipEstablished == rhs.leadershipEstablished && follower == rhs.follower &&
                   commitLagMS == rhs.commitLagMS && lastCommitStatus == rhs.lastCommitStatus &&
                   activeparticipants_config == rhs.activeparticipants_config &&
                   committedparticipants_config == rhs.committedparticipants_config;
        }
        bool operator!=(const LeaderStatus &rhs) const {
            return !(rhs == *this);
        }
    };

    template<class Inspector>
    auto inspect(Inspector &f, LeaderStatus &x) {
        auto role = StaticStrings::Leader;
        return f.object(x).fields(
            f.field("role", role), f.field("local", x.local), f.field("term", x.term),
            f.field("lowestIndexToKeep", x.lowestIndexToKeep),
            f.field("leadershipEstablished", x.leadershipEstablished), f.field("follower", x.follower),
            f.field("commitLagMS", x.commitLagMS)
                .transformWith(inspection::DurationTransformer<std::chrono::duration<double, std::milli>> {}),
            f.field("lastCommitStatus", x.lastCommitStatus),
            f.field("activeparticipants_config", x.activeparticipants_config),
            f.field("committedparticipants_config", x.committedparticipants_config));
    }

    struct FollowerStatus {
        LogStatistics local;
        std::optional<ParticipantId> leader;
        log_term term;
        log_index lowestIndexToKeep;
    };

    template<class Inspector>
    auto inspect(Inspector &f, FollowerStatus &x) {
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
        using VariantType = std::variant<UnconfiguredStatus, LeaderStatus, FollowerStatus>;

        // default constructs as unconfigured status
        log_status() = default;
        explicit log_status(UnconfiguredStatus) noexcept;
        explicit log_status(LeaderStatus) noexcept;
        explicit log_status(FollowerStatus) noexcept;

        [[nodiscard]] auto getVariant() const noexcept -> VariantType const &;

        [[nodiscard]] auto get_current_term() const noexcept -> std::optional<log_term>;
        [[nodiscard]] auto get_local_statistics() const noexcept -> std::optional<LogStatistics>;

        [[nodiscard]] auto asLeaderStatus() const noexcept -> LeaderStatus const *;
        [[nodiscard]] auto asFollowerStatus() const noexcept -> FollowerStatus const *;

        static auto from_velocy_pack(velocypack::Slice slice) -> log_status;
        void to_velocy_pack(velocypack::Builder &builder) const;

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

        void to_velocy_pack(velocypack::Builder &) const;
        static auto from_velocy_pack(velocypack::Slice) -> global_status_connection;
    };

    struct global_status {
        enum class SpecificationSource {
            kLocalCache,
            kRemoteAgency,
        };

        struct participant_status {
            struct Response {
                using VariantType = std::variant<log_status, velocypack::UInt8Buffer>;
                VariantType value;

                void to_velocy_pack(velocypack::Builder &) const;
                static auto from_velocy_pack(velocypack::Slice) -> Response;
            };

            global_status_connection connection;
            std::optional<Response> response;

            void to_velocy_pack(velocypack::Builder &) const;
            static auto from_velocy_pack(velocypack::Slice) -> participant_status;
        };

        struct SupervisionStatus {
            global_status_connection connection;
            std::optional<agency::LogCurrentSupervision> response;

            void to_velocy_pack(velocypack::Builder &) const;
            static auto from_velocy_pack(velocypack::Slice) -> SupervisionStatus;
        };

        struct Specification {
            SpecificationSource source {SpecificationSource::kLocalCache};
            agency::LogPlanSpecification plan;

            void to_velocy_pack(velocypack::Builder &) const;
            static auto from_velocy_pack(velocypack::Slice) -> Specification;
        };

        SupervisionStatus supervision;
        std::unordered_map<ParticipantId, participant_status> participants;
        Specification specification;
        std::optional<ParticipantId> leaderId;
        static auto from_velocy_pack(velocypack::Slice slice) -> global_status;
        void to_velocy_pack(velocypack::Builder &builder) const;
    };

    auto to_string(global_status::SpecificationSource source) -> std::string_view;

}    // namespace nil::dbms::replication::log
