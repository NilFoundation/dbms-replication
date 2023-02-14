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

#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <variant>
#include <optional>

#include "basics/static_strings.h"
#include <nil/replication_sdk/replicated_log/log_common.hpp>
#include "inspection/transformers.h"

namespace nil::dbms::futures {
    template<typename T>
    class Future;
}

namespace nil::dbms::velocypack {
    class Builder;
    class Slice;
}    // namespace nil::dbms::velocypack

namespace nil::dbms::replication_sdk::replicated_log {

    namespace static_strings {
        inline constexpr std::string_view upToDateString = "up-to-date";
        inline constexpr std::string_view errorBackoffString = "error-backoff";
        inline constexpr std::string_view requestInFlightString = "request-in-flight";
    }    // namespace static_strings

    struct follower_state {
        struct UpToDate { };
        struct ErrorBackoff {
            std::chrono::duration<double, std::milli> durationMS;
            std::size_t retryCount;
        };
        struct request_in_flight {
            std::chrono::duration<double, std::milli> durationMS;
        };

        std::variant<UpToDate, ErrorBackoff, request_in_flight> value;

        static auto with_up_to_date() noexcept -> follower_state;
        static auto withErrorBackoff(std::chrono::duration<double, std::milli>, std::size_t retryCount) noexcept
            -> follower_state;
        static auto withRequestInFlight(std::chrono::duration<double, std::milli>) noexcept -> follower_state;
        static auto fromVelocyPack(velocypack::Slice) -> follower_state;
        void toVelocyPack(velocypack::Builder &) const;

        follower_state() = default;

    private:
        template<typename... Args>
        explicit follower_state(std::in_place_t, Args &&...args) : value(std::forward<Args>(args)...) {
        }
    };

    template<class Inspector>
    auto inspect(Inspector &f, follower_state::UpToDate &x) {
        auto state = std::string {static_strings::upToDateString};
        return f.object(x).fields(f.field("state", state));
    }

    template<class Inspector>
    auto inspect(Inspector &f, follower_state::ErrorBackoff &x) {
        auto state = std::string {static_strings::errorBackoffString};
        return f.object(x).fields(
            f.field("state", state),
            f.field("durationMS", x.durationMS)
                .transformWith(inspection::DurationTransformer<std::chrono::duration<double, std::milli>> {}),
            f.field("retryCount", x.retryCount));
    }

    template<class Inspector>
    auto inspect(Inspector &f, follower_state::request_in_flight &x) {
        auto state = std::string {static_strings::requestInFlightString};
        return f.object(x).fields(
            f.field("state", state),
            f.field("durationMS", x.durationMS)
                .transformWith(inspection::DurationTransformer<std::chrono::duration<double, std::milli>> {}));
    }

    template<class Inspector>
    auto inspect(Inspector &f, follower_state &x) {
        if constexpr (Inspector::isLoading) {
            x = follower_state::fromVelocyPack(f.slice());
        } else {
            x.toVelocyPack(f.builder());
        }
        return nil::dbms::inspection::Status::Success {};
    }

    auto to_string(follower_state const &) -> std::string_view;

    struct append_entries_request;
    struct append_entries_result;

#if (defined(__GNUC__) && !defined(__clang__))
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
    struct append_entries_error_reason {
        enum class ErrorType {
            kNone,
            kInvalidLeaderId,
            kLostLogCore,
            kMessageOutdated,
            kWrongTerm,
            kNoPrevLogMatch,
            kPersistenceFailure,
            kCommunicationError,
            kPrevAppendEntriesInFlight,
        };

        ErrorType error = ErrorType::kNone;
        std::optional<std::string> details = std::nullopt;

        [[nodiscard]] auto getErrorMessage() const noexcept -> std::string_view;
        void toVelocyPack(velocypack::Builder &builder) const;
        [[nodiscard]] static auto fromVelocyPack(velocypack::Slice slice) -> append_entries_error_reason;
        static auto errorTypeFromString(const std::string &str) -> ErrorType;

        bool operator==(const append_entries_error_reason &rhs) const {
            return error == rhs.error && details == rhs.details;
        }
        bool operator!=(const append_entries_error_reason &rhs) const {
            return !(rhs == *this);
        }
    };
#if (defined(__GNUC__) && !defined(__clang__))
#pragma GCC diagnostic pop
#endif

    [[nodiscard]] auto to_string(append_entries_error_reason::ErrorType error) noexcept -> std::string_view;
    struct AppendEntriesErrorReasonTypeStringTransformer {
        using SerializedType = std::string;
        auto toSerialized(append_entries_error_reason::ErrorType source, std::string &target) const -> inspection::Status;
        auto fromSerialized(std::string const &source, append_entries_error_reason::ErrorType &target) const
            -> inspection::Status;
    };

    template<class Inspector>
    auto inspect(Inspector &f, append_entries_error_reason &x) {
        auto state = std::string {static_strings::requestInFlightString};
        auto errorMessage = std::string {x.getErrorMessage()};
        return f.object(x).fields(
            f.field("details", x.details),
            f.field("errorMessage", errorMessage).fallback(std::string {}),
            f.field("error", x.error).transformWith(AppendEntriesErrorReasonTypeStringTransformer {}));
    }

    struct log_statistics {
        term_index_pair spearHead {};
        log_index commitIndex {};
        log_index firstIndex {};
        log_index releaseIndex {};

        void toVelocyPack(velocypack::Builder &builder) const;
        [[nodiscard]] static auto fromVelocyPack(velocypack::Slice slice) -> log_statistics;

        bool operator==(const log_statistics &rhs) const {
            return spearHead == rhs.spearHead && commitIndex == rhs.commitIndex && firstIndex == rhs.firstIndex &&
                   releaseIndex == rhs.releaseIndex;
        }
        bool operator!=(const log_statistics &rhs) const {
            return !(rhs == *this);
        }
    };

    template<class Inspector>
    auto inspect(Inspector &f, log_statistics &x) {
        using namespace nil::dbms;
        return f.object(x).fields(f.field(StaticStrings::Spearhead, x.spearHead),
                                  f.field(StaticStrings::CommitIndex, x.commitIndex),
                                  f.field(StaticStrings::FirstIndex, x.firstIndex),
                                  f.field(StaticStrings::ReleaseIndex, x.releaseIndex));
    }

    struct abstract_follower {
        virtual ~abstract_follower() = default;
        [[nodiscard]] virtual auto getParticipantId() const noexcept -> ParticipantId const & = 0;
        [[nodiscard]] virtual auto appendEntries(append_entries_request) -> futures::Future<append_entries_result> = 0;
    };

    struct quorum_data {
        quorum_data(log_index index, log_term term, std::vector<ParticipantId> quorum);
        quorum_data(log_index index, log_term term);
        explicit quorum_data(velocypack::Slice slice);

        log_index index;
        log_term term;
        std::vector<ParticipantId> quorum;    // might be empty on follower

        void toVelocyPack(velocypack::Builder &builder) const;
    };

}    // namespace nil::dbms::replication_sdk::replicated_log
