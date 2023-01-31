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
#include <nil/dbms/replication/log/log_common.hpp>
#include "inspection/transformers.h"

namespace nil::dbms::futures {
    template<typename T>
    class Future;
}

namespace nil::dbms::velocypack {
    class Builder;
    class Slice;
}    // namespace nil::dbms::velocypack

namespace nil::dbms::replication::log {

    namespace static_strings {
        inline constexpr std::string_view upToDateString = "up-to-date";
        inline constexpr std::string_view errorBackoffString = "error-backoff";
        inline constexpr std::string_view requestInFlightString = "request-in-flight";
    }    // namespace static_strings

    struct FollowerState {
        struct UpToDate { };
        struct ErrorBackoff {
            std::chrono::duration<double, std::milli> durationMS;
            std::size_t retryCount;
        };
        struct RequestInFlight {
            std::chrono::duration<double, std::milli> durationMS;
        };

        std::variant<UpToDate, ErrorBackoff, RequestInFlight> value;

        static auto withUpToDate() noexcept -> FollowerState;
        static auto withErrorBackoff(std::chrono::duration<double, std::milli>, std::size_t retryCount) noexcept
            -> FollowerState;
        static auto withRequestInFlight(std::chrono::duration<double, std::milli>) noexcept -> FollowerState;
        static auto from_velocy_pack(velocypack::Slice) -> FollowerState;
        void to_velocy_pack(velocypack::Builder &) const;

        FollowerState() = default;

    private:
        template<typename... Args>
        explicit FollowerState(std::in_place_t, Args &&...args) : value(std::forward<Args>(args)...) {
        }
    };

    template<class Inspector>
    auto inspect(Inspector &f, FollowerState::UpToDate &x) {
        auto state = std::string {static_strings::upToDateString};
        return f.object(x).fields(f.field("state", state));
    }

    template<class Inspector>
    auto inspect(Inspector &f, FollowerState::ErrorBackoff &x) {
        auto state = std::string {static_strings::errorBackoffString};
        return f.object(x).fields(
            f.field("state", state),
            f.field("durationMS", x.durationMS)
                .transformWith(inspection::DurationTransformer<std::chrono::duration<double, std::milli>> {}),
            f.field("retryCount", x.retryCount));
    }

    template<class Inspector>
    auto inspect(Inspector &f, FollowerState::RequestInFlight &x) {
        auto state = std::string {static_strings::requestInFlightString};
        return f.object(x).fields(
            f.field("state", state),
            f.field("durationMS", x.durationMS)
                .transformWith(inspection::DurationTransformer<std::chrono::duration<double, std::milli>> {}));
    }

    template<class Inspector>
    auto inspect(Inspector &f, FollowerState &x) {
        if constexpr (Inspector::isLoading) {
            x = FollowerState::from_velocy_pack(f.slice());
        } else {
            x.to_velocy_pack(f.builder());
        }
        return nil::dbms::inspection::Status::Success {};
    }

    auto to_string(FollowerState const &) -> std::string_view;

    struct AppendEntriesRequest;
    struct AppendEntriesResult;

#if (defined(__GNUC__) && !defined(__clang__))
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
    struct AppendEntriesErrorReason {
        enum class ErrorType {
            kNone,
            kInvalidLeaderId,
            kLostlog_core,
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
        void to_velocy_pack(velocypack::Builder &builder) const;
        [[nodiscard]] static auto from_velocy_pack(velocypack::Slice slice) -> AppendEntriesErrorReason;
        static auto errorTypeFromString(const std::string &str) -> ErrorType;

        bool operator==(const AppendEntriesErrorReason &rhs) const {
            return error == rhs.error && details == rhs.details;
        }
        bool operator!=(const AppendEntriesErrorReason &rhs) const {
            return !(rhs == *this);
        }
    };
#if (defined(__GNUC__) && !defined(__clang__))
#pragma GCC diagnostic pop
#endif

    [[nodiscard]] auto to_string(AppendEntriesErrorReason::ErrorType error) noexcept -> std::string_view;
    struct AppendEntriesErrorReasonTypeStringTransformer {
        using SerializedType = std::string;
        auto toSerialized(AppendEntriesErrorReason::ErrorType source, std::string &target) const -> inspection::Status;
        auto fromSerialized(std::string const &source, AppendEntriesErrorReason::ErrorType &target) const
            -> inspection::Status;
    };

    template<class Inspector>
    auto inspect(Inspector &f, AppendEntriesErrorReason &x) {
        auto state = std::string {static_strings::requestInFlightString};
        auto errorMessage = std::string {x.getErrorMessage()};
        return f.object(x).fields(
            f.field("details", x.details),
            f.field("errorMessage", errorMessage).fallback(std::string {}),
            f.field("error", x.error).transformWith(AppendEntriesErrorReasonTypeStringTransformer {}));
    }

    struct LogStatistics {
        term_index_pair spearHead {};
        log_index commitIndex {};
        log_index firstIndex {};
        log_index releaseIndex {};

        void to_velocy_pack(velocypack::Builder &builder) const;
        [[nodiscard]] static auto from_velocy_pack(velocypack::Slice slice) -> LogStatistics;

        bool operator==(const LogStatistics &rhs) const {
            return spearHead == rhs.spearHead && commitIndex == rhs.commitIndex && firstIndex == rhs.firstIndex &&
                   releaseIndex == rhs.releaseIndex;
        }
        bool operator!=(const LogStatistics &rhs) const {
            return !(rhs == *this);
        }
    };

    template<class Inspector>
    auto inspect(Inspector &f, LogStatistics &x) {
        using namespace nil::dbms;
        return f.object(x).fields(f.field(StaticStrings::Spearhead, x.spearHead),
                                  f.field(StaticStrings::CommitIndex, x.commitIndex),
                                  f.field(StaticStrings::FirstIndex, x.firstIndex),
                                  f.field(StaticStrings::ReleaseIndex, x.releaseIndex));
    }

    struct AbstractFollower {
        virtual ~AbstractFollower() = default;
        [[nodiscard]] virtual auto getParticipantId() const noexcept -> ParticipantId const & = 0;
        [[nodiscard]] virtual auto appendEntries(AppendEntriesRequest) -> futures::Future<AppendEntriesResult> = 0;
    };

    struct QuorumData {
        QuorumData(log_index index, log_term term, std::vector<ParticipantId> quorum);
        QuorumData(log_index index, log_term term);
        explicit QuorumData(velocypack::Slice slice);

        log_index index;
        log_term term;
        std::vector<ParticipantId> quorum;    // might be empty on follower

        void to_velocy_pack(velocypack::Builder &builder) const;
    };

}    // namespace nil::dbms::replication::log
