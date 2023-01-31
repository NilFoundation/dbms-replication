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

#include <nil/dbms/replication/log/types.hpp>

#include <basics/exceptions.h>
#include <basics/application_exit.h>
#include <basics/voc_errors.h>
#include <inspection/vpack.h>
#include <logger/LogMacros.h>
#include <velocypack/Builder.h>
#include <velocypack/Iterator.h>

#include <cstddef>
#include <functional>
#include <utility>

using namespace nil::dbms;
using namespace nil::dbms::replication;
using namespace nil::dbms::replication::log;

log::QuorumData::QuorumData(log_index index, log_term term, std::vector<ParticipantId> quorum) :
    index(index), term(term), quorum(std::move(quorum)) {
}

log::QuorumData::QuorumData(log_index index, log_term term) : QuorumData(index, term, {}) {
}

log::QuorumData::QuorumData(VPackSlice slice) {
    index = slice.get(StaticStrings::Index).extract<log_index>();
    term = slice.get(StaticStrings::Term).extract<log_term>();
    for (auto part : VPackArrayIterator(slice.get("quorum"))) {
        quorum.push_back(part.copyString());
    }
}

void log::QuorumData::to_velocy_pack(velocypack::Builder &builder) const {
    VPackObjectBuilder ob(&builder);
    builder.add(StaticStrings::Index, VPackValue(index.value));
    builder.add(StaticStrings::Term, VPackValue(term.value));
    {
        VPackArrayBuilder ab(&builder, "quorum");
        for (auto const &part : quorum) {
            builder.add(VPackValue(part));
        }
    }
}

void log::LogStatistics::to_velocy_pack(velocypack::Builder &builder) const {
    serialize(builder, *this);
}

auto log::LogStatistics::from_velocy_pack(velocypack::Slice slice) -> LogStatistics {
    return velocypack::deserialize<LogStatistics>(slice);
}

auto log::AppendEntriesErrorReason::getErrorMessage() const noexcept -> std::string_view {
    switch (error) {
        case ErrorType::kNone:
            return "None";
        case ErrorType::kInvalidLeaderId:
            return "Leader id was invalid";
        case ErrorType::kLostlog_core:
            return "Term has changed and the internal state was lost";
        case ErrorType::kMessageOutdated:
            return "Message is outdated";
        case ErrorType::kWrongTerm:
            return "Term has changed and the internal state was lost";
        case ErrorType::kNoPrevLogMatch:
            return "Previous log index did not match";
        case ErrorType::kPersistenceFailure:
            return "Persisting the log entries failed";
        case ErrorType::kCommunicationError:
            return "Communicating with participant failed - network error";
        case ErrorType::kPrevAppendEntriesInFlight:
            return "A previous appendEntries request is still in flight";
    }
    LOG_TOPIC("ff21c", FATAL, Logger::replication)
        << "Invalid AppendEntriesErrorReason " << static_cast<std::underlying_type_t<decltype(error)>>(error);
    FATAL_ERROR_ABORT();
}

constexpr static std::string_view kNoneString = "None";
constexpr static std::string_view kInvalidLeaderIdString = "InvalidLeaderId";
constexpr static std::string_view kLostlog_coreString = "Lostlog_core";
constexpr static std::string_view kMessageOutdatedString = "MessageOutdated";
constexpr static std::string_view kWrongTermString = "WrongTerm";
constexpr static std::string_view kNoPrevLogMatchString = "NoPrevLogMatch";
constexpr static std::string_view kPersistenceFailureString = "PersistenceFailure";
constexpr static std::string_view kCommunicationErrorString = "CommunicationError";
constexpr static std::string_view kPrevAppendEntriesInFlightString = "PrevAppendEntriesInFlight";

auto log::AppendEntriesErrorReason::errorTypeFromString(const std::string &str) -> ErrorType {
    if (str == kNoneString) {
        return ErrorType::kNone;
    } else if (str == kInvalidLeaderIdString) {
        return ErrorType::kInvalidLeaderId;
    } else if (str == kLostlog_coreString) {
        return ErrorType::kLostlog_core;
    } else if (str == kMessageOutdatedString) {
        return ErrorType::kMessageOutdated;
    } else if (str == kWrongTermString) {
        return ErrorType::kWrongTerm;
    } else if (str == kNoPrevLogMatchString) {
        return ErrorType::kNoPrevLogMatch;
    } else if (str == kPersistenceFailureString) {
        return ErrorType::kPersistenceFailure;
    } else if (str == kCommunicationErrorString) {
        return ErrorType::kCommunicationError;
    } else if (str == kPrevAppendEntriesInFlightString) {
        return ErrorType::kPrevAppendEntriesInFlight;
    }
    THROW_DBMS_EXCEPTION_FORMAT(TRI_ERROR_BAD_PARAMETER, "unknown error type %*s", str.size(), str.data());
}

auto log::to_string(AppendEntriesErrorReason::ErrorType error) noexcept -> std::string_view {
    switch (error) {
        case AppendEntriesErrorReason::ErrorType::kNone:
            return kNoneString;
        case AppendEntriesErrorReason::ErrorType::kInvalidLeaderId:
            return kInvalidLeaderIdString;
        case AppendEntriesErrorReason::ErrorType::kLostlog_core:
            return kLostlog_coreString;
        case AppendEntriesErrorReason::ErrorType::kMessageOutdated:
            return kMessageOutdatedString;
        case AppendEntriesErrorReason::ErrorType::kWrongTerm:
            return kWrongTermString;
        case AppendEntriesErrorReason::ErrorType::kNoPrevLogMatch:
            return kNoPrevLogMatchString;
        case AppendEntriesErrorReason::ErrorType::kPersistenceFailure:
            return kPersistenceFailureString;
        case AppendEntriesErrorReason::ErrorType::kCommunicationError:
            return kCommunicationErrorString;
        case AppendEntriesErrorReason::ErrorType::kPrevAppendEntriesInFlight:
            return kPrevAppendEntriesInFlightString;
    }
    LOG_TOPIC("c2058", FATAL, Logger::replication)
        << "Invalid AppendEntriesErrorReason " << static_cast<std::underlying_type_t<decltype(error)>>(error);
    FATAL_ERROR_ABORT();
}

constexpr static const char *kDetailsString = "details";

void log::AppendEntriesErrorReason::to_velocy_pack(velocypack::Builder &builder) const {
    VPackObjectBuilder ob(&builder);
    builder.add(StaticStrings::Error, VPackValue(to_string(error)));
    builder.add(StaticStrings::ErrorMessage, VPackValue(getErrorMessage()));
    if (details) {
        builder.add(kDetailsString, VPackValue(details.value()));
    }
}

auto log::AppendEntriesErrorReason::from_velocy_pack(velocypack::Slice slice) -> AppendEntriesErrorReason {
    auto errorSlice = slice.get(StaticStrings::Error);
    TRI_ASSERT(errorSlice.isString()) << "Expected string, found: " << errorSlice.toJson();
    auto error = errorTypeFromString(errorSlice.copyString());

    std::optional<std::string> details;
    if (auto detailsSlice = slice.get(kDetailsString); !detailsSlice.isNone()) {
        details = detailsSlice.copyString();
    }
    return {error, std::move(details)};
}

auto FollowerState::withUpToDate() noexcept -> FollowerState {
    return FollowerState(std::in_place, UpToDate {});
}

auto FollowerState::withErrorBackoff(std::chrono::duration<double, std::milli> duration,
                                     std::size_t retryCount) noexcept -> FollowerState {
    return FollowerState(std::in_place, ErrorBackoff {duration, retryCount});
}

auto FollowerState::withRequestInFlight(std::chrono::duration<double, std::milli> duration) noexcept -> FollowerState {
    return FollowerState(std::in_place, RequestInFlight {duration});
}

auto FollowerState::from_velocy_pack(velocypack::Slice slice) -> FollowerState {
    auto state = slice.get("state").extract<std::string_view>();
    if (state == static_strings::errorBackoffString) {
        return FollowerState {std::in_place, velocypack::deserialize<ErrorBackoff>(slice)};
    } else if (state == static_strings::requestInFlightString) {
        return FollowerState {std::in_place, velocypack::deserialize<RequestInFlight>(slice)};
    } else {
        return FollowerState {std::in_place, velocypack::deserialize<UpToDate>(slice)};
    }
}

void FollowerState::to_velocy_pack(velocypack::Builder &builder) const {
    std::visit([&](auto const &v) { velocypack::serialize(builder, v); }, value);
}

auto to_string(FollowerState const &state) -> std::string_view {
    struct ToStringVisitor {
        auto operator()(FollowerState::UpToDate const &) {
            return static_strings::upToDateString;
        }
        auto operator()(FollowerState::ErrorBackoff const &err) {
            return static_strings::errorBackoffString;
        }
        auto operator()(FollowerState::RequestInFlight const &rif) {
            return static_strings::requestInFlightString;
        }
    };

    return std::visit(ToStringVisitor {}, state.value);
}

auto AppendEntriesErrorReasonTypeStringTransformer::toSerialized(AppendEntriesErrorReason::ErrorType source,
                                                                 std::string &target) const -> inspection::Status {
    target = to_string(source);
    return {};
}

auto AppendEntriesErrorReasonTypeStringTransformer::fromSerialized(std::string const &source,
                                                                   AppendEntriesErrorReason::ErrorType &target) const
    -> inspection::Status {
    if (source == kNoneString) {
        target = AppendEntriesErrorReason::ErrorType::kNone;
    } else if (source == kInvalidLeaderIdString) {
        target = AppendEntriesErrorReason::ErrorType::kInvalidLeaderId;
    } else if (source == kLostlog_coreString) {
        target = AppendEntriesErrorReason::ErrorType::kLostlog_core;
    } else if (source == kMessageOutdatedString) {
        target = AppendEntriesErrorReason::ErrorType::kMessageOutdated;
    } else if (source == kWrongTermString) {
        target = AppendEntriesErrorReason::ErrorType::kWrongTerm;
    } else if (source == kNoPrevLogMatchString) {
        target = AppendEntriesErrorReason::ErrorType::kNoPrevLogMatch;
    } else if (source == kPersistenceFailureString) {
        target = AppendEntriesErrorReason::ErrorType::kPersistenceFailure;
    } else if (source == kCommunicationErrorString) {
        target = AppendEntriesErrorReason::ErrorType::kCommunicationError;
    } else if (source == kPrevAppendEntriesInFlightString) {
        target = AppendEntriesErrorReason::ErrorType::kPrevAppendEntriesInFlight;
    } else {
        return inspection::Status {"unknown error type " + source};
    }
    return {};
}
