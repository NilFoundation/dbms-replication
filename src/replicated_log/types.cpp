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

#include <nil/dbms/replication/replicated_log/types.hpp>

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
using namespace nil::dbms::replication::replicated_log;

replicated_log::quorum_data::quorum_data(log_index index, log_term term, std::vector<ParticipantId> quorum) :
    index(index), term(term), quorum(std::move(quorum)) {
}

replicated_log::quorum_data::quorum_data(log_index index, log_term term) : quorum_data(index, term, {}) {
}

replicated_log::quorum_data::quorum_data(VPackSlice slice) {
    index = slice.get(StaticStrings::Index).extract<log_index>();
    term = slice.get(StaticStrings::Term).extract<log_term>();
    for (auto part : VPackArrayIterator(slice.get("quorum"))) {
        quorum.push_back(part.copyString());
    }
}

void replicated_log::quorum_data::toVelocyPack(velocypack::Builder &builder) const {
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

void replicated_log::log_statistics::toVelocyPack(velocypack::Builder &builder) const {
    serialize(builder, *this);
}

auto replicated_log::log_statistics::fromVelocyPack(velocypack::Slice slice) -> log_statistics {
    return velocypack::deserialize<log_statistics>(slice);
}

auto replicated_log::append_entries_error_reason::getErrorMessage() const noexcept -> std::string_view {
    switch (error) {
        case ErrorType::kNone:
            return "None";
        case ErrorType::kInvalidLeaderId:
            return "Leader id was invalid";
        case ErrorType::kLostLogCore:
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
    LOG_TOPIC("ff21c", FATAL, Logger::REPLICATION2)
        << "Invalid AppendEntriesErrorReason " << static_cast<std::underlying_type_t<decltype(error)>>(error);
    FATAL_ERROR_ABORT();
}

constexpr static std::string_view kNoneString = "None";
constexpr static std::string_view kInvalidLeaderIdString = "InvalidLeaderId";
constexpr static std::string_view kLostLogCoreString = "LostLogCore";
constexpr static std::string_view kMessageOutdatedString = "MessageOutdated";
constexpr static std::string_view kWrongTermString = "WrongTerm";
constexpr static std::string_view kNoPrevLogMatchString = "NoPrevLogMatch";
constexpr static std::string_view kPersistenceFailureString = "PersistenceFailure";
constexpr static std::string_view kCommunicationErrorString = "CommunicationError";
constexpr static std::string_view kPrevAppendEntriesInFlightString = "PrevAppendEntriesInFlight";

auto replicated_log::append_entries_error_reason::errorTypeFromString(const std::string &str) -> ErrorType {
    if (str == kNoneString) {
        return ErrorType::kNone;
    } else if (str == kInvalidLeaderIdString) {
        return ErrorType::kInvalidLeaderId;
    } else if (str == kLostLogCoreString) {
        return ErrorType::kLostLogCore;
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

auto replicated_log::to_string(append_entries_error_reason::ErrorType error) noexcept -> std::string_view {
    switch (error) {
        case append_entries_error_reason::ErrorType::kNone:
            return kNoneString;
        case append_entries_error_reason::ErrorType::kInvalidLeaderId:
            return kInvalidLeaderIdString;
        case append_entries_error_reason::ErrorType::kLostLogCore:
            return kLostLogCoreString;
        case append_entries_error_reason::ErrorType::kMessageOutdated:
            return kMessageOutdatedString;
        case append_entries_error_reason::ErrorType::kWrongTerm:
            return kWrongTermString;
        case append_entries_error_reason::ErrorType::kNoPrevLogMatch:
            return kNoPrevLogMatchString;
        case append_entries_error_reason::ErrorType::kPersistenceFailure:
            return kPersistenceFailureString;
        case append_entries_error_reason::ErrorType::kCommunicationError:
            return kCommunicationErrorString;
        case append_entries_error_reason::ErrorType::kPrevAppendEntriesInFlight:
            return kPrevAppendEntriesInFlightString;
    }
    LOG_TOPIC("c2058", FATAL, Logger::REPLICATION2)
        << "Invalid AppendEntriesErrorReason " << static_cast<std::underlying_type_t<decltype(error)>>(error);
    FATAL_ERROR_ABORT();
}

constexpr static const char *kDetailsString = "details";

void replicated_log::append_entries_error_reason::toVelocyPack(velocypack::Builder &builder) const {
    VPackObjectBuilder ob(&builder);
    builder.add(StaticStrings::Error, VPackValue(to_string(error)));
    builder.add(StaticStrings::ErrorMessage, VPackValue(getErrorMessage()));
    if (details) {
        builder.add(kDetailsString, VPackValue(details.value()));
    }
}

auto replicated_log::append_entries_error_reason::fromVelocyPack(velocypack::Slice slice) -> append_entries_error_reason {
    auto errorSlice = slice.get(StaticStrings::Error);
    TRI_ASSERT(errorSlice.isString()) << "Expected string, found: " << errorSlice.toJson();
    auto error = errorTypeFromString(errorSlice.copyString());

    std::optional<std::string> details;
    if (auto detailsSlice = slice.get(kDetailsString); !detailsSlice.isNone()) {
        details = detailsSlice.copyString();
    }
    return {error, std::move(details)};
}

auto follower_state::with_up_to_date() noexcept -> follower_state {
    return follower_state(std::in_place, UpToDate {});
}

auto follower_state::withErrorBackoff(std::chrono::duration<double, std::milli> duration,
                                     std::size_t retryCount) noexcept -> follower_state {
    return follower_state(std::in_place, ErrorBackoff {duration, retryCount});
}

auto follower_state::withRequestInFlight(std::chrono::duration<double, std::milli> duration) noexcept -> follower_state {
    return follower_state(std::in_place, request_in_flight {duration});
}

auto follower_state::fromVelocyPack(velocypack::Slice slice) -> follower_state {
    auto state = slice.get("state").extract<std::string_view>();
    if (state == static_strings::errorBackoffString) {
        return follower_state {std::in_place, velocypack::deserialize<ErrorBackoff>(slice)};
    } else if (state == static_strings::requestInFlightString) {
        return follower_state {std::in_place, velocypack::deserialize<request_in_flight>(slice)};
    } else {
        return follower_state {std::in_place, velocypack::deserialize<UpToDate>(slice)};
    }
}

void follower_state::toVelocyPack(velocypack::Builder &builder) const {
    std::visit([&](auto const &v) { velocypack::serialize(builder, v); }, value);
}

auto to_string(follower_state const &state) -> std::string_view {
    struct ToStringVisitor {
        auto operator()(follower_state::UpToDate const &) {
            return static_strings::upToDateString;
        }
        auto operator()(follower_state::ErrorBackoff const &err) {
            return static_strings::errorBackoffString;
        }
        auto operator()(follower_state::request_in_flight const &rif) {
            return static_strings::requestInFlightString;
        }
    };

    return std::visit(ToStringVisitor {}, state.value);
}

auto AppendEntriesErrorReasonTypeStringTransformer::toSerialized(append_entries_error_reason::ErrorType source,
                                                                 std::string &target) const -> inspection::Status {
    target = to_string(source);
    return {};
}

auto AppendEntriesErrorReasonTypeStringTransformer::fromSerialized(std::string const &source,
                                                                   append_entries_error_reason::ErrorType &target) const
    -> inspection::Status {
    if (source == kNoneString) {
        target = append_entries_error_reason::ErrorType::kNone;
    } else if (source == kInvalidLeaderIdString) {
        target = append_entries_error_reason::ErrorType::kInvalidLeaderId;
    } else if (source == kLostLogCoreString) {
        target = append_entries_error_reason::ErrorType::kLostLogCore;
    } else if (source == kMessageOutdatedString) {
        target = append_entries_error_reason::ErrorType::kMessageOutdated;
    } else if (source == kWrongTermString) {
        target = append_entries_error_reason::ErrorType::kWrongTerm;
    } else if (source == kNoPrevLogMatchString) {
        target = append_entries_error_reason::ErrorType::kNoPrevLogMatch;
    } else if (source == kPersistenceFailureString) {
        target = append_entries_error_reason::ErrorType::kPersistenceFailure;
    } else if (source == kCommunicationErrorString) {
        target = append_entries_error_reason::ErrorType::kCommunicationError;
    } else if (source == kPrevAppendEntriesInFlightString) {
        target = append_entries_error_reason::ErrorType::kPrevAppendEntriesInFlight;
    } else {
        return inspection::Status {"unknown error type " + source};
    }
    return {};
}
