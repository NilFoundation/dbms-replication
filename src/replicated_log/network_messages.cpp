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

#include <nil/dbms/replication/replicated_log/network_messages.hpp>

#include <velocypack/Builder.h>
#include <velocypack/Iterator.h>

#include <basics/application_exit.h>
#include <containers/immer_memory_policy.h>
#include <logger/LogMacros.h>
#include <basics/voc_errors.h>
#include <basics/result.h>

#if (_MSC_VER >= 1)
// suppress warnings:
#pragma warning(push)
// conversion from 'size_t' to 'immer::detail::rbts::count_t', possible loss of
// data
#pragma warning(disable : 4267)
// result of 32-bit shift implicitly converted to 64 bits (was 64-bit shift
// intended?)
#pragma warning(disable : 4334)
#endif
#include <immer/flex_vector_transient.hpp>
#if (_MSC_VER >= 1)
#pragma warning(pop)
#endif

using namespace nil::dbms;
using namespace nil::dbms::replication;
using namespace nil::dbms::replication::replicated_log;

#if (_MSC_VER >= 1)
// suppress false positive warning:
#pragma warning(push)
// function assumed not to throw an exception but does
#pragma warning(disable : 4297)
#endif
append_entries_request::append_entries_request(append_entries_request &&other) noexcept try :
    leaderTerm(other.leaderTerm), leaderId(std::move(other.leaderId)), prevLogEntry(other.prevLogEntry),
    leaderCommit(other.leaderCommit), lowestIndexToKeep(other.lowestIndexToKeep), messageId(other.messageId),
    entries(std::move(other.entries)), waitForSync(other.waitForSync) {
    // Note that immer::flex_vector is currently not nothrow move-assignable,
    // though it probably does not throw any exceptions. However, we *need* this
    // to be noexcept, otherwise we cannot keep the persistent and in-memory state
    // in sync.
    //
    // So even if flex_vector could actually throw here, we deliberately add the
    // noexcept!
    //
    // The try/catch is *only* for logging, but *must* terminate (e.g. by
    // rethrowing) the process if an exception is caught.
} catch (std::exception const &ex) {
    LOG_TOPIC("f8d2e", FATAL, Logger::REPLICATION2)
        << "Caught an exception when moving an AppendEntriesRequest. This is "
           "fatal, as consistency of persistent and in-memory state can no "
           "longer be guaranteed. The process will terminate now. The exception "
           "was: "
        << ex.what();
    FATAL_ERROR_ABORT();
} catch (...) {
    LOG_TOPIC("12f06", FATAL, Logger::REPLICATION2)
        << "Caught an exception when moving an AppendEntriesRequest. This is "
           "fatal, as consistency of persistent and in-memory state can no "
           "longer be guaranteed. The process will terminate now.";
    FATAL_ERROR_ABORT();
}
#if (_MSC_VER >= 1)
#pragma warning(pop)
#endif

auto append_entries_request::operator=(replicated_log::append_entries_request &&other) noexcept
    -> append_entries_request &try {
    // Note that immer::flex_vector is currently not nothrow move-assignable,
    // though it probably does not throw any exceptions. However, we *need* this
    // to be noexcept, otherwise we cannot keep the persistent and in-memory state
    // in sync.
    //
    // So even if flex_vector could actually throw here, we deliberately add the
    // noexcept!
    //
    // The try/catch is *only* for logging, but *must* terminate (e.g. by
    // rethrowing) the process if an exception is caught.
    leaderTerm = other.leaderTerm;
    leaderId = std::move(other.leaderId);
    prevLogEntry = other.prevLogEntry;
    leaderCommit = other.leaderCommit;
    lowestIndexToKeep = other.lowestIndexToKeep;
    messageId = other.messageId;
    waitForSync = other.waitForSync;
    entries = std::move(other.entries);

    return *this;
} catch (std::exception const &ex) {
    LOG_TOPIC("dec5f", FATAL, Logger::REPLICATION2)
        << "Caught an exception when moving an AppendEntriesRequest. This is "
           "fatal, as consistency of persistent and in-memory state can no "
           "longer be guaranteed. The process will terminate now. The exception "
           "was: "
        << ex.what();
    FATAL_ERROR_ABORT();
} catch (...) {
    LOG_TOPIC("facec", FATAL, Logger::REPLICATION2)
        << "Caught an exception when moving an AppendEntriesRequest. This is "
           "fatal, as consistency of persistent and in-memory state can no "
           "longer be guaranteed. The process will terminate now.";
    FATAL_ERROR_ABORT();
}

auto replicated_log::operator++(message_id &id) -> message_id & {
    ++id.value;
    return id;
}

auto replicated_log::operator<<(std::ostream &os, message_id id) -> std::ostream & {
    return os << id.value;
}

auto replicated_log::to_string(message_id id) -> std::string {
    return std::to_string(id.value);
}

message_id::operator velocypack::Value() const noexcept {
    return velocypack::Value(value);
}

void replicated_log::append_entries_result::toVelocyPack(velocypack::Builder &builder) const {
    {
        velocypack::ObjectBuilder ob(&builder);
        builder.add("term", VPackValue(logTerm.value));
        builder.add("errorCode", VPackValue(errorCode));
        builder.add(VPackValue("reason"));
        reason.toVelocyPack(builder);
        builder.add("messageId", VPackValue(messageId));
        if (conflict.has_value()) {
            TRI_ASSERT(errorCode == TRI_ERROR_REPLICATION_REPLICATED_LOG_APPEND_ENTRIES_REJECTED);
            TRI_ASSERT(reason.error == append_entries_error_reason::ErrorType::kNoPrevLogMatch);
            builder.add(VPackValue("conflict"));
            conflict->toVelocyPack(builder);
        }
    }
}

auto replicated_log::append_entries_result::fromVelocyPack(velocypack::Slice slice) -> append_entries_result {
    auto logTerm = slice.get("term").extract<log_term>();
    auto errorCode = ErrorCode {slice.get("errorCode").extract<int>()};
    auto reason = append_entries_error_reason::fromVelocyPack(slice.get("reason"));
    auto messageId = slice.get("messageId").extract<message_id>();

    if (reason.error == append_entries_error_reason::ErrorType::kNoPrevLogMatch) {
        TRI_ASSERT(errorCode == TRI_ERROR_REPLICATION_REPLICATED_LOG_APPEND_ENTRIES_REJECTED);
        auto conflict = slice.get("conflict");
        TRI_ASSERT(conflict.isObject());
        return append_entries_result {logTerm, messageId, term_index_pair::fromVelocyPack(conflict), std::move(reason)};
    }

    TRI_ASSERT(errorCode == TRI_ERROR_NO_ERROR || reason.error != append_entries_error_reason::ErrorType::kNone);
    return append_entries_result {logTerm, errorCode, reason, messageId};
}

replicated_log::append_entries_result::append_entries_result(log_term logTerm, ErrorCode errorCode,
                                                             append_entries_error_reason reason, message_id id) noexcept
    :
    logTerm(logTerm),
    errorCode(errorCode), reason(std::move(reason)), messageId(id) {
    static_assert(std::is_nothrow_move_constructible_v<append_entries_error_reason>);
    TRI_ASSERT(errorCode == TRI_ERROR_NO_ERROR || reason.error != append_entries_error_reason::ErrorType::kNone);
}

replicated_log::append_entries_result::append_entries_result(log_term logTerm, message_id id) noexcept :
    append_entries_result(logTerm, TRI_ERROR_NO_ERROR, {}, id) {
}

replicated_log::append_entries_result::append_entries_result(log_term term, replicated_log::message_id id,
                                                             term_index_pair conflict,
                                                             append_entries_error_reason reason) noexcept :
    append_entries_result(term, TRI_ERROR_REPLICATION_REPLICATED_LOG_APPEND_ENTRIES_REJECTED, std::move(reason), id) {
    static_assert(std::is_nothrow_move_constructible_v<append_entries_error_reason>);
    this->conflict = conflict;
}

auto replicated_log::append_entries_result::withConflict(log_term term, replicated_log::message_id id,
                                                         term_index_pair conflict) noexcept
    -> replicated_log::append_entries_result {
    return {term, id, conflict, {append_entries_error_reason::ErrorType::kNoPrevLogMatch}};
}

auto replicated_log::append_entries_result::withRejection(log_term term, message_id id,
                                                          append_entries_error_reason reason) noexcept
    -> append_entries_result {
    static_assert(std::is_nothrow_move_constructible_v<append_entries_error_reason>);
    return {term, TRI_ERROR_REPLICATION_REPLICATED_LOG_APPEND_ENTRIES_REJECTED, std::move(reason), id};
}

auto replicated_log::append_entries_result::withPersistenceError(log_term term, replicated_log::message_id id,
                                                                 Result const &res) noexcept
    -> replicated_log::append_entries_result {
    return {term,
            res.errorNumber(),
            {append_entries_error_reason::ErrorType::kPersistenceFailure, std::string {res.errorMessage()}},
            id};
}

auto replicated_log::append_entries_result::withOk(log_term term, replicated_log::message_id id) noexcept
    -> replicated_log::append_entries_result {
    return {term, id};
}

auto replicated_log::append_entries_result::isSuccess() const noexcept -> bool {
    return errorCode == TRI_ERROR_NO_ERROR;
}

void replicated_log::append_entries_request::toVelocyPack(velocypack::Builder &builder) const {
    {
        velocypack::ObjectBuilder ob(&builder);
        builder.add("leaderTerm", VPackValue(leaderTerm.value));
        builder.add("leaderId", VPackValue(leaderId));
        builder.add(VPackValue("prevLogEntry"));
        prevLogEntry.toVelocyPack(builder);
        builder.add("leaderCommit", VPackValue(leaderCommit.value));
        builder.add("lowestIndexToKeep", VPackValue(lowestIndexToKeep.value));
        builder.add("messageId", VPackValue(messageId));
        builder.add("waitForSync", VPackValue(waitForSync));
        builder.add("entries", VPackValue(VPackValueType::Array));
        for (auto const &it : entries) {
            it.entry().toVelocyPack(builder);
        }
        builder.close();    // close entries
    }
}

auto replicated_log::append_entries_request::fromVelocyPack(velocypack::Slice slice) -> append_entries_request {
    auto leaderTerm = slice.get("leaderTerm").extract<log_term>();
    auto leaderId = ParticipantId {slice.get("leaderId").copyString()};
    auto prevLogEntry = term_index_pair::fromVelocyPack(slice.get("prevLogEntry"));
    auto leaderCommit = slice.get("leaderCommit").extract<log_index>();
    auto largestCommonIndex = slice.get("lowestIndexToKeep").extract<log_index>();
    auto messageId = slice.get("messageId").extract<message_id>();
    auto waitForSync = slice.get("waitForSync").extract<bool>();
    auto entries = std::invoke([&] {
        auto entriesVp = velocypack::ArrayIterator(slice.get("entries"));
        auto transientEntries = EntryContainer::transient_type {};
        std::transform(entriesVp.begin(), entriesVp.end(), std::back_inserter(transientEntries),
                       [](auto const &it) { return in_memory_log_entry(persisting_log_entry::fromVelocyPack(it)); });
        return std::move(transientEntries).persistent();
    });

    return append_entries_request {leaderTerm,         leaderId,  prevLogEntry, leaderCommit,
                                   largestCommonIndex, messageId, waitForSync,  std::move(entries)};
}

replicated_log::append_entries_request::append_entries_request(log_term leaderTerm, ParticipantId leaderId,
                                                               term_index_pair prevLogEntry, log_index leaderCommit,
                                                               log_index lowestIndexToKeep,
                                                               replicated_log::message_id messageId, bool waitForSync,
                                                               EntryContainer entries) :
    leaderTerm(leaderTerm),
    leaderId(std::move(leaderId)), prevLogEntry(prevLogEntry), leaderCommit(leaderCommit),
    lowestIndexToKeep(lowestIndexToKeep), messageId(messageId), entries(std::move(entries)), waitForSync(waitForSync) {
}
