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

#include <nil/dbms/replication/log/network_messages.hpp>

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
using namespace nil::dbms::replication::log;

#if (_MSC_VER >= 1)
// suppress false positive warning:
#pragma warning(push)
// function assumed not to throw an exception but does
#pragma warning(disable : 4297)
#endif
AppendEntriesRequest::AppendEntriesRequest(AppendEntriesRequest &&other) noexcept try :
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
    LOG_TOPIC("f8d2e", FATAL, Logger::replication)
        << "Caught an exception when moving an AppendEntriesRequest. This is "
           "fatal, as consistency of persistent and in-memory state can no "
           "longer be guaranteed. The process will terminate now. The exception "
           "was: "
        << ex.what();
    FATAL_ERROR_ABORT();
} catch (...) {
    LOG_TOPIC("12f06", FATAL, Logger::replication)
        << "Caught an exception when moving an AppendEntriesRequest. This is "
           "fatal, as consistency of persistent and in-memory state can no "
           "longer be guaranteed. The process will terminate now.";
    FATAL_ERROR_ABORT();
}
#if (_MSC_VER >= 1)
#pragma warning(pop)
#endif

auto AppendEntriesRequest::operator=(log::AppendEntriesRequest &&other) noexcept
    -> AppendEntriesRequest &try {
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
    LOG_TOPIC("dec5f", FATAL, Logger::replication)
        << "Caught an exception when moving an AppendEntriesRequest. This is "
           "fatal, as consistency of persistent and in-memory state can no "
           "longer be guaranteed. The process will terminate now. The exception "
           "was: "
        << ex.what();
    FATAL_ERROR_ABORT();
} catch (...) {
    LOG_TOPIC("facec", FATAL, Logger::replication)
        << "Caught an exception when moving an AppendEntriesRequest. This is "
           "fatal, as consistency of persistent and in-memory state can no "
           "longer be guaranteed. The process will terminate now.";
    FATAL_ERROR_ABORT();
}

auto log::operator++(MessageId &id) -> MessageId & {
    ++id.value;
    return id;
}

auto log::operator<<(std::ostream &os, MessageId id) -> std::ostream & {
    return os << id.value;
}

auto log::to_string(MessageId id) -> std::string {
    return std::to_string(id.value);
}

MessageId::operator velocypack::Value() const noexcept {
    return velocypack::Value(value);
}

void log::AppendEntriesResult::to_velocy_pack(velocypack::Builder &builder) const {
    {
        velocypack::ObjectBuilder ob(&builder);
        builder.add("term", VPackValue(log_term.value));
        builder.add("errorCode", VPackValue(errorCode));
        builder.add(VPackValue("reason"));
        reason.to_velocy_pack(builder);
        builder.add("messageId", VPackValue(messageId));
        if (conflict.has_value()) {
            TRI_ASSERT(errorCode == TRI_ERROR_REPLICATION_REPLICATED_LOG_APPEND_ENTRIES_REJECTED);
            TRI_ASSERT(reason.error == AppendEntriesErrorReason::ErrorType::kNoPrevLogMatch);
            builder.add(VPackValue("conflict"));
            conflict->to_velocy_pack(builder);
        }
    }
}

auto log::AppendEntriesResult::from_velocy_pack(velocypack::Slice slice) -> AppendEntriesResult {
    auto log_term = slice.get("term").extract<log_term>();
    auto errorCode = ErrorCode {slice.get("errorCode").extract<int>()};
    auto reason = AppendEntriesErrorReason::from_velocy_pack(slice.get("reason"));
    auto messageId = slice.get("messageId").extract<MessageId>();

    if (reason.error == AppendEntriesErrorReason::ErrorType::kNoPrevLogMatch) {
        TRI_ASSERT(errorCode == TRI_ERROR_REPLICATION_REPLICATED_LOG_APPEND_ENTRIES_REJECTED);
        auto conflict = slice.get("conflict");
        TRI_ASSERT(conflict.isObject());
        return AppendEntriesResult {log_term, messageId, term_index_pair::from_velocy_pack(conflict), std::move(reason)};
    }

    TRI_ASSERT(errorCode == TRI_ERROR_NO_ERROR || reason.error != AppendEntriesErrorReason::ErrorType::kNone);
    return AppendEntriesResult {log_term, errorCode, reason, messageId};
}

log::AppendEntriesResult::AppendEntriesResult(log_term log_term, ErrorCode errorCode,
                                                         AppendEntriesErrorReason reason, MessageId id) noexcept :
    log_term(log_term),
    errorCode(errorCode), reason(std::move(reason)), messageId(id) {
    static_assert(std::is_nothrow_move_constructible_v<AppendEntriesErrorReason>);
    TRI_ASSERT(errorCode == TRI_ERROR_NO_ERROR || reason.error != AppendEntriesErrorReason::ErrorType::kNone);
}

log::AppendEntriesResult::AppendEntriesResult(log_term log_term, MessageId id) noexcept :
    AppendEntriesResult(log_term, TRI_ERROR_NO_ERROR, {}, id) {
}

log::AppendEntriesResult::AppendEntriesResult(log_term term, log::MessageId id,
                                                         term_index_pair conflict,
                                                         AppendEntriesErrorReason reason) noexcept :
    AppendEntriesResult(term, TRI_ERROR_REPLICATION_REPLICATED_LOG_APPEND_ENTRIES_REJECTED, std::move(reason), id) {
    static_assert(std::is_nothrow_move_constructible_v<AppendEntriesErrorReason>);
    this->conflict = conflict;
}

auto log::AppendEntriesResult::withConflict(log_term term, log::MessageId id,
                                                       term_index_pair conflict) noexcept
    -> log::AppendEntriesResult {
    return {term, id, conflict, {AppendEntriesErrorReason::ErrorType::kNoPrevLogMatch}};
}

auto log::AppendEntriesResult::withRejection(log_term term, MessageId id,
                                                        AppendEntriesErrorReason reason) noexcept
    -> AppendEntriesResult {
    static_assert(std::is_nothrow_move_constructible_v<AppendEntriesErrorReason>);
    return {term, TRI_ERROR_REPLICATION_REPLICATED_LOG_APPEND_ENTRIES_REJECTED, std::move(reason), id};
}

auto log::AppendEntriesResult::withPersistenceError(log_term term, log::MessageId id,
                                                               Result const &res) noexcept
    -> log::AppendEntriesResult {
    return {term,
            res.errorNumber(),
            {AppendEntriesErrorReason::ErrorType::kPersistenceFailure, std::string {res.errorMessage()}},
            id};
}

auto log::AppendEntriesResult::withOk(log_term term, log::MessageId id) noexcept
    -> log::AppendEntriesResult {
    return {term, id};
}

auto log::AppendEntriesResult::isSuccess() const noexcept -> bool {
    return errorCode == TRI_ERROR_NO_ERROR;
}

void log::AppendEntriesRequest::to_velocy_pack(velocypack::Builder &builder) const {
    {
        velocypack::ObjectBuilder ob(&builder);
        builder.add("leaderTerm", VPackValue(leaderTerm.value));
        builder.add("leaderId", VPackValue(leaderId));
        builder.add(VPackValue("prevLogEntry"));
        prevLogEntry.to_velocy_pack(builder);
        builder.add("leaderCommit", VPackValue(leaderCommit.value));
        builder.add("lowestIndexToKeep", VPackValue(lowestIndexToKeep.value));
        builder.add("messageId", VPackValue(messageId));
        builder.add("waitForSync", VPackValue(waitForSync));
        builder.add("entries", VPackValue(VPackValueType::Array));
        for (auto const &it : entries) {
            it.entry().to_velocy_pack(builder);
        }
        builder.close();    // close entries
    }
}

auto log::AppendEntriesRequest::from_velocy_pack(velocypack::Slice slice) -> AppendEntriesRequest {
    auto leaderTerm = slice.get("leaderTerm").extract<log_term>();
    auto leaderId = ParticipantId {slice.get("leaderId").copyString()};
    auto prevLogEntry = term_index_pair::from_velocy_pack(slice.get("prevLogEntry"));
    auto leaderCommit = slice.get("leaderCommit").extract<log_index>();
    auto largestCommonIndex = slice.get("lowestIndexToKeep").extract<log_index>();
    auto messageId = slice.get("messageId").extract<MessageId>();
    auto waitForSync = slice.get("waitForSync").extract<bool>();
    auto entries = std::invoke([&] {
        auto entriesVp = velocypack::ArrayIterator(slice.get("entries"));
        auto transientEntries = EntryContainer::transient_type {};
        std::transform(entriesVp.begin(), entriesVp.end(), std::back_inserter(transientEntries),
                       [](auto const &it) { return in_memory_logEntry(persisting_log_entry::from_velocy_pack(it)); });
        return std::move(transientEntries).persistent();
    });

    return AppendEntriesRequest {leaderTerm,         leaderId,  prevLogEntry, leaderCommit,
                                 largestCommonIndex, messageId, waitForSync,  std::move(entries)};
}

log::AppendEntriesRequest::AppendEntriesRequest(log_term leaderTerm, ParticipantId leaderId,
                                                           term_index_pair prevLogEntry, log_index leaderCommit,
                                                           log_index lowestIndexToKeep,
                                                           log::MessageId messageId, bool waitForSync,
                                                           EntryContainer entries) :
    leaderTerm(leaderTerm),
    leaderId(std::move(leaderId)), prevLogEntry(prevLogEntry), leaderCommit(leaderCommit),
    lowestIndexToKeep(lowestIndexToKeep), messageId(messageId), entries(std::move(entries)), waitForSync(waitForSync) {
}
