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

#include <nil/dbms/replication/log/log_entries.hpp>
#include "inspection/vpack.h"

#include <basics/static_strings.h>
#include <basics/string_utils.h>

#include <basics/velocypack_helper.h>

using namespace nil::dbms;
using namespace nil::dbms::replication;

auto replication::operator==(log_payload const &left, log_payload const &right) -> bool {
    return nil::dbms::basics::VelocyPackHelper::equal(left.slice(), right.slice(), true);
}

log_payload::log_payload(BufferType buffer) : buffer(std::move(buffer)) {
}

auto log_payload::create_from_slice(velocypack::Slice slice) -> log_payload {
    return log_payload(BufferType {slice.start(), slice.byte_size()});
}

auto log_payload::create_from_string(std::string_view string) -> log_payload {
    VPackBuilder builder;
    builder.add(VPackValue(string));
    return log_payload::create_from_slice(builder.slice());
}

auto log_payload::copy_buffer() const -> velocypack::UInt8Buffer {
    velocypack::UInt8Buffer result;
    result.append(buffer.data(), buffer.size());
    return result;
}

auto log_payload::byte_size() const noexcept -> std::size_t {
    return buffer.size();
}

auto log_payload::slice() const noexcept -> velocypack::Slice {
    return VPackSlice(buffer.data());
}

PersistingLogEntry::PersistingLogEntry(term_index_pair term_index_pair, std::variant<LogMetaPayload, log_payload> payload) :
    _termIndex(term_index_pair), _payload(std::move(payload)) {
}

auto PersistingLogEntry::log_term() const noexcept -> log_term {
    return _termIndex.term;
}

auto PersistingLogEntry::log_index() const noexcept -> log_index {
    return _termIndex.index;
}

auto PersistingLogEntry::log_payload() const noexcept -> log_payload const * {
    return std::get_if<log_payload>(&_payload);
}

void PersistingLogEntry::to_velocy_pack(velocypack::Builder &builder) const {
    builder.openObject();
    builder.add("log_index", velocypack::Value(_termIndex.index.value));
    entriesWithoutIndexto_velocy_pack(builder);
    builder.close();
}

void PersistingLogEntry::to_velocy_pack(velocypack::Builder &builder, PersistingLogEntry::Omitlog_index) const {
    builder.openObject();
    entriesWithoutIndexto_velocy_pack(builder);
    builder.close();
}

void PersistingLogEntry::entriesWithoutIndexto_velocy_pack(velocypack::Builder &builder) const {
    builder.add("log_term", velocypack::Value(_termIndex.term.value));
    if (std::holds_alternative<log_payload>(_payload)) {
        builder.add("payload", std::get<log_payload>(_payload).slice());
    } else {
        TRI_ASSERT(std::holds_alternative<LogMetaPayload>(_payload));
        builder.add(velocypack::Value("meta"));
        std::get<LogMetaPayload>(_payload).to_velocy_pack(builder);
    }
}

auto PersistingLogEntry::from_velocy_pack(velocypack::Slice slice) -> PersistingLogEntry {
    auto const log_term = slice.get("log_term").extract<log_term>();
    auto const log_index = slice.get("log_index").extract<log_index>();
    auto const termIndex = term_index_pair {log_term, log_index};

    if (auto payload = slice.get("payload"); !payload.isNone()) {
        return {termIndex, log_payload::create_from_slice(payload)};
    } else {
        auto meta = slice.get("meta");
        TRI_ASSERT(!meta.isNone());
        return {termIndex, LogMetaPayload::from_velocy_pack(meta)};
    }
}

auto PersistingLogEntry::log_term_index_pair() const noexcept -> term_index_pair {
    return _termIndex;
}

auto PersistingLogEntry::approxbyte_size() const noexcept -> std::size_t {
    auto size = approxMetaDataSize;

    if (std::holds_alternative<log_payload>(_payload)) {
        return std::get<log_payload>(_payload).byte_size();
    }

    return size;
}

PersistingLogEntry::PersistingLogEntry(log_index index, velocypack::Slice persisted) {
    _termIndex.index = index;
    _termIndex.term = persisted.get("log_term").extract<log_term>();
    if (auto payload = persisted.get("payload"); !payload.isNone()) {
        _payload = log_payload::create_from_slice(payload);
    } else {
        auto meta = persisted.get("meta");
        TRI_ASSERT(!meta.isNone());
        _payload = LogMetaPayload::from_velocy_pack(meta);
    }
}

auto PersistingLogEntry::hasPayload() const noexcept -> bool {
    return std::holds_alternative<log_payload>(_payload);
}
auto PersistingLogEntry::hasMeta() const noexcept -> bool {
    return std::holds_alternative<LogMetaPayload>(_payload);
}

auto PersistingLogEntry::meta() const noexcept -> const LogMetaPayload * {
    return std::get_if<LogMetaPayload>(&_payload);
}

InMemoryLogEntry::InMemoryLogEntry(PersistingLogEntry entry, bool waitForSync) :
    _waitForSync(waitForSync), _logEntry(std::move(entry)) {
}

void InMemoryLogEntry::setInsertTp(clock::time_point tp) noexcept {
    _insertTp = tp;
}

auto InMemoryLogEntry::insertTp() const noexcept -> clock::time_point {
    return _insertTp;
}

auto InMemoryLogEntry::entry() const noexcept -> PersistingLogEntry const & {
    // Note that while get() isn't marked as noexcept, it actually is.
    return _logEntry.get();
}

LogEntryView::LogEntryView(log_index index, log_payload const &payload) noexcept :
    _index(index), _payload(payload.slice()) {
}

LogEntryView::LogEntryView(log_index index, velocypack::Slice payload) noexcept : _index(index), _payload(payload) {
}

auto LogEntryView::log_index() const noexcept -> log_index {
    return _index;
}

auto LogEntryView::log_payload() const noexcept -> velocypack::Slice {
    return _payload;
}

void LogEntryView::to_velocy_pack(velocypack::Builder &builder) const {
    auto og = velocypack::ObjectBuilder(&builder);
    builder.add("log_index", velocypack::Value(_index));
    builder.add("payload", _payload);
}

auto LogEntryView::from_velocy_pack(velocypack::Slice slice) -> LogEntryView {
    return LogEntryView(slice.get("log_index").extract<log_index>(), slice.get("payload"));
}

auto LogEntryView::clonePayload() const -> log_payload {
    return log_payload::create_from_slice(_payload);
}

namespace {
    constexpr std::string_view StringFirstIndexOfTerm = "FirstIndexOfTerm";
    constexpr std::string_view StringUpdateparticipants_config = "Updateparticipants_config";
}    // namespace

auto LogMetaPayload::from_velocy_pack(velocypack::Slice s) -> LogMetaPayload {
    auto typeSlice = s.get(StaticStrings::IndexType);
    if (typeSlice.toString() == StringFirstIndexOfTerm) {
        return {FirstEntryOfTerm::from_velocy_pack(s)};
    } else if (typeSlice.toString() == StringUpdateparticipants_config) {
        return {Updateparticipants_config::from_velocy_pack(s)};
    } else {
        TRI_ASSERT(false);
        THROW_DBMS_EXCEPTION(TRI_ERROR_BAD_PARAMETER);
    }
}

void LogMetaPayload::to_velocy_pack(velocypack::Builder &builder) const {
    std::visit([&](auto const &v) { v.to_velocy_pack(builder); }, info);
}

void LogMetaPayload::FirstEntryOfTerm::to_velocy_pack(velocypack::Builder &b) const {
    velocypack::ObjectBuilder ob(&b);
    b.add(StaticStrings::IndexType, velocypack::Value(StringFirstIndexOfTerm));
    b.add(StaticStrings::Leader, velocypack::Value(leader));
    b.add(velocypack::Value(StaticStrings::Participants));
    velocypack::serialize(b, participants);
}

void LogMetaPayload::Updateparticipants_config::to_velocy_pack(velocypack::Builder &b) const {
    velocypack::ObjectBuilder ob(&b);
    b.add(StaticStrings::IndexType, velocypack::Value(StringUpdateparticipants_config));
    b.add(velocypack::Value(StaticStrings::Participants));
    velocypack::serialize(b, participants);
}

auto LogMetaPayload::Updateparticipants_config::from_velocy_pack(velocypack::Slice s) -> Updateparticipants_config {
    TRI_ASSERT(s.get(StaticStrings::IndexType).toString() == StringUpdateparticipants_config);
    auto participants = velocypack::deserialize<agency::participants_config>(s.get(StaticStrings::Participants));
    return Updateparticipants_config {std::move(participants)};
}

auto LogMetaPayload::FirstEntryOfTerm::from_velocy_pack(velocypack::Slice s) -> FirstEntryOfTerm {
    TRI_ASSERT(s.get(StaticStrings::IndexType).toString() == StringFirstIndexOfTerm);
    auto leader = s.get(StaticStrings::Leader).copyString();
    auto participants = velocypack::deserialize<agency::participants_config>(s.get(StaticStrings::Participants));
    return FirstEntryOfTerm {std::move(leader), std::move(participants)};
}

auto LogMetaPayload::withFirstEntryOfTerm(ParticipantId leader, agency::participants_config config) -> LogMetaPayload {
    return LogMetaPayload {FirstEntryOfTerm {.leader = std::move(leader), .participants = std::move(config)}};
}

auto LogMetaPayload::withUpdateparticipants_config(agency::participants_config config) -> LogMetaPayload {
    return LogMetaPayload {Updateparticipants_config {.participants = std::move(config)}};
}
