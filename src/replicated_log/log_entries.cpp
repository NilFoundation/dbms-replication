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

#include <nil/dbms/replication/replicated_log/log_entries.hpp>
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
    return log_payload(BufferType {slice.start(), slice.byteSize()});
}

auto log_payload::create_from_string(std::string_view string) -> log_payload {
    VPackBuilder builder;
    builder.add(VPackValue(string));
    return log_payload::create_from_slice(builder.slice());
}

auto log_payload::copyBuffer() const -> velocypack::UInt8Buffer {
    velocypack::UInt8Buffer result;
    result.append(buffer.data(), buffer.size());
    return result;
}

auto log_payload::byteSize() const noexcept -> std::size_t {
    return buffer.size();
}

auto log_payload::slice() const noexcept -> velocypack::Slice {
    return VPackSlice(buffer.data());
}

persisting_log_entry::persisting_log_entry(term_index_pair termIndexPair,
                                           std::variant<log_meta_payload, log_payload> payload) :
    _termIndex(termIndexPair),
    _payload(std::move(payload)) {
}

auto persisting_log_entry::logTerm() const noexcept -> log_term {
    return _termIndex.term;
}

auto persisting_log_entry::logIndex() const noexcept -> log_index {
    return _termIndex.index;
}

auto persisting_log_entry::logPayload() const noexcept -> log_payload const * {
    return std::get_if<log_payload>(&_payload);
}

void persisting_log_entry::toVelocyPack(velocypack::Builder &builder) const {
    builder.openObject();
    builder.add("logIndex", velocypack::Value(_termIndex.index.value));
    entriesWithoutIndexToVelocyPack(builder);
    builder.close();
}

void persisting_log_entry::toVelocyPack(velocypack::Builder &builder, persisting_log_entry::OmitLogIndex) const {
    builder.openObject();
    entriesWithoutIndexToVelocyPack(builder);
    builder.close();
}

void persisting_log_entry::entriesWithoutIndexToVelocyPack(velocypack::Builder &builder) const {
    builder.add("logTerm", velocypack::Value(_termIndex.term.value));
    if (std::holds_alternative<log_payload>(_payload)) {
        builder.add("payload", std::get<log_payload>(_payload).slice());
    } else {
        TRI_ASSERT(std::holds_alternative<log_meta_payload>(_payload));
        builder.add(velocypack::Value("meta"));
        std::get<log_meta_payload>(_payload).toVelocyPack(builder);
    }
}

auto persisting_log_entry::fromVelocyPack(velocypack::Slice slice) -> persisting_log_entry {
    auto const logTerm = slice.get("logTerm").extract<log_term>();
    auto const logIndex = slice.get("logIndex").extract<log_index>();
    auto const termIndex = term_index_pair {logTerm, logIndex};

    if (auto payload = slice.get("payload"); !payload.isNone()) {
        return {termIndex, log_payload::create_from_slice(payload)};
    } else {
        auto meta = slice.get("meta");
        TRI_ASSERT(!meta.isNone());
        return {termIndex, log_meta_payload::fromVelocyPack(meta)};
    }
}

auto persisting_log_entry::logTermIndexPair() const noexcept -> term_index_pair {
    return _termIndex;
}

auto persisting_log_entry::approxByteSize() const noexcept -> std::size_t {
    auto size = approxMetaDataSize;

    if (std::holds_alternative<log_payload>(_payload)) {
        return std::get<log_payload>(_payload).byteSize();
    }

    return size;
}

persisting_log_entry::persisting_log_entry(log_index index, velocypack::Slice persisted) {
    _termIndex.index = index;
    _termIndex.term = persisted.get("logTerm").extract<log_term>();
    if (auto payload = persisted.get("payload"); !payload.isNone()) {
        _payload = log_payload::create_from_slice(payload);
    } else {
        auto meta = persisted.get("meta");
        TRI_ASSERT(!meta.isNone());
        _payload = log_meta_payload::fromVelocyPack(meta);
    }
}

auto persisting_log_entry::hasPayload() const noexcept -> bool {
    return std::holds_alternative<log_payload>(_payload);
}
auto persisting_log_entry::hasMeta() const noexcept -> bool {
    return std::holds_alternative<log_meta_payload>(_payload);
}

auto persisting_log_entry::meta() const noexcept -> const log_meta_payload * {
    return std::get_if<log_meta_payload>(&_payload);
}

in_memory_log_entry::in_memory_log_entry(persisting_log_entry entry, bool waitForSync) :
    _waitForSync(waitForSync), _logEntry(std::move(entry)) {
}

void in_memory_log_entry::setInsertTp(clock::time_point tp) noexcept {
    _insertTp = tp;
}

auto in_memory_log_entry::insert_tp() const noexcept -> clock::time_point {
    return _insertTp;
}

auto in_memory_log_entry::entry() const noexcept -> persisting_log_entry const & {
    // Note that while get() isn't marked as noexcept, it actually is.
    return _logEntry.get();
}

log_entry_view::log_entry_view(log_index index, log_payload const &payload) noexcept :
    _index(index), _payload(payload.slice()) {
}

log_entry_view::log_entry_view(log_index index, velocypack::Slice payload) noexcept : _index(index), _payload(payload) {
}

auto log_entry_view::logIndex() const noexcept -> log_index {
    return _index;
}

auto log_entry_view::logPayload() const noexcept -> velocypack::Slice {
    return _payload;
}

void log_entry_view::toVelocyPack(velocypack::Builder &builder) const {
    auto og = velocypack::ObjectBuilder(&builder);
    builder.add("logIndex", velocypack::Value(_index));
    builder.add("payload", _payload);
}

auto log_entry_view::fromVelocyPack(velocypack::Slice slice) -> log_entry_view {
    return log_entry_view(slice.get("logIndex").extract<log_index>(), slice.get("payload"));
}

auto log_entry_view::clonePayload() const -> log_payload {
    return log_payload::create_from_slice(_payload);
}

namespace {
    constexpr std::string_view StringFirstIndexOfTerm = "FirstIndexOfTerm";
    constexpr std::string_view StringUpdateParticipantsConfig = "UpdateParticipantsConfig";
}    // namespace

auto log_meta_payload::fromVelocyPack(velocypack::Slice s) -> log_meta_payload {
    auto typeSlice = s.get(StaticStrings::IndexType);
    if (typeSlice.toString() == StringFirstIndexOfTerm) {
        return {first_entry_of_term::fromVelocyPack(s)};
    } else if (typeSlice.toString() == StringUpdateParticipantsConfig) {
        return {update_participants_config::fromVelocyPack(s)};
    } else {
        TRI_ASSERT(false);
        THROW_DBMS_EXCEPTION(TRI_ERROR_BAD_PARAMETER);
    }
}

void log_meta_payload::toVelocyPack(velocypack::Builder &builder) const {
    std::visit([&](auto const &v) { v.toVelocyPack(builder); }, info);
}

void log_meta_payload::first_entry_of_term::toVelocyPack(velocypack::Builder &b) const {
    velocypack::ObjectBuilder ob(&b);
    b.add(StaticStrings::IndexType, velocypack::Value(StringFirstIndexOfTerm));
    b.add(StaticStrings::Leader, velocypack::Value(leader));
    b.add(velocypack::Value(StaticStrings::Participants));
    velocypack::serialize(b, participants);
}

void log_meta_payload::update_participants_config::toVelocyPack(velocypack::Builder &b) const {
    velocypack::ObjectBuilder ob(&b);
    b.add(StaticStrings::IndexType, velocypack::Value(StringUpdateParticipantsConfig));
    b.add(velocypack::Value(StaticStrings::Participants));
    velocypack::serialize(b, participants);
}

auto log_meta_payload::update_participants_config::fromVelocyPack(velocypack::Slice s) -> update_participants_config {
    TRI_ASSERT(s.get(StaticStrings::IndexType).toString() == StringUpdateParticipantsConfig);
    auto participants = velocypack::deserialize<agency::participants_config>(s.get(StaticStrings::Participants));
    return update_participants_config {std::move(participants)};
}

auto log_meta_payload::first_entry_of_term::fromVelocyPack(velocypack::Slice s) -> first_entry_of_term {
    TRI_ASSERT(s.get(StaticStrings::IndexType).toString() == StringFirstIndexOfTerm);
    auto leader = s.get(StaticStrings::Leader).copyString();
    auto participants = velocypack::deserialize<agency::participants_config>(s.get(StaticStrings::Participants));
    return first_entry_of_term {std::move(leader), std::move(participants)};
}

auto log_meta_payload::withFirstEntryOfTerm(ParticipantId leader, agency::participants_config config)
    -> log_meta_payload {
    return log_meta_payload {first_entry_of_term {.leader = std::move(leader), .participants = std::move(config)}};
}

auto log_meta_payload::withUpdateParticipantsConfig(agency::participants_config config) -> log_meta_payload {
    return log_meta_payload {update_participants_config {.participants = std::move(config)}};
}
