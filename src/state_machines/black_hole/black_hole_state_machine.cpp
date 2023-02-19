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
#include <basics/voc_errors.h>
#include <futures/Future.h>

#include <nil/dbms/replication/state_machines/black_hole/black_hole_state_machine.hpp>

using namespace nil::dbms;
using namespace nil::dbms::replication;
using namespace nil::dbms::replication::replicated_state;
using namespace nil::dbms::replication::replicated_state::black_hole;

auto BlackHoleLeaderState::recoverEntries(std::unique_ptr<EntryIterator> ptr) -> futures::Future<Result> {
    return {TRI_ERROR_NO_ERROR};
}

auto BlackHoleLeaderState::write(std::string_view data) -> log_index {
    BlackHoleLogEntry entry {.value = std::string {data}};
    return getStream()->insert(entry);
}

BlackHoleLeaderState::BlackHoleLeaderState(std::unique_ptr<BlackHoleCore> core) : _core(std::move(core)) {
}

auto BlackHoleLeaderState::resign() &&noexcept -> std::unique_ptr<BlackHoleCore> {
    return std::move(_core);
}

auto BlackHoleFollowerState::acquireSnapshot(ParticipantId const &destination, log_index) noexcept
    -> futures::Future<Result> {
    return {TRI_ERROR_NO_ERROR};
}

auto BlackHoleFollowerState::apply_entries(std::unique_ptr<EntryIterator> ptr) noexcept -> futures::Future<Result> {
    return {TRI_ERROR_NO_ERROR};
}

BlackHoleFollowerState::BlackHoleFollowerState(std::unique_ptr<BlackHoleCore> core) : _core(std::move(core)) {
}
auto BlackHoleFollowerState::resign() &&noexcept -> std::unique_ptr<BlackHoleCore> {
    return std::move(_core);
}

auto BlackHoleFactory::constructFollower(std::unique_ptr<BlackHoleCore> core)
    -> std::shared_ptr<BlackHoleFollowerState> {
    return std::make_shared<BlackHoleFollowerState>(std::move(core));
}

auto BlackHoleFactory::constructLeader(std::unique_ptr<BlackHoleCore> core) -> std::shared_ptr<BlackHoleLeaderState> {
    return std::make_shared<BlackHoleLeaderState>(std::move(core));
}

auto BlackHoleFactory::constructCore(global_log_identifier const &) -> std::unique_ptr<BlackHoleCore> {
    return std::make_unique<BlackHoleCore>();
}

auto replicated_state::entry_deserializer<replicated_state::black_hole::BlackHoleLogEntry>::operator()(
    streams::serializer_tag_t<replicated_state::black_hole::BlackHoleLogEntry>,
    velocypack::Slice s) const -> replicated_state::black_hole::BlackHoleLogEntry {
    return replicated_state::black_hole::BlackHoleLogEntry {s.copyString()};
}

void replicated_state::entry_serializer<replicated_state::black_hole::BlackHoleLogEntry>::operator()(
    streams::serializer_tag_t<replicated_state::black_hole::BlackHoleLogEntry>, black_hole::BlackHoleLogEntry const &e,
    velocypack::Builder &b) const {
    b.add(velocypack::Value(e.value));
}

#include <nil/dbms/replication/replicated_state/replicated_state.tpp>

template struct replicated_state::replicated_state_t<BlackHoleState>;
