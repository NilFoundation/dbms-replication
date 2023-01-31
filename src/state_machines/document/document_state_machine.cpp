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

#include <nil/dbms/replication/state_machines/document/document_state_machine.hpp>

using namespace nil::dbms::replication::state::document;

DocumentLeaderState::DocumentLeaderState(std::unique_ptr<DocumentCore> core) : _core(std::move(core)) {};

auto DocumentLeaderState::resign() &&noexcept -> std::unique_ptr<DocumentCore> {
    return std::move(_core);
}

auto DocumentLeaderState::recover_entries(std::unique_ptr<EntryIterator> ptr) -> futures::Future<Result> {
    return {TRI_ERROR_NO_ERROR};
}

DocumentFollowerState::DocumentFollowerState(std::unique_ptr<DocumentCore> core) : _core(std::move(core)) {};

auto DocumentFollowerState::resign() &&noexcept -> std::unique_ptr<DocumentCore> {
    return std::move(_core);
}

auto DocumentFollowerState::acquireSnapshot(ParticipantId const &destination, log_index) noexcept
    -> futures::Future<Result> {
    return {TRI_ERROR_NO_ERROR};
}

auto DocumentFollowerState::apply_entries(std::unique_ptr<EntryIterator> ptr) noexcept -> futures::Future<Result> {
    return {TRI_ERROR_NO_ERROR};
};

auto DocumentFactory::construct_follower(std::unique_ptr<DocumentCore> core) -> std::shared_ptr<DocumentFollowerState> {
    return std::make_shared<DocumentFollowerState>(std::move(core));
}

auto DocumentFactory::construct_leader(std::unique_ptr<DocumentCore> core) -> std::shared_ptr<DocumentLeaderState> {
    return std::make_shared<DocumentLeaderState>(std::move(core));
}

auto DocumentFactory::construct_core(global_log_identifier const &gid) -> std::unique_ptr<DocumentCore> {
    return std::make_unique<DocumentCore>();
}

auto nil::dbms::replication::state::EntryDeserializer<DocumentLogEntry>::operator()(
    streams::serializer_tag_t<DocumentLogEntry>,
    velocypack::Slice s) const -> DocumentLogEntry {
    return DocumentLogEntry {};
}

void nil::dbms::replication::state::EntrySerializer<DocumentLogEntry>::operator()(
    streams::serializer_tag_t<DocumentLogEntry>,
    DocumentLogEntry const &e,
    velocypack::Builder &b) const {
}

#include <nil/dbms/replication/state/state.tpp>

template struct nil::dbms::replication::state::ReplicatedState<DocumentState>;
