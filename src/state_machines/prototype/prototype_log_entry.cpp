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

#include <nil/dbms/replication/state_machines/prototype/prototype_log_entry.hpp>

using namespace nil::dbms;
using namespace nil::dbms::replication;
using namespace nil::dbms::replication::state;
using namespace nil::dbms::replication::state::prototype;

const char *prototype_log_entry::getType() noexcept {
    return std::visit(overload {
                          [](prototype_log_entry::DeleteOperation const &o) { return kDelete; },
                          [](prototype_log_entry::InsertOperation const &o) { return kInsert; },
                          [](prototype_log_entry::compare_exchangeOperation const &o) { return kcompare_exchange; },
                      },
                      op);
}

auto prototype_log_entry::create_insert(std::unordered_map<std::string, std::string> map) -> prototype_log_entry {
    return prototype_log_entry {prototype_log_entry::InsertOperation {std::move(map)}};
}

auto prototype_log_entry::create_delete(std::vector<std::string> keys) -> prototype_log_entry {
    return prototype_log_entry {prototype_log_entry::DeleteOperation {std::move(keys)}};
}

auto prototype_log_entry::createcompare_exchange(std::string key, std::string oldValue, std::string newValue)
    -> prototype_log_entry {
    return prototype_log_entry {
        prototype_log_entry::compare_exchangeOperation {std::move(key), std::move(oldValue), std::move(newValue)}};
}

auto state::EntryDeserializer<state::prototype::prototype_log_entry>::operator()(
    streams::serializer_tag_t<state::prototype::prototype_log_entry>,
    velocypack::Slice s) const -> state::prototype::prototype_log_entry {
    return velocypack::deserialize<prototype::prototype_log_entry>(s);
}

void state::EntrySerializer<state::prototype::prototype_log_entry>::operator()(
    streams::serializer_tag_t<state::prototype::prototype_log_entry>, prototype::prototype_log_entry const &e,
    velocypack::Builder &b) const {
    velocypack::serialize(b, e);
}
