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

#include <velocypack/Iterator.h>

#include <basics/debugging.h>
#include <basics/static_strings.h>

#include <nil/replication_sdk/agency_collection_specification.hpp>

using namespace nil::dbms::replication_sdk::agency;
using namespace nil::dbms::basics;

collection_group::collection_group(VPackSlice slice) :
    id(collection_group_id {slice.get(StaticStrings::Id).extract<collection_group_id::Identifier::BaseType>()}),
    attributes(slice.get("attributes")) {
    {
        auto cs = slice.get("collections");
        TRI_ASSERT(cs.isObject());
        collections.reserve(cs.length());
        for (auto const &[key, value] : VPackObjectIterator(cs)) {
            auto cid = key.extract<std::string>();
            collections.emplace(std::move(cid), Collection(value));
        }
    }

    {
        auto sss = slice.get("shardSheaves");
        TRI_ASSERT(sss.isArray());
        shardSheaves.reserve(sss.length());
        for (auto const &rs : VPackArrayIterator(sss)) {
            shardSheaves.emplace_back(rs);
        }
    }
}

void collection_group::toVelocyPack(VPackBuilder &builder) const {
    VPackObjectBuilder ob(&builder);
    builder.add(StaticStrings::Id, VPackValue(id.id()));
    builder.add(VPackValue("attributes"));
    attributes.toVelocyPack(builder);
    {
        VPackObjectBuilder cb(&builder, "collections");
        for (auto const &[cid, collection] : collections) {
            builder.add(VPackValue(cid));
            collection.toVelocyPack(builder);
        }
    }
    {
        VPackArrayBuilder sb(&builder, "shardSheaves");
        for (auto const &sheaf : shardSheaves) {
            sheaf.toVelocyPack(builder);
        }
    }
}

collection_group::Collection::Collection(VPackSlice slice) {
    TRI_ASSERT(slice.isEmptyObject());
}

void collection_group::Collection::toVelocyPack(VPackBuilder &builder) const {
    builder.add(VPackSlice::emptyObjectSlice());
}

collection_group::shard_sheaf::shard_sheaf(VPackSlice slice) {
    TRI_ASSERT(slice.isObject());
    replicatedLog = LogId {slice.get("replicatedLog").extract<uint64_t>()};
}

void collection_group::shard_sheaf::toVelocyPack(VPackBuilder &builder) const {
    VPackObjectBuilder ob(&builder);
    builder.add("replicatedLog", VPackValue(replicatedLog.id()));
}

collection_group::Attributes::Attributes(VPackSlice slice) {
    TRI_ASSERT(slice.isObject());
    waitForSync = slice.get(StaticStrings::WaitForSyncString).extract<bool>();
    writeConcern = slice.get(StaticStrings::WriteConcern).extract<std::size_t>();
}

void collection_group::Attributes::toVelocyPack(VPackBuilder &builder) const {
    VPackObjectBuilder ob(&builder);
    builder.add(StaticStrings::WaitForSyncString, VPackValue(waitForSync));
    builder.add(StaticStrings::WriteConcern, VPackValue(writeConcern));
}
