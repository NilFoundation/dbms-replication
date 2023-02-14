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
#pragma once

#include <unordered_map>

#include <velocypack/Builder.h>
#include <velocypack/Slice.h>

#include <basics/identifier.h>
#include <nil/replication_sdk/cluster/cluster_types.hpp>
#include <nil/replication_sdk/replicated_log/types.hpp>

namespace nil::dbms::replication_sdk::agency {

    struct collection_group_id : basics::Identifier {
        using Identifier::Identifier;
    };

    struct collection_group {
        collection_group_id id;

        struct Collection {
            explicit Collection(VPackSlice slice);
            void toVelocyPack(VPackBuilder &builder) const;
        };
        std::unordered_map<CollectionID, Collection> collections;

        struct shard_sheaf {
            LogId replicatedLog;

            explicit shard_sheaf(VPackSlice slice);
            void toVelocyPack(VPackBuilder &builder) const;
        };
        std::vector<shard_sheaf> shardSheaves;

        struct Attributes {
            std::size_t writeConcern;
            bool waitForSync;

            explicit Attributes(VPackSlice slice);
            void toVelocyPack(VPackBuilder &builder) const;
        };
        Attributes attributes;

        explicit collection_group(VPackSlice slice);
        void toVelocyPack(VPackBuilder &builder) const;
    };

}    // namespace nil::dbms::replication_sdk::agency

DECLARE_HASH_FOR_IDENTIFIER(nil::dbms::replication_sdk::agency::collection_group_id)
DECLARE_EQUAL_FOR_IDENTIFIER(nil::dbms::replication_sdk::agency::collection_group_id)
