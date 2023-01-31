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
#include <nil/dbms/cluster/cluster_types.hpp>
#include <nil/dbms/replication/log/types.hpp>

namespace nil::dbms::replication::agency {

    struct CollectionGroupId : basics::Identifier {
        using Identifier::Identifier;
    };

    struct CollectionGroup {
        CollectionGroupId id;

        struct Collection {
            explicit Collection(VPackSlice slice);
            void to_velocy_pack(VPackBuilder &builder) const;
        };
        std::unordered_map<CollectionID, Collection> collections;

        struct ShardSheaf {
            log_id replicatedLog;

            explicit ShardSheaf(VPackSlice slice);
            void to_velocy_pack(VPackBuilder &builder) const;
        };
        std::vector<ShardSheaf> shardSheaves;

        struct Attributes {
            std::size_t writeConcern;
            bool waitForSync;

            explicit Attributes(VPackSlice slice);
            void to_velocy_pack(VPackBuilder &builder) const;
        };
        Attributes attributes;

        explicit CollectionGroup(VPackSlice slice);
        void to_velocy_pack(VPackBuilder &builder) const;
    };

}    // namespace nil::dbms::replication::agency

DECLARE_HASH_FOR_IDENTIFIER(nil::dbms::replication::agency::CollectionGroupId)
DECLARE_EQUAL_FOR_IDENTIFIER(nil::dbms::replication::agency::CollectionGroupId)
