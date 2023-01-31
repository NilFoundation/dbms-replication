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

namespace nil::dbms::replication::state {

    template<typename T>
    struct EntryDeserializer { };
    template<typename T>
    struct EntrySerializer { };

    template<typename S>
    struct ReplicatedStateTraits {
        using FactoryType = typename S::FactoryType;
        using LeaderType = typename S::LeaderType;
        using FollowerType = typename S::FollowerType;
        using EntryType = typename S::EntryType;
        using CoreType = typename S::CoreType;
        using Deserializer = EntryDeserializer<EntryType>;
        using Serializer = EntrySerializer<EntryType>;
    };

}    // namespace nil::dbms::replication::state
