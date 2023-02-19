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

#include <string>
#include <string_view>

namespace nil::dbms {
    template<typename T>
    class ResultT;
}
namespace nil::dbms::velocypack {
    class Slice;
}

namespace nil::dbms::replication {

    enum class Version { ONE = 1, TWO = 2 };

    auto parseVersion(std::string_view version) -> ResultT<replication::Version>;
    auto parseVersion(velocypack::Slice version) -> ResultT<replication::Version>;

    auto versionToString(Version version) -> std::string_view;

}    // namespace nil::dbms::replication
