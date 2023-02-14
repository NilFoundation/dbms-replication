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

#include <nil/replication_sdk/version.hpp>

#include <basics/result_t.h>
#include <basics/exceptions.h>
#include <basics/string_utils.h>
#include <logger/LogMacros.h>
#include <velocypack/Slice.h>

using namespace nil::dbms;
using namespace nil::dbms::basics;

auto nil::dbms::replication::parseVersion(std::string_view version) -> ResultT<replication::Version> {
    if (version.data() == nullptr) {
        version = "";
    }
    if (version == "1") {
        return replication::Version::ONE;
    } else if (version == "2") {
        return replication::Version::TWO;
    }
    return ResultT<replication::Version>::error(
        TRI_ERROR_BAD_PARAMETER, StringUtils::concatT(R"(Replication version must be "1" or "2", but is )", version));
}

auto nil::dbms::replication::parseVersion(velocypack::Slice version) -> nil::dbms::ResultT<replication::Version> {
    if (version.isString()) {
        return parseVersion(version.stringView());
    } else {
        return ResultT<replication::Version>::error(
            TRI_ERROR_BAD_PARAMETER,
            StringUtils::concatT(R"(Replication version must be a string, but is )", version.typeName()));
    }
}

auto replication::versionToString(replication::Version version) -> std::string_view {
    switch (version) {
        case Version::ONE:
            return "1";
        case Version::TWO:
            return "2";
    }
    abortOrThrow(
        TRI_ERROR_INTERNAL,
        StringUtils::concatT("Unhandled replication version: ", static_cast<std::underlying_type_t<Version>>(version)),
        ADB_HERE);
}
