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

namespace nil::dbms::velocypack {
    class ArrayIterator;
}

namespace nil::dbms::replication_sdk::replicated_log {

    class vpack_array_to_log_payload_iterator : public typed_log_iterator<log_payload> {
    public:
        auto next() -> std::optional<log_payload> override {
            if (_iter.valid()) {
                return log_payload::create_from_slice(*_iter++);
            }

            return std::nullopt;
        }

        explicit vpack_array_to_log_payload_iterator(VPackSlice slice) : _iter(slice) {
        }

    private:
        velocypack::ArrayIterator _iter;
    };

}    // namespace nil::dbms::replication_sdk::replicated_log