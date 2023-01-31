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

#include <nil/dbms/replication/state/state_traits.hpp>
#include <nil/dbms/replication/streams/stream_specification.hpp>

#include "basics/overload.h"
#include "inspection/vpack.h"
#include "inspection/vpack_load_inspector.h"

#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

namespace nil::dbms::replication::state {
    namespace prototype {
        // prototype_log_entry fields
        constexpr static const char *kOp = "op";
        constexpr static const char *kType = "type";

        // Operation names
        constexpr static const char *kDelete = "Delete";
        constexpr static const char *kInsert = "Insert";
        constexpr static const char *kcompare_exchange = "compare_exchange";

        struct prototype_log_entry {
            struct InsertOperation {
                std::unordered_map<std::string, std::string> map;
            };
            struct DeleteOperation {
                std::vector<std::string> keys;
            };
            struct compare_exchangeOperation {
                std::string key;
                std::string oldValue;
                std::string newValue;
            };

            std::variant<DeleteOperation, InsertOperation, compare_exchangeOperation> op;

            const char *getType() noexcept;

            static auto create_insert(std::unordered_map<std::string, std::string> map) -> prototype_log_entry;
            static auto create_delete(std::vector<std::string> keys) -> prototype_log_entry;
            static auto createcompare_exchange(std::string key, std::string oldValue, std::string newValue)
                -> prototype_log_entry;
        };

        namespace detail {
            template<typename Inspector, typename T>
            auto loader_func(Inspector &f, prototype_log_entry &x, T op) {
                auto opSlice = f.slice().get(kOp);
                Inspector ff(opSlice, {});
                auto res = ff.apply(op);
                if (res.ok()) {
                    x.op = op;
                }
                return res;
            }
        }    // namespace detail

        template<class Inspector>
        auto inspect(Inspector &f, prototype_log_entry::InsertOperation &x) {
            return f.object(x).fields(f.field("map", x.map));
        }

        template<class Inspector>
        auto inspect(Inspector &f, prototype_log_entry::DeleteOperation &x) {
            return f.object(x).fields(f.field("keys", x.keys));
        }

        template<class Inspector>
        auto inspect(Inspector &f, prototype_log_entry::compare_exchangeOperation &x) {
            return f.object(x).fields(
                f.field("key", x.key), f.field("oldValue", x.oldValue), f.field("newValue", x.newValue));
        }

        template<class Inspector>
        auto inspect(Inspector &f, prototype_log_entry &x) {
            if constexpr (Inspector::isLoading) {
                auto typeSlice = f.slice().get(kType);
                TRI_ASSERT(typeSlice.isString());

                if (typeSlice.toString() == kInsert) {
                    return detail::loader_func(f, x, prototype_log_entry::InsertOperation {});
                } else if (typeSlice.toString() == kDelete) {
                    return detail::loader_func(f, x, prototype_log_entry::DeleteOperation {});
                } else if (typeSlice.toString() == kcompare_exchange) {
                    return detail::loader_func(f, x, prototype_log_entry::compare_exchangeOperation {});
                } else {
                    THROW_DBMS_EXCEPTION_MESSAGE(
                        TRI_ERROR_BAD_PARAMETER,
                        basics::StringUtils::concatT("Unknown operation '", typeSlice.copyString(), "'"));
                }
            } else {
                auto &b = f.builder();
                VPackObjectBuilder ob(&b);
                b.add(kType, VPackValue(x.getType()));
                b.add(VPackValue(kOp));
                return std::visit([&](auto &&op) { return f.apply(op); }, x.op);
            }
        }

    }    // namespace prototype

    template<>
    struct EntryDeserializer<prototype::prototype_log_entry> {
        auto operator()(streams::serializer_tag_t<prototype::prototype_log_entry>, velocypack::Slice s) const
            -> prototype::prototype_log_entry;
    };

    template<>
    struct EntrySerializer<prototype::prototype_log_entry> {
        void operator()(streams::serializer_tag_t<prototype::prototype_log_entry>,
                        prototype::prototype_log_entry const &e,
                        velocypack::Builder &b) const;
    };

}    // namespace nil::dbms::replication::state
