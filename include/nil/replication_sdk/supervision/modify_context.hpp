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
#include <optional>
#include <type_traits>
#include <tuple>
#include <functional>
#include <basics/debugging.h>

namespace nil::dbms::replication_sdk {
    template<typename... Ts>
    struct modify_context {
        explicit modify_context(std::optional<Ts>... values) : values(std::move(values)...) {
        }

        [[nodiscard]] auto has_modification() const noexcept -> bool {
            return (forType<Ts>().wasModified || ...);
        }

        template<typename... T, typename F>
        auto modify(F &&fn) {
            static_assert(std::is_invocable_v<F, T &...>);
            TRI_ASSERT((forType<T>().value.has_value() && ...)) << "modifying action expects value to be present";
            ((forType<T>().wasModified = true), ...);
            return std::invoke(std::forward<F>(fn), (*forType<T>().value)...);
        }

        template<typename... T, typename F>
        auto modify_or_create(F &&fn) {
            static_assert(std::is_invocable_v<F, T &...>);
            (
                [&] {
                    if (!forType<T>().value.has_value()) {
                        static_assert(std::is_default_constructible_v<T>);
                        forType<T>().value.emplace();
                    }
                }(),
                ...);
            ((forType<T>().wasModified = true), ...);
            return std::invoke(std::forward<F>(fn), (*forType<T>().value)...);
        }

        template<typename T, typename... Args>
        void setValue(Args &&...args) {
            forType<T>().value.emplace(std::forward<Args>(args)...);
            forType<T>().wasModified = true;
        }

        template<typename T>
        auto getValue() const -> T const & {
            return forType<T>().value.value();
        }

        template<typename T>
        [[nodiscard]] auto has_modification_for() const noexcept -> bool {
            return forType<T>().wasModified;
        }

        template<typename T>
        struct modify_some_type {
            explicit modify_some_type(std::optional<T> const &value) : value(value) {
            }
            std::optional<T> value;
            bool wasModified = false;
        };

    private:
        std::tuple<modify_some_type<Ts>...> values;

        template<typename T>
        auto forType() -> modify_some_type<T> & {
            return std::get<modify_some_type<T>>(values);
        }
        template<typename T>
        auto forType() const -> modify_some_type<T> const & {
            return std::get<modify_some_type<T>>(values);
        }
    };

}    // namespace nil::dbms::replication_sdk
