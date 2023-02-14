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
#include <memory>
#include <ostream>

#if (_MSC_VER >= 1)
// suppress warnings:
#pragma warning(push)
// conversion from 'size_t' to 'immer::detail::rbts::count_t', possible loss of
// data
#pragma warning(disable : 4267)
// result of 32-bit shift implicitly converted to 64 bits (was 64-bit shift
// intended?)
#pragma warning(disable : 4334)
#endif
#include <immer/flex_vector.hpp>
#include <immer/box.hpp>
#if (_MSC_VER >= 1)
#pragma warning(pop)
#endif

#include "containers/immer_memory_policy.h"
#include "logger/LogMacros.h"

namespace nil::dbms {

    struct loggable_value {
        virtual ~loggable_value() = default;
        virtual auto operator<<(std::ostream &os) const noexcept -> std::ostream & = 0;
    };

    template<typename T, const char *N>
    struct log_name_value_pair : loggable_value {
        explicit log_name_value_pair(T t) : value(std::move(t)) {
        }
        T value;
        auto operator<<(std::ostream &os) const noexcept -> std::ostream & override {
            return os << N << "=" << value;
        }
    };

    struct logger_context {
        explicit logger_context(LogTopic const &topic) : topic(topic) {
        }

        template<const char N[], typename T>
        auto with(T &&t) const -> logger_context {
            using S = std::decay_t<T>;
            auto pair = std::make_shared<log_name_value_pair<S, N>>(std::forward<T>(t));
            return logger_context(values.push_back(std::move(pair)), topic);
        }

        auto withTopic(LogTopic const &newTopic) const {
            return logger_context(values, newTopic);
        }

        friend auto operator<<(std::ostream &os, logger_context const &ctx) -> std::ostream & {
            os << "[";
            bool first = true;
            for (auto const &v : ctx.values) {
                if (!first) {
                    os << ", ";
                }
                v->operator<<(os);
                first = false;
            }
            os << "]";
            return os;
        }

        using Container = ::immer::flex_vector<std::shared_ptr<loggable_value>, nil::dbms::immer::dbms_memory_policy>;
        LogTopic const &topic;
        Container const values = {};

    private:
        logger_context(Container values, LogTopic const &topic) : topic(topic), values(std::move(values)) {
        }
    };
}    // namespace nil::dbms

#define LOG_CTX(id, level, ctx) LOG_TOPIC(id, level, (ctx).topic) << (ctx) << " "
#define LOG_CTX_IF(id, level, ctx, cond) LOG_TOPIC_IF(id, level, (ctx).topic, cond) << (ctx) << " "
