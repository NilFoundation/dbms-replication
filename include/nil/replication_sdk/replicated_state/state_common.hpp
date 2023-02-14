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

#include <chrono>
#include <compare>
#include <cstdint>
#include <iostream>
#include <optional>

#include <velocypack/Slice.h>
#include "basics/result.h"
#include "inspection/vpack_load_inspector.h"
#include "inspection/vpack_save_inspector.h"
#include "inspection/transformers.h"

namespace nil::dbms::velocypack {
    class Value;
    template<typename, typename>
    struct Extractor;
    class Builder;
    class Slice;
}    // namespace nil::dbms::velocypack
namespace nil::dbms {
    namespace replication_sdk::replicated_state {

        struct state_generation {
            constexpr state_generation() noexcept : value {0} {
            }
            constexpr explicit state_generation(std::uint64_t value) noexcept : value {value} {
            }
            std::uint64_t value;

            [[nodiscard]] auto saturated_decrement(uint64_t delta = 1) const noexcept -> state_generation;

            bool operator==(const state_generation &rhs) const {
                return value == rhs.value;
            }
            bool operator!=(const state_generation &rhs) const {
                return !(rhs == *this);
            }

            bool operator<(const state_generation &rhs) const {
                return value < rhs.value;
            }
            bool operator>(const state_generation &rhs) const {
                return rhs < *this;
            }
            bool operator<=(const state_generation &rhs) const {
                return !(rhs < *this);
            }
            bool operator>=(const state_generation &rhs) const {
                return !(*this < rhs);
            }

            [[nodiscard]] auto operator+(std::uint64_t delta) const -> state_generation;
            auto operator++() noexcept -> state_generation &;
            auto operator++(int) noexcept -> state_generation;

            friend auto operator<<(std::ostream &, state_generation) -> std::ostream &;

            [[nodiscard]] explicit operator velocypack::Value() const noexcept;
        };

        auto to_string(state_generation) -> std::string;

        template<class Inspector>
        auto inspect(Inspector &f, state_generation &x) {
            if constexpr (Inspector::isLoading) {
                auto v = uint64_t {0};
                auto res = f.apply(v);
                if (res.ok()) {
                    x = state_generation(v);
                }
                return res;
            } else {
                return f.apply(x.value);
            }
        }

        enum class snapshot_status {
            kUninitialized,
            kInProgress,
            kCompleted,
            kFailed,
        };

        struct snapshot_info {
            using clock = std::chrono::system_clock;

            struct Error {
                ErrorCode error {0};
                std::optional<std::string> message;
                clock::time_point retryAt;

                bool operator==(const Error &rhs) const {
                    return error == rhs.error && message == rhs.message && retryAt == rhs.retryAt;
                }
                bool operator!=(const Error &rhs) const {
                    return !(rhs == *this);
                }
            };

            void updateStatus(snapshot_status status) noexcept;

            snapshot_status status {snapshot_status::kUninitialized};
            clock::time_point timestamp;
            std::optional<Error> error;

            bool operator==(const snapshot_info &rhs) const {
                return status == rhs.status && timestamp == rhs.timestamp && error == rhs.error;
            }
            bool operator!=(const snapshot_info &rhs) const {
                return !(rhs == *this);
            }
        };

        auto to_string(snapshot_status) noexcept -> std::string_view;
        auto snapshotStatusFromString(std::string_view) noexcept -> snapshot_status;
        auto operator<<(std::ostream &, snapshot_status const &) -> std::ostream &;
        auto operator<<(std::ostream &, state_generation) -> std::ostream &;

        struct SnapshotStatusStringTransformer {
            using SerializedType = std::string;
            auto toSerialized(snapshot_status source, std::string &target) const -> inspection::Status;
            auto fromSerialized(std::string const &source, snapshot_status &target) const -> inspection::Status;
        };

        template<class Inspector>
        auto inspect(Inspector &f, snapshot_info &x) {
            return f.object(x).fields(
                f.field("timestamp", x.timestamp).transformWith(inspection::TimeStampTransformer {}),
                f.field("error", x.error),
                f.field("status", x.status).transformWith(SnapshotStatusStringTransformer {}));
        }

        template<class Inspector>
        auto inspect(Inspector &f, snapshot_info::Error &x) {
            return f.object(x).fields(f.field("retryAt", x.retryAt).transformWith(inspection::TimeStampTransformer {}),
                                      f.field("error", x.error).transformWith(inspection::ErrorCodeTransformer {}),
                                      f.field("message", x.message));
        }

    }    // namespace replication_sdk::replicated_state

    template<>
    struct nil::dbms::velocypack::Extractor<replication_sdk::replicated_state::state_generation> {
        static auto extract(velocypack::Slice slice) -> replication_sdk::replicated_state::state_generation {
            return replication_sdk::replicated_state::state_generation {slice.getNumericValue<std::uint64_t>()};
        }
    };
}    // namespace nil::dbms
