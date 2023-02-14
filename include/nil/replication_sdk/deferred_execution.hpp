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
#include <utility>
#include <cstddef>
#include <functional>
#include <memory>

namespace nil::dbms {

    /**
     * @brief Just to move callable stuff around without allocations and exceptions
     */
    struct deferred_action {
        static constexpr std::size_t alloc_size = 32;
        deferred_action() = default;

        deferred_action(deferred_action &&other) noexcept {
            *this = std::move(other);
        }

        deferred_action &operator=(deferred_action &&other) noexcept {
            fire();

            // other will have hasAction == false and invoke_func == false
            std::swap(invoke_func, other.invoke_func);
            if (invoke_func != nullptr) {
                // this will run the move constructor and then destroy other.storage
                invoke_func(&other.storage, action::move_construct_into_and_destroy, &storage);
            }

            return *this;
        }

        deferred_action(deferred_action const &) = delete;
        deferred_action &operator=(deferred_action const &) = delete;

        template<typename F, typename Func = std::decay_t<F>,
                 std::enable_if_t<std::is_nothrow_invocable_r_v<void, F>, int> = 0>
        explicit deferred_action(F &&f) noexcept : invoke_func(call_action<F>) {
            static_assert(sizeof(F) <= alloc_size, "DeferredAction's size exceeded");
            static_assert(std::is_nothrow_move_constructible_v<Func>);
            new (&storage) Func(std::forward<F>(f));
        }

        ~deferred_action() {
            fire();
        }

        explicit operator bool() const noexcept {
            return invoke_func != nullptr;
        }

        void fire() noexcept {
            if (invoke_func != nullptr) {
                invoke_func(&storage, action::invoke_and_destroy, nullptr);
                invoke_func = nullptr;
            }
        }

        void operator()() noexcept {
            fire();
        }

        template<typename... Fs>
        static auto combine(Fs &&...fs) -> deferred_action {
            return combine(std::index_sequence_for<Fs...> {}, std::forward<Fs>(fs)...);
        }

    private:
        template<typename... Fs, std::size_t... Idx>
        static auto combine(std::index_sequence<Idx...>, Fs &&...fs) {
            auto tup =
                std::make_unique<std::tuple<typename std::unwrap_ref_decay<Fs>::type...>>(std::forward<Fs>(fs)...);
            return deferred_action([tup = std::move(tup)]() noexcept { (std::invoke(std::get<Idx>(*tup)), ...); });
        }

        enum class action {
            invoke_and_destroy,
            move_construct_into_and_destroy,
        };

        template<typename F, typename Func = std::decay_t<F>>
        static void call_action(void *storage, action what, void *ptr) noexcept {
            auto &func = *reinterpret_cast<Func *>(storage);
            switch (what) {
                case action::invoke_and_destroy:
                    std::invoke(std::forward<F>(func));
                    static_assert(std::is_nothrow_destructible_v<Func>);
                    func.~Func();
                    break;
                case action::move_construct_into_and_destroy:
                    new (ptr) Func(std::move(func));
                    func.~Func();
                    break;
            }
        }

        std::aligned_storage_t<alloc_size, alignof(std::max_align_t)> storage {};
        void (*invoke_func)(void *, action, void *) noexcept = nullptr;
    };

}    // namespace nil::dbms
