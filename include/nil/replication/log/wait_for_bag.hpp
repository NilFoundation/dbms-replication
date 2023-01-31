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

#include <exception>
#include <vector>

namespace nil::dbms::futures {
    template<typename T>
    class Future;
    template<typename T>
    class Promise;
    struct Unit;
}    // namespace nil::dbms::futures

namespace nil::dbms {

    struct WaitForBag {
        WaitForBag() = default;

        auto addWaitFor() -> futures::Future<futures::Unit>;

        void resolveAll();

        void resolveAll(std::exception_ptr const &);

        [[nodiscard]] auto empty() const noexcept -> bool;

    private:
        std::vector<futures::Promise<futures::Unit>> _waitForBag;
    };

}    // namespace nil::dbms
