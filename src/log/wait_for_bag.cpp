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

#include <nil/dbms/replication/log/wait_for_bag.hpp>

#include <futures/Promise.h>
#include <futures/Future.h>
#include <futures/Unit.h>

using namespace nil::dbms;

auto WaitForBag::addWaitFor() -> futures::Future<futures::Unit> {
    using namespace nil::dbms::futures;
    return _waitForBag.emplace_back(Promise<Unit> {}).getFuture();
}

void WaitForBag::resolveAll() {
    for (auto &promise : _waitForBag) {
        TRI_ASSERT(promise.valid());
        promise.setValue();
    }
    _waitForBag.clear();
}

void WaitForBag::resolveAll(std::exception_ptr const &ex) {
    for (auto &promise : _waitForBag) {
        TRI_ASSERT(promise.valid());
        promise.setException(ex);
    }
    _waitForBag.clear();
}

auto WaitForBag::empty() const noexcept -> bool {
    return _waitForBag.empty();
}
