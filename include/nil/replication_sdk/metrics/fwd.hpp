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

namespace nil::dbms::metrics {

    template<typename T>
    class Guard;

    class Counter;

    template<typename T>
    class Gauge;

    template<typename T>
    class FixScale;

    template<typename T>
    class LinScale;

    template<typename T>
    class LogScale;

    template<typename Scale>
    class Histogram;

    class MetricsFeature;

}    // namespace nil::dbms::metrics
