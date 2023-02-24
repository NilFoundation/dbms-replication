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

#include "test_helper.hpp"

#include <nil/dbms/replication/replicated_log/log_core.hpp>
#include <nil/dbms/replication/replicated_log/log_leader.hpp>
#include <nil/dbms/replication/replicated_log/replicated_log.hpp>
#include <nil/dbms/replication/replicated_log/types.hpp>

#include <utility>
