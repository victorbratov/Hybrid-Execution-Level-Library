#include "doctest/extensions/doctest_mpi.h"
#include "hell.hpp"

MPI_TEST_CASE("test", 2) {
	MPI_CHECK(0, 0 == test_rank);
}
