#include <iostream>
#include <mpi.h>
#include <thread>

int main(int argc, char* argv[]) {
    int provided_thread_support;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided_thread_support);

    if (provided_thread_support != MPI_THREAD_MULTIPLE) {
        std::cerr << "Error: MPI implementation does not support MPI_THREAD_MULTIPLE." << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    if (world_rank == 0) {
        std::cout << "H.E.L.L. is running with " << world_size << " processes." << std::endl;
        std::cout << "Hardware concurrency on rank 0: " << std::thread::hardware_concurrency() << " threads." << std::endl;
    }

    MPI_Finalize();
    return 0;
}
