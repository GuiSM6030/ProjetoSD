#include <mpi.h>
#include <stdio.h>

int main(){
    int rank, size;
    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int msg;
    if(rank != 0){
        int prev_rank = rank - 1;
        MPI_Recv(&msg, 1, MPI_INT, prev_rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        printf("msg: %d (%d -> %d)\n", msg, prev_rank, rank);
    } else {
        msg = 0;
    }

    msg++;
    MPI_Send(&msg, 1, MPI_INT, (rank + 1) % size, 0, MPI_COMM_WORLD);

    if(rank == 0){
        int prev_rank = size - 1;
        MPI_Recv(&msg, 1, MPI_INT, prev_rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        printf("msg: %d (%d -> %d)\n", msg, prev_rank, rank);
    }

    MPI_Finalize();
}
