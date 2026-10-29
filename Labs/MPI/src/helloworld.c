#include <mpi.h>
#include <stdio.h>

int main(){
  MPI_Init(NULL, NULL);
  int size, rank;

  MPI_Comm_size(MPI_COMM_WORLD, &size);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  printf("Hello world (rank %d de %d workers)\n", rank, size);

  MPI_Finalize();
}
