#include <mpi.h>
#include <stdio.h>

int main(){
    int rank, size;

    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int count=0;
    int sender_rank = 0;
    int recvrs_rank = 1;
    do{
        if(rank == sender_rank){
            count++;
            MPI_Send(&count, 1, MPI_INT, recvrs_rank, 0, MPI_COMM_WORLD);
            printf("mensagem \"%d\" enviada de %d para %d \n", count, sender_rank, recvrs_rank);
        }

        if(rank == recvrs_rank){
            MPI_Recv(&count, 1, MPI_INT, sender_rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            printf("mensagem \"%d\" de %d recebida por %d\n", count, sender_rank, recvrs_rank);
        }
    }while(count < 10);

    MPI_Finalize();
    if(rank == 0){
       printf("fim da execução");
    }
    return 0;
}
