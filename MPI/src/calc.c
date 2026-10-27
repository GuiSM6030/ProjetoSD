#include <stdio.h>
#include <mpi.h>

void dados(){
    float v1, v2, res;
    char op;

    printf("Entre com a expressao: ");
    scanf("%f %c %f", &v1, &op, &v2);

    int func_rank = (op == '+') ? 1 : (op == '-') ? 2 : (op == '*') ? 3 : 4 ;

    MPI_Send(&op, 1, MPI_CHAR,  func_rank, 0, MPI_COMM_WORLD);
    MPI_Send(&v1, 1, MPI_FLOAT, func_rank, 0, MPI_COMM_WORLD);
    MPI_Send(&v2, 1, MPI_FLOAT, func_rank, 0, MPI_COMM_WORLD);

    MPI_Recv(&res, 1, MPI_FLOAT, func_rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    printf("%f %c %f = %f (rank %d)\n",v1, op, v2, res, func_rank);
}

void soma(){
    float v1, v2, res;
    char op;

    MPI_Recv(&op, 1, MPI_CHAR,  0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    MPI_Recv(&v1, 1, MPI_FLOAT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    MPI_Recv(&v2, 1, MPI_FLOAT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    res = v1 + v2;
    MPI_Send(&res, 1, MPI_FLOAT, 0, 0, MPI_COMM_WORLD);
}

void subtracao(){
    float v1, v2, res;
    char op;

    MPI_Recv(&op, 1, MPI_CHAR,  0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    MPI_Recv(&v1, 1, MPI_FLOAT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    MPI_Recv(&v2, 1, MPI_FLOAT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    res = v1 - v2;
    MPI_Send(&res, 1, MPI_FLOAT, 0, 0, MPI_COMM_WORLD);
}

void divisao(){
    float v1, v2, res;
    char op;

    MPI_Recv(&op, 1, MPI_CHAR,  0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    MPI_Recv(&v1, 1, MPI_FLOAT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    MPI_Recv(&v2, 1, MPI_FLOAT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    if(v2 != 0)
        res = v1 / v2;
    else
        res = -0;
    MPI_Send(&res, 1, MPI_FLOAT, 0, 0, MPI_COMM_WORLD);
}

void multiplicacao(){
    float v1, v2, res;
    char op;

    MPI_Recv(&op, 1, MPI_CHAR,  0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    MPI_Recv(&v1, 1, MPI_FLOAT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    MPI_Recv(&v2, 1, MPI_FLOAT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    res = v1 * v2;
    MPI_Send(&res, 1, MPI_FLOAT, 0, 0, MPI_COMM_WORLD);
}

typedef void(*func)();
func funcs[] = {dados, soma, subtracao, multiplicacao, divisao};

int main() {
    int rank, size;
    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int count = 0;
    do{
        funcs[rank]();
    }while(count < 10);

    MPI_Finalize();
    return 0;
}
