#include <stdlib.h>
#include <string>

#include "mpi.h"
#include "master_markov.h"

#define MASTER_RANK 0

using namespace std;

int main(int argc, char *argv[]) {
  int rank, numtasks;
  string pathbooks;
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
  
  if (rank == MASTER_RANK) {
    if (argv[0] == NULL) {
      pathbooks = "./";
    }
    else {
      pathbooks = argv[0];
    }
    char* cstr;
    strstr(cstr, pathbooks.c_str());
    master(numtasks, cstr, strtol(argv[1], NULL, 10));
  }
  else {
    slave(rank);
  }
}

