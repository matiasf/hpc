#include <stdlib.h>
#include <string.h>
#include <string>
#include <iostream>

#include "mpi.h"
#include "master_markov.h"
#include "slave_markov.h"

#define MASTER_RANK 0

using namespace std;

int main(int argc, char *argv[]) {
  int rank, numtasks, provided, provided2;
  string pathbooks;
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
  cout << "Numtasks - " << numtasks << endl;
  if (rank == MASTER_RANK) {
    if (argv[1] == NULL) {
      pathbooks = "/tmp/markov";
    }
    else {
      pathbooks = argv[1];
    }
    cout << "Master: Created." << endl;
    master(numtasks, pathbooks.c_str(), 5);
  }
  else {
    cout << "Slave: Created " << rank << endl;
    slave(rank);
  }
}

