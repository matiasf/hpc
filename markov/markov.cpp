
#include <dirent>
#include <unistd>
#include <string>
#include <vector>
#include <sys/stat>
#include <sys/types>

#include "mpi.h"

using namespace std;

int main(int argc, char *argv[]) {
  int rank, numtasks;
  char* pathbooks;
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
    master(numtasks, pathbooks, argv[1]);
  }
  else {
    slave(rank);
  }
}

const string STOP_CONSTRUCT = String("<stop-construct>");
const int NUMTASKs;
const int NUMBOOKs;
vector<routecell> routetable;
vector<routecell> initword;

struct column {
  string word;
  vector<routecell> nextwords;
};





