#include <dirent>
#include <unistd>
#include <string>
#include <vector>
#include <sys/stat>
#include <sys/types>

#include "mpi.h"

#define MASTER_RANK 0
#define BUFFER_MESSAGE 50
#define STOP_CONSTRUCT "<stop-construct>"
#define READY_TO_RUN "<ready-to-run>"
#define INIT_WORD "INIT"

using namespace std;

vector<string> initword;

struct cell {
  int value;
  string word;
}

int main(int argc, char *argv[]) {zzxa
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
sdf    }
    master(numtasks, pathbooks);
  }
  else {
    slave(rank);
  }
}

void master(int numtasks, char* pathbooks) {
  DIR *dp;
  dp = opendir(dir.c_str());
  if (dp == NULL) {
    cout << "Error(" << errno << ") opening " << dir << endl;
    return errno;
  }
  else {
    struct dirent *dirp;
    struct stat filestat;
    ifstream fin;
    string line;
    vector <string> fields;
    vector<cell> routetable;
    string previousword;
    int nextrank;

    while (dirp = readdir(dp)) {
      filepath = pathbooks + "/" + dirp->d_name;

      if (stat(filepath.c_str(), &filestat)) continue;
      if (S_ISDIR(filestat.st_mode)) continue;

      fin.open(filepath.c_str());
      previousword = INIT_WORD;
      while (line)
      getline(fin, line);
      split(fields, line, ' ');      
      for (iterator it1 = fields.begin(); it1 < fields.end(); it1++) {
	if (previousword == INIT_WORD) {
	  for (iterator it2 = initword.begin(); it2< initword.end(); it2++) {
	    if (*it2.word == *it1) {
	      *it2.prob++;
	    }
	    else {
	      *it2.word = *it1;
	      *it2.prob = 1;
	    }
	  }
	  previousword = *it1;
	}
	else {
	  //Enviar palabra!
	  for(iterator it3 = routetable.begin(); it3 < routable.end(); it3++) {
	    if(*it3.word == *it1) {
	      ranknext = *it3.prob; 
	    }
	  }

	  MPI_send();
	  MPI_Request request;
	  ierr = MPI_Isend(it2,count, MPI_CHAR,nextrank,rank, MPI_COMM_WORLD, request);

	}
      }
      fin.close();
    }
    closedir( dp );
  }
}

struct columns {
  char* word;
  vector<cell> nextwords;
};

void slave(int rank) {
  char* word;
  boolean construct = true;
  vector<columns> columns;
  while (construct) {
    MPI_Recv(word, BUFFER_MESSAGE, MPI_CHAR, MASTER_RANK, MASTER_RANK, MPI_COMM_WORLD, &status);
    if (strcmp(word, STOP_CONSTRUCT) == 0) {
      construct = false;
    }
    else {
      
    }
  } 
}
