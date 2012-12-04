#include <dirent>
#include <unistd>
#include <string>
#include <vector>
#include <sys/stat>
#include <sys/types>

#include "mpi.h"

#define MASTER_RANK 0
#define BUFFER_MESSAGE 50
#define "<stop-construct>"
#define "<ready-to-run>"
#define INIT_WORD "INIT"



using namespace std;

vector<string> initword;

struct cell {
  int value;
  string word;
}

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
  else if (createMatix(dp)) {
  }
};

int createMatrix(DIR* dp) {
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

    //If not is a valid file continue.
    if (stat(filepath.c_str(), &filestat)) continue;
    if (S_ISDIR(filestat.st_mode)) continue;
    
    fin.open(filepath.c_str());
    previousword = INIT_WORD;
    getline(fin, line);
    while (line) {
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
      getline(fin, line);
    }
    fin.close();
  }
  closedir( dp );
};

int sendMessage(char* buffer, int length, int rank) {
  MPI_Request request;
  return MPI_Isend(buffer, length, MPI_CHAR, rank, rank, MPI_COMM_WORLD, &request);
}

string receiveMessage() {
	MPI_Recv(word, BUFFER_MESSAGE, MPI_CHAR, MPI_ANY_SOURCE,MPI_ANY_SOURCE, MPI_COMM_WORLD, &status);
	return string(word); 
}

struct columns {
  string word;
  vector<cell> nextwords;
};

void slave(int rank) {
	char* word;
	boolean construct = true;
	vector<columns> columns;
	while (construct) {
		word = receiveMessage();
		if (strcmp(word, STOP_CONSTRUCT) == 0) {
			construct = false;	
		}
		else {
			addWord(word,columns);
		}
	}

	
	while(){

		message = receiveMessage();
		
		if (strcmp(word, BALANCE) == 0) {
		}
		else if (strcmp(word, THE_END) == 0) {
		}
		else {
			readMessage(message,seqNum,word,bookNum);
			cell = searchNextWord(word,vector);
			masterMessage = createMessage(seqNum,word,bookNum);
			slaveMessage  = createMessage(seqNum+1,word,bookNum);
			sendMessage(masterMessage, masterMessage.length(), cell.value);
			sendMessage(slaveMessage, slaveMessage.length(), MASTER_RANK);
		}	
	}
}

void addWord(word,columns) {
	
	notInWord1 = true;
	notInWord2 = true;
	readMessage(word,word1,word2);
	for (iterator it1 = column.begin(); it1 < column.end(); it1++) {
		if(word1.compare(*it1.word)==0) {
			notInWord1 = false;
			for(iterator it2 = nextwords.begin(); it2 < nextwords.end(); it2++) {
				if(word2.compare(*it2.word)==0) {
					*it2.value += 1;
					notInWord2 = false;
					break;
				}
			}		
			if(notIn) {
				cell c = cell();
				c.
				*it1.nextwords.push_back(new cell);
			}
			break;
		}
	}
}

string createMessage(seqNum,word,bookNum) {
}

void readMessage(message,seqNum,word,bookNum) {
}

cell searchNextWord(word,vector) {
}

