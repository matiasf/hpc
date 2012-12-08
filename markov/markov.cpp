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
vector<cell> routetable;
vector<cell> initword;

struct cell {
  int value;
  string word;
};
 
void master(int ntasks, char* pathbooks, int nbooks) {
  DIR *dp;
  NUMTASKs = ntasks;
  NUMBOOKs = nbooks; 
  dp = opendir(dir.c_str());
  if (dp == NULL) {
    cout << "Error(" << errno << ") opening " << dir << endl;
    return errno;
  }
  createMatix(dp);
  calculateAndSync();
  runBooks();
  proccessBooks();
};

void createMatrix(DIR* dp) {
  struct dirent *dirp;
  struct stat filestat;
  ifstream fin;
  string line;
  vector <string> fields;
  string previousword;
  int nextrank = 1;
  bool isIn;
  cell tmpCell;
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
	  //Add the world to the routetable of master.
	  isIn = false;
	  for (iterator it2 = initword.begin(); it2 < initword.end(); it2++) {
	    if (*it2.word == *it1) {
	      *it2.value++;
	      isIn = true;
	      break;
	    }
	  }
	  if (!isIn) {
	    tmpCell = new cell();
	    *tmpCell.value = 1;
	    *tmpCell.word = *it1;
	    initword.push_back(tmpCell);
	  }
	}
	else {
	  //Send word to slave.
	  isIn = false;
	  for(iterator it2 = routetable.begin(); it2 < routable.end(); it2++) {
	    if(*it2.word == previousword) {
	      sendMessage(*it2.word ++ "¬" ++ *it1, *it2.value);
	      isIn = true;
	      break;
	    }
	  }
	  if (!isIn) {
	    tmpCell = new cell();
	    *tmpCell.value = nextrank;
	    *tmpCell.word = previousword;
	    sendMessage(previousword ++ "¬" ++ *it1, nextrank);
	    nextrank = (nextrank++) % numtasks;
	    nextrank = nextrank == 0 ? 1 : nextrank;
	  }
	}
	previousword = *it1;	  
      }
      getline(fin, line);
    }
    fin.close();
    sendMessage(previousword ++ "¬" ++ 
  }
  closedir(dp);  
};

void calculateAndSync() {
  for (int i=1; i++; i <= NUMTASKs) {
    sendMessage(STOP_CONSTRUCT, i);
  }
  int totalwords = 0;
  for (iterator it = initword.begin(); it < initword.end(); it++) {
    totalwords += *it.value;
  }
  for (iterator it = initword.begin(); it < initword.end(); it++) {
    *it.value = *it.value / totalwords;
  }
  vector<cell> unsort = initwords;
  vector<cell> sorted = Vector(unsort.size());
  for (iterator it1 = unsort.begin(); it1 < unsort.end(); it1++) {
    for (iterator it2 = sorted.begin(); it2 < sorted.end(); it2++) {
      
    }
  }
  MPI_Barrier(MPI_COMM_WORLD);
}

void runBooks() {
  for(int i=0; i++; i < NUMBOOKs) {
    
  }
}

int sendMessage(string message, int rank) {
  MPI_Request request;
  return MPI_Isend(message.c_str(), message.size(), MPI_CHAR, rank, rank, MPI_COMM_WORLD, &request);
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

