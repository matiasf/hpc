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
vector<cell> routetable;
vector<cell> initword;

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

