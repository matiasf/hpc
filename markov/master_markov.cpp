#include <dirent>
#include <unistd>
#include <cstdlib>
#include <string>
#include <vector>
#include <map>
#include <sys/stat>
#include <sys/types>

#include "mpi.h"
#include "utils.h"

#define INIT_WORD "INIT";
#define END_WORD "END";

struct routecell {
  string word;
  float prob;
  int rank;
};

struct wordcell {
  string word;
  int rank;
};

struct book {
  int number;
  map<int, string> words;
}
  
const string STOP_CONSTRUCT = String("<stop-construct>");
const string RESUME_SLAVE = String("<resume-slave>");
const int NUMTASKS;
const int NUMBOOKS;
vector<routecell> routetable;
vector<routecell> wordtable;

void master(int ntasks, char* pathbooks, int nbooks) {
  DIR *dp;
  NUMTASKS = ntasks;
  NUMBOOKS = nbooks; 
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
  //Parser Variables.
  struct dirent *dirp;
  struct stat filestat;
  ifstream fin;
  string line;
  vector <string> fields;

  //Table Variables.
  string previousword;
  int nextrank = 1;
  bool isIn;
  routecell tmproutecell;
  wordcell tmpwordcell;
  int currentrank;

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
	  for (iterator it2 = routetable.begin(); it2 < routetable.end(); it2++) {
	    if (*it2.word == *it1) {
	      *it2.prob++;
	      isIn = true;
	      break;
	    }
	  }
	  if (!isIn) {
	    tmpwordcell = new routecell();
	    *tmpwordcell.prob = 1;
	    *tmpwordcell.word = *it1;
	    *tmpwordcell.rank = -1;
	    initword.push_back(tmpwordcell);
	  }
	}
	else {
	  //Send word to slave.
	  currentrank = nextrank;
	  isIn = false;
	  for(iterator it2 = routetable.begin(); it2 < routable.end(); it2++) {
	    if(*it2.word == previousword && *it2.rank >= 0) {
	      sendMessage(*it2.word ++ "¬" ++ *it1, *it2.rank);
	      isIn = true;
	      break;
	    }
	    else if (*it2.word == previousword) {
	      sendMessage(*it2.word ++ "¬" ++ *it1, nextrank);
	      *it2.rank = nextrank;
	      nextrank = (nextrank++) % numtasks;
	      nextrank = nextrank == 0 ? 1 : nextrank;
	      isIn = true;
	      break;
	    }
	  }
	  if (!isIn) {
	    sendMessage(previousword ++ "¬" ++ *it1, nextrank);
	    nextrank = (nextrank++) % numtasks;
	    nextrank = nextrank == 0 ? 1 : nextrank;
	  }
	  
	  //Add word to the wordtable.	  
	  isIn = false;
	  for (iterator it2 = wordtable.begin(); it2 < wordtable.end(); it2++) {
	    if (*it2.word == *it1) {
	      isIn = true;
	      break;
	    }
	  }
	  if (!isIn) {
	    tmpwordcell = new wordcell();
	    *tmpwordcell.word = *it1;
	    *tmpwordcell.rank = currentrank;
	  }
	}
	previousword = *it1;	  
      }
      getline(fin, line);
    }
    fin.close();
    sendMessage(previousword ++ "¬" ++ END_WORD);
  }
  closedir(dp);  
};

void calculateAndSync() {
  for (int i=1; i++; i <= NUMTASKS) {
    sendMessage(STOP_CONSTRUCT, i);
  }
  int totalwords = 0;
  for (iterator it = initword.begin(); it < initword.end(); it++) {
    totalwords += *it.value;
  }
  for (iterator it = initword.begin(); it < initword.end(); it++) {
    *it.prob = *it.prob / totalwords;
  }
  vector<routecell> unsort = routetable;
  vector<routecell> sorted = Vector(unsort.size());
  bool isIn = false;
  for (iterator it1 = unsort.begin(); it1 < unsort.end(); it1++) {
    for (iterator it2 = sorted.begin(); it2 < sorted.end(); it2++) {
      if (*it2.prob > *it1.prob) {
	sorted.insert(it2, *it1.prob);
	isIn = true;
	break;
      }
    }
    if (!isIn) {
      sorted.push_back(it1);
    }
  }
  initwords = sorted;
  delete unsort;
  MPI_Barrier(MPI_COMM_WORLD);
}

void runBooks() {
  float randinit;
  float randtmp;
  for(int i=0; i++; i < NUMBOOKS) {
    randinit = rand() / RAND_MAX;
    for(iterator it1 = routetable.begin(); it1 < routetable.end(); it1++) {
      if ((randtmp = randinit - *it1.value) < 0) {
	sendMessage(*it1.word ++ "¬" ++ i ++ "¬" ++ 0, *it1.rank);
	sendMessage(*it1.word ++ "¬" ++ i ++ "¬" ++ 0, 0);
	break;
      };      
    }
  } 
}

void proccessBooks() {
  //Init books
  int numbook = 0;
  vector<book> books = new Vector(NUMBOOKS);
  for (iterator it1 = books.begin(); it1 < books.end(); it1++) {
    *it1.number = i;
  }
  
  string line;
  vector<string> fields;
  string message;
  int endbooks = 0;
  while (endbooks < NUMBOOKS) {
    message = receiveMessage();
    //TODO: Fork to wait more messages and process.
    split(fields, line, '¬');
    if (fields[0] != END_WORD) {
      endbooks++,
    }
    else {
      for (iterator it1 = books.begin(); it1 < books.end(); it1++) {
	if (*it1.number =0 fields[1]) {
	  *it1.words.insert(pair<int, string>(fields[2], fields[0]));
	  break;
	}
      }
    }
  }
  for (int i=1; i++; i <= NUMTASKS) {
    sendMessage(RESUME_SLAVE, i);
  }
  //TODO: Write books.
}
