#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include <vector>
#include <map>

#include "mpi.h"
#include "utils.h"

#define PATH_BOOKS "/tmp" 
#define INIT_WORD "INIT";
#define END_WORD "END";
#define STOP_CONSTRUCT "<stop-construct>";
#define RESUME_SLAVE "<resume-slave>";
#define SEPARATOR "¬";

using namespace std;

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
};
  
int NUMTASKS;
int NUMBOOKS;
vector<routecell> routetable;
vector<wordcell> wordtable;

void createMatrix(DIR* dp) {
  //Parser Variables.
  struct dirent *dirp;
  struct stat filestat;
  ifstream fin;
  string line, filepath, pathbooks;
  vector <string> fields;
  char numstr[21]; //Note: Enough to hold all numbers up to 64-bits
  string convstr;
  pathbooks = string(PATH_BOOKS);

  //Table Variables.
  string previousword;
  int nextrank = 1;
  bool isinp, isina;
  int rankp, ranka;
  routecell* tmproutecell;
  wordcell* tmpwordcell;
  int currentrank;

  while (dirp = readdir(dp)) {
    filepath = pathbooks + "/" + string(dirp->d_name);
    
    //If not is a valid file continue.
    if (stat(filepath.c_str(), &filestat)) continue;
    if (S_ISDIR(filestat.st_mode)) continue;
    
    fin.open(filepath.c_str());
    previousword = INIT_WORD;
    getline(fin, line, ' ');
    while (fin) {
      for (vector<string>::iterator it1 = fields.begin(); it1 < fields.end(); it1++) {
	for (vector<routecell>::iterator it2 = routetable.begin(); it2 < routetable.end(); it2++) {
	  //FIXME: Can't use the constant INIT_WORD
	  if ((*it2).word == previousword && previousword == "INIT") {
	    isinp = true;
	    rankp = (*it2).rank;
	  }
	  else if ((*it2).word == *it1) {
	    isina = true;
	    ranka = (*it2).rank;
	  }
	  if (isinp && isina) {
	    break;
	  }
	}
	if (previousword == "INIT") {
	  if (!isina) {
	    tmproutecell = (struct routecell *) malloc(sizeof(struct routecell));
	    (*tmproutecell).prob = 1;
	    (*tmproutecell).word = *it1;
	    (*tmproutecell).rank = nextrank;
	    routetable.push_back(*tmproutecell);
	    nextrank = (nextrank++) % NUMTASKS;
	    nextrank = nextrank == 0 ? 1 : nextrank; 
	  }
	  else {
	    for (vector<routecell>::iterator it2 = routetable.begin(); it2 < routetable.end(); it2++) {
	      if ((*it2).word == *it1) {
		(*it2).prob++;
		break;
	      }
	    }
	  }
	}
	else {
	  if (!isina) {
	    tmpwordcell = (struct wordcell *) malloc(sizeof(struct wordcell));
	    (*tmpwordcell).word = *it1;
	    (*tmpwordcell).rank = nextrank;
	    ranka = (*tmpwordcell).rank;
	    nextrank = (nextrank++) % NUMTASKS;
	    nextrank = nextrank == 0 ? 1 : nextrank; 
	  }
	  sprintf(numstr, "%d", ranka);
	  sendMessage(previousword + "¬" + (*it1) + "¬" + numstr, rankp);
	}
	previousword = *it1;	  
      }
      getline(fin, line, ' ');
    }
    fin.close();
    sendMessage(previousword + "¬END¬0", rankp);
  }
  closedir(dp);
  cout << "Matrix ended.";
};

void calculateAndSync() {
  for (int i=1; i++; i <= NUMTASKS) {
    sendMessage("<stop-construct>", i);
  }
  int totalwords = 0;
  for (vector<routecell>::iterator it = routetable.begin(); it < routetable.end(); it++) {
    totalwords += (*it).prob;
  }
  for (vector<routecell>::iterator it = routetable.begin(); it < routetable.end(); it++) {
    (*it).prob = (*it).prob / totalwords;
  }
  vector<routecell> unsort = routetable;
  vector<routecell> sorted(unsort.size());
  bool isIn = false;
  for (vector<routecell>::iterator it1 = unsort.begin(); it1 < unsort.end(); it1++) {
    for (vector<routecell>::iterator it2 = sorted.begin(); it2 < sorted.end(); it2++) {
      if ((*it2).prob > (*it1).prob) {
	sorted.insert(it2, *it1);
	isIn = true;
	break;
      }
    }
    if (!isIn) {
      sorted.push_back(*it1);
    }
  }
  routetable = sorted;
  MPI_Barrier(MPI_COMM_WORLD);
};

void runBooks() {
  float randinit;
  char numstr[21]; //Note: Enough to hold all numbers up to 64-bits  
  for(int i = 0; i++; i < NUMBOOKS) {
    randinit = rand() / RAND_MAX;
    for(vector<routecell>::iterator it1 = routetable.begin(); it1 < routetable.end(); it1++) {
      if ((randinit = randinit - (*it1).prob) < 0) {
	sprintf(numstr, "%d", i);
	sendMessage((*it1).word + "¬" + numstr + "¬0", (*it1).rank);
	sendMessage((*it1).word + "¬" + numstr + "¬0", 0);
	break;
      };      
    }
  } 
};

void proccessBooks() {
  //Init books
  int numbook = 0;
  int i = 0;
  vector<book> books(NUMBOOKS);
  for (vector<book>::iterator it1 = books.begin(); it1 < books.end(); it1++) {
    (*it1).number = i;
    i++;
  }
  
  string line;
  vector<string> fields;
  string message;
  string word, booknum, secnum;
  istream *stream;
  int pos1, pos2, endbooks = 0;
  stringstream *sstream1, *sstream2;
  while (endbooks < NUMBOOKS) {
    message = receiveMessage();
    //TODO: Fork to wait more messages and process.
    pos1 = message.find("¬");
    word = message.substr(0, pos1-1);
    pos2 = message.find("¬", pos1+1);
    booknum = message.substr(pos1+1, pos2-1);
    secnum = message.substr(pos2+1);
    if (word != "END") {
      endbooks++;
    }
    else {
      sstream1 = new stringstream(secnum);
      (*sstream1) >> pos1;      
      for (vector<book>::iterator it1 = books.begin(); it1 < books.end(); it1++) {
	if ((*it1).number == pos1) {
	  sstream2 = new stringstream(secnum);
	  (*sstream2) >> pos2;
	  (*it1).words.insert(pair<int, string>(pos2, word));
	  delete sstream2;
	  break;
	}	
      }
      delete sstream1;
    }
  }
  string resume = RESUME_SLAVE;
  for (int i1=1; i1++; i1 <= NUMTASKS) {
    sendMessage(resume, i1);
  }
  //TODO: Write books.
  for (vector<book>::iterator it1 = books.begin(); it1 < books.end(); it1++) {
    cout << "Book number " << (*it1).number << ".\n";
    for (int i1 = 0; i1++; i1 < (*it1).words.size()) {
      cout << (*it1).words[i1] << " ";
    }
  }
};

void master(int ntasks, const char* pathbooks, int nbooks) {
  DIR *dp;
  NUMTASKS = ntasks;
  NUMBOOKS = nbooks;
  dp = opendir(pathbooks);
  if (dp == NULL) {
    cout << "Error opening " << pathbooks << endl;
    return;
  }
  createMatrix(dp);
  //  calculateAndSync();
  //runBooks();
  //proccessBooks();
};
