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

#define INIT_WORD "INIT";
#define END_WORD "END";
#define STOP_CONSTRUCT "<stop-construct>";
#define RESUME_SLAVE "<resume-slave>";
#define SEPARATOR_CHAR "¬";

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
  int size;
  int sec;
  bool end;
  map<int, string> words;
};
  
int NUMTASKS;
int NUMBOOKS;
vector<routecell> routetable;
vector<wordcell> wordtable;

//Constants.
string INITWORD = INIT_WORD;
string ENDWORD = END_WORD;
string STOPCONSTRUCT = STOP_CONSTRUCT;
string RESUMESLAVE = RESUME_SLAVE;
string SEPARATOR = SEPARATOR_CHAR;

void createMatrix(DIR* dp, string pathbegin) {
  //Parser Variables.
  struct dirent *dirp;
  struct stat filestat;
  string line, filepath;
  vector <string> fields;
  char numstr[21]; //Note: Enough to hold all numbers up to 64-bits
  string convstr;

  //Table Variables.
  string previousword;
  int nextrank = 1;
  bool isinp, isina;
  int rankp, ranka;
  routecell* tmproutecell;
  wordcell* tmpwordcell;
  int currentrank;
  
  //////////cerr << "Master: Creating matrix....\n";
  while (dirp = readdir(dp)) {
    //////////cerr << "Master: Check if is a valid book.\n";
    if (string(dirp->d_name) == "." || string(dirp->d_name) == "..") continue;

    filepath = pathbegin + "/" + string(dirp->d_name);   
    //////////cerr << "Master: Reading book " << filepath << endl;
    ifstream fin(filepath.c_str());
    if (fin.is_open()) {
      //////////cerr << "Master: The file is ok!" << endl;
    }
    else {
      //////////cerr << "Master: Bad file" << endl; 
    }
    previousword = INITWORD;
    while (!fin.eof()) {
      isina = false;
      isinp = false;
      getline(fin, line, ' ');
      line.erase(line.find_last_not_of(" \n\r\t")+1);
      //////////cerr << "Master: Readed word " << line << endl;
      if (previousword == INITWORD) {
	//////////cerr << "Master: First word of book" << endl; 
	for (vector<routecell>::iterator it2 = routetable.begin(); it2 < routetable.end(); it2++) {
	  if ((*it2).word == line) {
	    isina = true;
	    ranka = (*it2).rank;
	    break;
	  }
	}
	if (!isina) {
	  tmproutecell = new routecell();
	  (*tmproutecell).prob = 1;
	  (*tmproutecell).word = line;
	  (*tmproutecell).rank = nextrank;
	  routetable.push_back(*tmproutecell);
	  tmpwordcell = new wordcell();
	  (*tmpwordcell).word = line;
	  (*tmpwordcell).rank = nextrank;
	  wordtable.push_back(*tmpwordcell);
	  nextrank = (nextrank+1) % NUMTASKS;
	  nextrank = (nextrank == 0 ? 1 : nextrank); 
	  ////////cerr << "Master: New word to master " << (*tmpwordcell).word << " with rank " << (*tmpwordcell).rank << endl;
	}
	else {
	  for (vector<routecell>::iterator it2 = routetable.begin(); it2 < routetable.end(); it2++) {
	    if ((*it2).word == line) {
	      (*it2).prob++;
	      //////////cerr << "Master: Know word " << line << " - Quantity " << (*it2).prob << endl;
	      break;
	    }
	  }
	}
      }
      else {
	////////cerr << "Master: Serching previousword " << previousword << " and actualword " << line << endl;
	for (vector<wordcell>::iterator it2 = wordtable.begin(); it2 < wordtable.end(); it2++) {
	  if ((*it2).word == previousword) {
	    isinp = true;
	    rankp = (*it2).rank;
	    ////////cerr << "Master: Rank p is " << rankp << endl;
	  }
	  else if ((*it2).word == line) {
	    isina = true;
	    ranka = (*it2).rank;
	    ////////cerr << "Master: Rank a is " << ranka << endl;
	  }
	  if (isinp && isina) {
	    break;
	  }
	}
	if (!isina) {
	  tmpwordcell = new wordcell();
	  (*tmpwordcell).word = line;
	  (*tmpwordcell).rank = nextrank;
	  ranka = (*tmpwordcell).rank;
	  wordtable.push_back(*tmpwordcell);
	  nextrank = (nextrank+1) % NUMTASKS;
	  nextrank = nextrank == 0 ? 1 : nextrank;
	  ////////cerr << "Master: New word to slave " << (*tmpwordcell).word << " with rank " << (*tmpwordcell).rank << endl;
	}
	sprintf(numstr, "%d", ranka);
	//////cerr << "Master: Sending " << (previousword + SEPARATOR + line + SEPARATOR + numstr + SEPARATOR) << " to " << rankp << endl;
	sendMessage(previousword + SEPARATOR + line + SEPARATOR + numstr + SEPARATOR, rankp);
      }
      previousword = line;	  
    }
    fin.close();
    //////cerr << "Master: Sending end of the book " << (previousword + SEPARATOR + ENDWORD + SEPARATOR + "0" + SEPARATOR) << " to " << rankp << endl;
    sendMessage(previousword + SEPARATOR + ENDWORD + SEPARATOR + "0" + SEPARATOR, ranka);
  }
  closedir(dp);
  //////////cerr << "Master: Matrix ended.\n";
};

void calculateAndSync() {
  //////////cerr << "Master: Calculating and sync for " << NUMTASKS << " tasks" << endl;
  for (int i=1; i < NUMTASKS; i++)  {
    //////////cerr << "Master: Sending end construct to " << i << endl;
    sendMessage(STOPCONSTRUCT + SEPARATOR, i);
  }
  int totalwords = 0;
  for (vector<routecell>::iterator it = routetable.begin(); it < routetable.end(); it++) {
    totalwords += (*it).prob;
  }
  for (vector<routecell>::iterator it = routetable.begin(); it < routetable.end(); it++) {
    (*it).prob = (*it).prob / totalwords;
  }
  vector<routecell> unsort = routetable;
  vector<routecell> sorted(0);
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
  ////////cerr << "Master: Calculated probabilities and sync after barrier" << endl;
};

void runBooks() {
  float randinit;
  char numstr[21]; //Note: Enough to hold all numbers up to 64-bits  
  ////////cerr << "Master: Start to create books " << NUMBOOKS << endl;
  for(int i = 0; i < NUMBOOKS; i++) {
    randinit = (((double) rand()) / ((double)RAND_MAX));
    ////////cerr << "Master: Creating book number " << i << endl;
    for(vector<routecell>::iterator it1 = routetable.begin(); it1 < routetable.end(); it1++) {
      if ((randinit = randinit - (*it1).prob) < 0) {
	sprintf(numstr, "%d", i);
	////////cerr << "Master: Sending " << (*it1).word << " to " << (*it1).rank << endl;
	sendMessage((*it1).word + SEPARATOR + numstr + SEPARATOR + "0" + SEPARATOR, (*it1).rank);
	sendMessage((*it1).word + SEPARATOR + numstr + SEPARATOR + "0" + SEPARATOR, 0);
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
    (*it1).end = false;
    i++;
  }
  
  string line;
  vector<string> fields;
  string message;
  string word, booknum, secnum;
  istream *stream;
  int pos1, pos2, pos3, endbooks = 0;
  stringstream *sstream1, *sstream2;
  bool fend = false;
  bool eend = false;
  while (endbooks < NUMBOOKS) {
    message = receiveMessage();
    //cerr << "Master: Message recived " << message << endl;
    //TODO: Fork to wait more messages and process.
    pos1 = message.find(SEPARATOR);
    word = message.substr(0, pos1);
    ////////cerr << "Master: Word recived " << word << endl;
    pos2 = message.find(SEPARATOR, pos1+1);
    booknum = message.substr(pos1+1, pos2-(pos1));
    booknum = booknum.substr(1, booknum.length());
    booknum = booknum.substr(0, booknum.length()-1);
    ////////cerr << "Master: Num book " << booknum << endl;
    pos3 = message.find(SEPARATOR, pos2+1);
    secnum = message.substr(pos2+1, pos3-(pos2));
    secnum = secnum.substr(1, secnum.length());
    secnum = secnum.substr(0, secnum.length()-1);
    ////////cerr << "Master: Sec num " << secnum << endl;
    if (word == "END") {
      //////cerr << "Master: End " << endbooks << endl;
      endbooks++;
      (*sstream1) >> pos1;
      for (vector<book>::iterator it1 = books.begin(); it1 < books.end(); it1++) {
	if ((*it1).number == pos1) {
	  (*sstream2) >> pos2;
	  (*it1).end = true;
	  (*it1).size = pos2;
	}
      }
      fend = true;
    }
    else {
      ////////cerr << "Master: Book message " << booknum << endl;
      sstream1 = new stringstream(booknum);
      (*sstream1) >> pos1;
      for (vector<book>::iterator it1 = books.begin(); it1 < books.end(); it1++) {
	////////cerr << "Master: Checking with book " << (*it1).number << endl;
	if ((*it1).number == pos1) {
	  sstream2 = new stringstream(secnum);
	  (*sstream2) >> pos2;
	  (*it1).words.insert(pair<int, string>(pos2, word));
	  (*it1).sec = pos2 > (*it1).sec ? pos2 : (*it1).sec;
	  delete sstream2;
	  break;
	}	
      }
      delete sstream1;
      if (fend) {
	//
      }
    }
  }
  //////cerr << "Master: Resume slaves" << endl;
  string resume = RESUMESLAVE;
  for (int i1=1; i1 < NUMTASKS; i1++) {
    sendMessage(resume + SEPARATOR, i1);
  }
  //TODO: Write books.
  for (vector<book>::iterator it1 = books.begin(); it1 < books.end(); it1++) {
    cerr << "Master: Book number " << (*it1).number << endl;
    for (int i1 = 0; i1 < (*it1).words.size(); i1++) {
      cerr << (*it1).words[i1] << " ";
    }
    cerr << endl;
  }
};

void master(int ntasks, const char* pathbooks, int nbooks) {
  DIR *dp;
  NUMTASKS = ntasks;
  //////////cerr << "Master: Number of tasks created " << ntasks << ", and " << nbooks << " books." << endl;
  NUMBOOKS = nbooks;
  dp = opendir(pathbooks);
  if (dp == NULL) {
    ////////cerr << "Error opening " << pathbooks << endl;
    return;
  }
  string spath(pathbooks);
  srand(time(NULL));
  createMatrix(dp, spath);
  calculateAndSync();
  ////cerr << "Master: Start to run books." << endl;
  //  for (vector<routecell>::iterator it2 = routetable.begin(); it2 < routetable.end(); it2++) {
  //    ////////cerr << "\tprob - " << (*it2).prob << "\tword - " << (*it2).word << "\trank - " << (*it2).rank << endl;
  //  }
  runBooks();
  proccessBooks();
  //////cerr << "Master: Befor barrier." << endl;
  MPI_Finalize();
  //err << "Master: End." << endl;
  return;
};
