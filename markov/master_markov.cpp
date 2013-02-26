#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <pthread.h>
#include <semaphore.h>
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
  int count;
};

struct slavecell {
  int rank;
  int cores;
  int count;
  sem_t mutex;
  vector<wordcell> words;
};

struct book {
  int number;
  int size;
  sem_t mutex;
  bool end;
  map<int, string> words;
};

struct buffer {
  int top;
  int twork;
  bool check;
  vector<string> queuemessage;
  sem_t emptymutex;
  sem_t fullmutex;
  sem_t buffermutex;
};
  
int NUMTASKS;
int NUMBOOKS;
vector<routecell> routetable;
vector<slavecell> slavetable;

//Constants.
string INITWORD = INIT_WORD;
string ENDWORD = END_WORD;
string STOPCONSTRUCT = STOP_CONSTRUCT;
string RESUMESLAVE = RESUME_SLAVE;
string SEPARATORM = SEPARATOR_CHAR;

//Global variables
vector<book>* books;
int inum = 0;
buffer messagebuffer;
bool tochange = false;
bool tomigrate = false;
slavecell* slavelow;
slavecell* slavebusy;
wordcell* wordbusy;
vector<wordcell>::iterator wordbusyiter;
sem_t changemutex;
sem_t waitingworkmutexm;
bool waitingWorkm = false;

void processMessage(string message, string* word, string* booknum, string* secnum, string* strrank) {
  int pos1, pos2, pos3, pos4;
  pos1 = message.find(SEPARATORM);
  *word = message.substr(0, pos1);
  pos2 = message.find(SEPARATORM, pos1+1);
  *booknum = message.substr(pos1+1, pos2-(pos1));
  *booknum = (*booknum).substr(1, (*booknum).length());
  *booknum = (*booknum).substr(0, (*booknum).length()-1);
  pos3 = message.find(SEPARATORM, pos2+1);
  *secnum = message.substr(pos2+1, pos3-(pos2));
  *secnum = (*secnum).substr(1, (*secnum).length());
  *secnum = (*secnum).substr(0, (*secnum).length()-1);
	pos4 = message.find(SEPARATORM, pos3+1);
  *strrank = message.substr(pos3+1, pos4-(pos3));
  *strrank = (*strrank).substr(1, (*strrank).length());
  *strrank = (*strrank).substr(0, (*strrank).length()-1);
}

void *reciveThread(void *voids) {
	pthread_t self = pthread_self();;
	int pos1, pos2, intrank, prom;
  string message;
  string word, booknum, secnum, strrank;
  istream *stream;
  stringstream *sstream1, *sstream2, *sstream3;
  bool wakeupmaster = false;
  while (true) {
    sem_wait(&(messagebuffer.emptymutex));
    sem_wait(&(messagebuffer.buffermutex));
		message = messagebuffer.queuemessage.front();
    messagebuffer.queuemessage.erase(messagebuffer.queuemessage.begin());
    sem_post(&(messagebuffer.buffermutex));
    processMessage(message, &word, &booknum, &secnum,&strrank);
    sstream1 = new stringstream(booknum);
    (*sstream1) >> pos1;
		sstream2 = new stringstream(strrank);
		(*sstream2) >> intrank;
		for (vector<slavecell>::iterator it1 = slavetable.begin(); it1 < slavetable.end(); it1++) {
			if(intrank == (*it1).rank) {
				sem_wait(&((*it1).mutex));
				for(vector<wordcell>::iterator it2 = (*it1).words.begin(); it2 < (*it1).words.end(); it2++){
					if(word == (*it2).word) {
						(*it1).count++;
						(*it2).count++;
						break;
					}
				}
				sem_wait(&changemutex);
				if (((*it1).count != 1) && ((*it1).count % 100) == 0 && !tochange && !tomigrate) {
					prom = 0;
					for(vector<slavecell>::iterator it2 = slavetable.begin(); it2 < slavetable.end(); it2++) {
						prom += (*it2).count;
					}
					prom /= slavetable.size();
					if ((*it1).count >= ((prom/2)+prom)) {
						for(vector<wordcell>::iterator it2 = (*it1).words.begin(); it2 < (*it1).words.end(); it2++) {
							if (((*it1).count - (*it2).count) >= prom/2 && ((*it1).count - (*it2).count) <= (prom + prom/2)) {
								for(vector<slavecell>::iterator it3 = slavetable.begin(); it3 < slavetable.end(); it3++) {
									if (((*it3).rank != (*it1).rank) && ((*it3).count + (*it2).count) <= (prom + prom/2)) {
										tochange = true;
										slavelow = &(*it3);
										slavebusy = &(*it1);
										for(vector<wordcell>::iterator it4 = (*it1).words.begin(); it4 < (*it1).words.end(); it4++){
											if(word == (*it4).word) {
												wordbusy = &(*it4);
												wordbusyiter = it4;
												break;
											}
										}										
										break;
									}
								}
								break;
							}
						}
					}
				}
				sem_post(&changemutex);
				sem_post(&((*it1).mutex));
				break;
			}
		}
    for (vector<book>::iterator it1 = (*books).begin(); it1 < (*books).end(); it1++) {
      if ((*it1).number == pos1) {
				sem_wait(&((*it1).mutex));
				sstream3 = new stringstream(secnum);
				(*sstream3) >> pos2;
				if (word == "END") {
					(*it1).size = pos2 + 1;
				}
				(*it1).words.insert(pair<int, string>(pos2, word));
				(*it1).end = (*it1).words.size() == (*it1).size;
				delete sstream3;
				sem_post(&((*it1).mutex));
				break;
      }
    }
    sem_wait(&(messagebuffer.buffermutex));
    messagebuffer.twork--;
    messagebuffer.check = false;
		if (waitingWorkm && messagebuffer.twork == 0) {
			waitingWorkm = false;
			sem_post(&(waitingworkmutexm));
		}
		sem_post(&(messagebuffer.fullmutex));
    sem_post(&(messagebuffer.buffermutex));
    delete sstream1;
		delete sstream2;
  }
}

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
  bool isinp, isina, isins;
  int rankp, ranka;
  routecell* tmproutecell;
  wordcell* tmpwordcell;
	slavecell* tmpslavecell;  
	int currentrank;
  
  while (dirp = readdir(dp)) {
    if (string(dirp->d_name) == "." || string(dirp->d_name) == "..") continue;

    filepath = pathbegin + "/" + string(dirp->d_name);   
    ifstream fin(filepath.c_str());
    if (fin.is_open()) {
    }
    else {
    }
    previousword = INITWORD;
    while (!fin.eof()) {
      isina = false;
      isinp = false;
      getline(fin, line, ' ');
      line.erase(line.find_last_not_of(" \n\r\t")+1);
			if (line == "") {
				continue;
			}
      if (previousword == INITWORD) {
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
					isins = false;					
					for (vector<slavecell>::iterator it2 = slavetable.begin(); it2 < slavetable.end(); it2++) {
						if ((*it2).rank == nextrank) {
							isins = true;
							tmpwordcell = new wordcell();
							(*tmpwordcell).count = 0;
							(*tmpwordcell).word = line;
							(*it2).words.push_back(*tmpwordcell);
							break;
						}
					}
					if (!isins){
						tmpslavecell = new slavecell();
						(*tmpslavecell).cores = 0;
						(*tmpslavecell).count = 0;
						(*tmpslavecell).rank = nextrank;
						sem_init(&((*tmpslavecell).mutex), 0, 1);
						tmpwordcell = new wordcell();
						(*tmpwordcell).count = 0;
						(*tmpwordcell).word = line;
						(*tmpslavecell).words.push_back(*tmpwordcell);
						slavetable.push_back(*tmpslavecell);
					}					
					nextrank = (nextrank+1) % NUMTASKS;
					nextrank = (nextrank == 0 ? 1 : nextrank); 
				}
				else {
					for (vector<routecell>::iterator it2 = routetable.begin(); it2 < routetable.end(); it2++) {
						if ((*it2).word == line) {
							(*it2).prob++;
							break;
						}
					}
				}
      }
      else {
				for (vector<slavecell>::iterator it2 = slavetable.begin(); it2 < slavetable.end(); it2++) {
					if (isinp && isina) {
							break;
					}
					for (vector<wordcell>::iterator it3 = (*it2).words.begin(); it3 < (*it2).words.end(); it3++) {
						if ((*it3).word == previousword) {
							isinp = true;
							rankp = (*it2).rank;
						}
						if ((*it3).word == line) {
							isina = true;
							ranka = (*it2).rank;
						}
						if (isinp && isina) {
							break;
						}
					}
				}
				if (!isina) {
					isins = false;					
					for (vector<slavecell>::iterator it2 = slavetable.begin(); it2 < slavetable.end(); it2++) {
						if ((*it2).rank == nextrank) {
							ranka = (*it2).rank;
							isins = true;
							tmpwordcell = new wordcell();
							(*tmpwordcell).count = 0;
							(*tmpwordcell).word = line;
							(*it2).words.push_back(*tmpwordcell);
							break;
						}
					}
					if (!isins){
						tmpslavecell = new slavecell();
						sem_init(&((*tmpslavecell).mutex), 0, 1);
						(*tmpslavecell).cores = 0;
						(*tmpslavecell).count = 0;
						(*tmpslavecell).rank = nextrank;
						ranka = (*tmpslavecell).rank;
						tmpwordcell = new wordcell();
						(*tmpwordcell).count = 0;
						(*tmpwordcell).word = line;
						(*tmpslavecell).words.push_back(*tmpwordcell);
						slavetable.push_back(*tmpslavecell);
					}					
					nextrank = (nextrank+1) % NUMTASKS;
					nextrank = (nextrank == 0 ? 1 : nextrank); 
				}
				sprintf(numstr, "%d", ranka);
				sendMessage(previousword + SEPARATORM + line + SEPARATORM + numstr + SEPARATORM, rankp);
			}
      previousword = line;	  
    }
    fin.close();
    sendMessage(previousword + SEPARATORM + ENDWORD + SEPARATORM + "0" + SEPARATORM, ranka);
  }
  closedir(dp);

	string coremessage,corestr;
	size_t pos1;
	for(vector<slavecell>::iterator it2 = slavetable.begin(); it2 < slavetable.end(); it2++){
		sendMessage("<get-cores>",(*it2).rank);
		coremessage = receiveMessage();
		pos1 = coremessage.find("¬");
    corestr = coremessage.substr(0, pos1);
		stringstream convertcore(corestr);
    convertcore >> (*it2).cores;
		
	}
	
};

void calculateAndSync() {
  for (int i=1; i < NUMTASKS; i++)  {
    sendMessage(STOPCONSTRUCT + SEPARATORM, i);
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
};

void runBooks() {
  float randinit;
  char numstr[21]; //Note: Enough to hold all numbers up to 64-bits  
  for(int i = 0; i < NUMBOOKS; i++) {
    randinit = (((double) rand()) / ((double)RAND_MAX));
    for(vector<routecell>::iterator it1 = routetable.begin(); it1 < routetable.end(); it1++) {
      if ((randinit = randinit - (*it1).prob) < 0) {
				sprintf(numstr, "%d", i);
				sendMessage((*it1).word + SEPARATORM + numstr + SEPARATORM + "0" + SEPARATORM, (*it1).rank);
				break;
      };      
    }
  }
};

void proccessBooks() {
  //Init books
  int inum = 0;
  int numcpu = sysconf(_SC_NPROCESSORS_ONLN);
  bool eend = false;
  vector<pthread_t*> poolthreds(0);
  for (vector<book>::iterator it1 = (*books).begin(); it1 < (*books).end(); it1++) {
    (*it1).number = inum;
    sem_init(&((*it1).mutex), 0, 1);
    (*it1).end = false;
    (*it1).size = -1;
    inum++;
  }
  messagebuffer.top = numcpu;
  messagebuffer.twork = 0;
  messagebuffer.check = true;
	sem_init(&(messagebuffer.buffermutex), 0, 1);
  sem_init(&(messagebuffer.emptymutex), 0, 0);
  sem_init(&(messagebuffer.fullmutex), 0, numcpu);
	sem_init(&(changemutex), 0, 1);
	sem_init(&(waitingworkmutexm), 0, 0);
  pthread_t* tid;
  for (int i = 0; i < numcpu; i++) {
    tid = new pthread_t(); 
    pthread_create(tid, NULL, &reciveThread, NULL);
    poolthreds.push_back(tid);
  }
  string line;
  string* message;
  string word, booknum, secnum;
  int toread;
  while (true) {
		sem_wait(&(changemutex));
		if (tochange) {
			stringstream stream;
			stream << slavelow->rank;
			sendMessage("<column-change>" + wordbusy->word + SEPARATORM + stream.str() + SEPARATORM, slavebusy->rank);			
			tochange = false;
			tomigrate = true;
		}
		sem_post(&(changemutex));
		sem_wait(&(messagebuffer.fullmutex));
    sem_wait(&(messagebuffer.buffermutex));
    if (messagebuffer.twork == 0 && messagebuffer.check) {
      sem_post(&(messagebuffer.buffermutex));
      message = new string(receiveMessage());
    }
    else if (messagebuffer.twork == 0) {
      messagebuffer.check = true;
      eend = true;
      for (vector<book>::iterator it1 = (*books).begin(); it1 < (*books).end(); it1++) {
				(*it1).end = (*it1).words.size() == (*it1).size;
				eend = (*it1).end && eend;
      }
      sem_post(&(messagebuffer.buffermutex));
      if (eend) {
				break;
			}
      else {
				sem_post(&(messagebuffer.fullmutex));
				continue;
			}
    }
    else {
      sem_post(&(messagebuffer.buffermutex));
      message = new string(receiveMessageHurry(&toread));
      if (!toread) {
				sem_wait(&(messagebuffer.buffermutex));
				if (messagebuffer.twork == 0) {
					sem_post(&(messagebuffer.buffermutex));
				}
				else {
					waitingWorkm = true;
					sem_post(&(messagebuffer.buffermutex));
					sem_wait(&(waitingworkmutexm));
				}
				sem_post(&(messagebuffer.fullmutex));
				continue;
      }
    }
		if (tomigrate && (*message).find_last_of(SEPARATORM) >= 13 && (*message).substr(0, 14).compare("<column-ready>") == 0) {
			sem_post(&(messagebuffer.fullmutex));
			sendMessage("<column-ready>", slavebusy->rank);
			delete message;
		}
		else if (tomigrate && (*message).find_last_of(SEPARATORM) >= 16 && (*message).substr(0, 17).compare("<column-disabled>") == 0) {
			sem_post(&(messagebuffer.fullmutex));
			slavelow->words.push_back(*wordbusy);
			slavebusy->count -= wordbusy->count;
			slavelow->count += wordbusy->count;
			stringstream stream;
			stream << slavelow->rank;
			for (vector<slavecell>::iterator it1 = slavetable.begin(); it1 < slavetable.end(); it1++) {
				sendMessage("<column-broadcast>" + wordbusy->word + SEPARATORM + stream.str() + SEPARATORM, (*it1).rank);
			}
			slavebusy->words.erase(wordbusyiter);
			tomigrate = false;
			delete message;
		}
		else if ((*message).find_last_of(SEPARATORM) >= 11 && (*message).substr(0, 12).compare("<word-wrong>") == 0) {
			sem_post(&(messagebuffer.fullmutex));
			int pos1 = (*message).find(SEPARATORM);
			string wrongword = (*message).substr(11, pos1-10);
			wrongword = wrongword.substr(1, wrongword.length());
			wrongword = wrongword.substr(0, wrongword.length()-1);
			int endSep = (*message).find(SEPARATORM, (*message).find(SEPARATORM, pos1+1)+1);
			bool isin = false;
			for (vector<slavecell>::iterator it1 = slavetable.begin(); it1 < slavetable.end(); it1++) {
				for (vector<wordcell>::iterator it2 = (*it1).words.begin(); it2 < (*it1).words.end(); it2++) {
					if ((*it2).word == wrongword) {
						string mess = (*message).substr(12, endSep-10);
						sendMessage(mess, (*it1).rank);
						isin = true;
						break;
					}
				}
				if (isin)
					break;
			}
			delete message;
		}
		else {
			sem_wait(&(messagebuffer.buffermutex));
			messagebuffer.queuemessage.push_back(*message);
			messagebuffer.twork++;
			sem_post(&(messagebuffer.emptymutex));
			sem_post(&(messagebuffer.buffermutex));
		}
	}
  string resume = RESUMESLAVE;
  for (int i1=1; i1 < NUMTASKS; i1++) {
    sendMessage(resume + SEPARATORM, i1);
  }
  
};

void master(int ntasks, const char* pathbooks, int nbooks) {
  DIR *dp;
  NUMTASKS = ntasks;
  NUMBOOKS = nbooks;
  books = new vector<book>(NUMBOOKS);
  dp = opendir(pathbooks);
  if (dp == NULL) {
    return;
  }
  string spath(pathbooks);
  srand(time(NULL));
  createMatrix(dp, spath);
  calculateAndSync();
  runBooks();
	proccessBooks();
	
	MPI_Finalize();
  return;
};
