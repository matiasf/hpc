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
  //////////////////////cerr << "Master: Word recived " << word << endl;
  pos2 = message.find(SEPARATORM, pos1+1);
  *booknum = message.substr(pos1+1, pos2-(pos1));
  *booknum = (*booknum).substr(1, (*booknum).length());
  *booknum = (*booknum).substr(0, (*booknum).length()-1);
  //////////////////////cerr << "Master: Num book " << booknum << endl;
  pos3 = message.find(SEPARATORM, pos2+1);
  *secnum = message.substr(pos2+1, pos3-(pos2));
  *secnum = (*secnum).substr(1, (*secnum).length());
  *secnum = (*secnum).substr(0, (*secnum).length()-1);
	//////////////////////cerr << "Master: Sec num " << secnum << endl;
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
  stringstream *sstream1, *sstream2;
  bool wakeupmaster = false;
  while (true) {
    ////cerr << "Master - Thread " << self << " : Waiting for job" << endl;
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
		////cerr << "Master - Thread " << self << " : Checking counts and busy" << endl;
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
					////cerr << "Master - Thread " << self << " : Checking busy " << (*it1).count << " of word " << word << endl;
					prom = 0;
					for(vector<slavecell>::iterator it2 = slavetable.begin(); it2 < slavetable.end(); it2++) {
						prom += (*it2).count;
					}
					prom /= slavetable.size();
					////cerr << "Master - Thread " << self << " : Prom " << prom << endl;
					if ((*it1).count >= ((prom/2)+prom)) {
						////cerr << "Master - Thread " << self << " : Busy and searching" << endl;
						for(vector<wordcell>::iterator it2 = (*it1).words.begin(); it2 < (*it1).words.end(); it2++) {
							if (((*it1).count - (*it2).count) >= prom/2 && ((*it1).count - (*it2).count) <= (prom + prom/2)) {
								////cerr << "Master - Thread " << self << " : More busy" << endl;
								for(vector<slavecell>::iterator it3 = slavetable.begin(); it3 < slavetable.end(); it3++) {
									if (((*it3).rank != (*it1).rank) && ((*it3).count + (*it2).count) <= (prom + prom/2)) {
										tochange = true;
										////cerr << "Master - Thread " << self << " : Busy!" << endl;
										slavelow = &(*it3);
										slavebusy = &(*it1);
										for(vector<wordcell>::iterator it4 = (*it1).words.begin(); it4 < (*it1).words.end(); it4++){
											if(word == (*it4).word) {
												////cerr << "Master - Thread " << self << " : Word busy! " << word << endl;
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
		////cerr << "Master - Thread " << self << " : Put message on book" << endl;
    for (vector<book>::iterator it1 = (*books).begin(); it1 < (*books).end(); it1++) {
      if ((*it1).number == pos1) {
				sem_wait(&((*it1).mutex));
				////////////cerr << "Master - Thread: Checking with book " << (*it1).number << endl;
				sstream2 = new stringstream(secnum);
				(*sstream2) >> pos2;
				if (word == "END") {
					////////////cerr << "Master - Thread: End! with size - " << pos2 << endl;
					(*it1).size = pos2 + 1;
				}
				(*it1).words.insert(pair<int, string>(pos2, word));
				(*it1).end = (*it1).words.size() == (*it1).size;
				delete sstream2;
				sem_post(&((*it1).mutex));
				break;
      }
    }
    sem_wait(&(messagebuffer.buffermutex));
    messagebuffer.twork--;
    messagebuffer.check = false;
		cerr << "Master - Thread " << self << " : Ending proccess" << endl;
		if (waitingWorkm && messagebuffer.twork == 0) {
			cerr << "Master - Thread  " << self << ": Unlocking waiting master." << endl;
			waitingWorkm = false;
			sem_post(&(waitingworkmutexm));
		}
		sem_post(&(messagebuffer.fullmutex));
    sem_post(&(messagebuffer.buffermutex));
    delete sstream1;
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
  
  ////////////////////////cerr << "Master: Creating matrix....\n";
  while (dirp = readdir(dp)) {
    ////////////////////////cerr << "Master: Check if is a valid book.\n";
    if (string(dirp->d_name) == "." || string(dirp->d_name) == "..") continue;

    filepath = pathbegin + "/" + string(dirp->d_name);   
    ////////////////////////cerr << "Master: Reading book " << filepath << endl;
    ifstream fin(filepath.c_str());
    if (fin.is_open()) {
      ////////////////////////cerr << "Master: The file is ok!" << endl;
    }
    else {
      ////////////////////////cerr << "Master: Bad file" << endl; 
    }
    previousword = INITWORD;
    while (!fin.eof()) {
      isina = false;
      isinp = false;
      getline(fin, line, ' ');
      line.erase(line.find_last_not_of(" \n\r\t")+1);
      //////cerr << "Master: Readed word " << line << endl;
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
					//////////////////////cerr << "Master: New word to master " << (*tmpwordcell).word << " with rank " << (*tmpwordcell).rank << endl;
				}
				else {
					for (vector<routecell>::iterator it2 = routetable.begin(); it2 < routetable.end(); it2++) {
						if ((*it2).word == line) {
							(*it2).prob++;
							////////////////////////cerr << "Master: Know word " << line << " - Quantity " << (*it2).prob << endl;
							break;
						}
					}
				}
      }
      else {
				//////cerr << "Master: Serching previousword " << previousword << " and actualword " << line << endl;
				for (vector<slavecell>::iterator it2 = slavetable.begin(); it2 < slavetable.end(); it2++) {
					if (isinp && isina) {
							break;
					}
					for (vector<wordcell>::iterator it3 = (*it2).words.begin(); it3 < (*it2).words.end(); it3++) {
						if ((*it3).word == previousword) {
							isinp = true;
							rankp = (*it2).rank;
							//////cerr << "Master: Rank p is " << rankp << endl;
						}
						if ((*it3).word == line) {
							isina = true;
							ranka = (*it2).rank;
							////////cerr << "Master: Rank a is " << ranka << endl;
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
							////////cerr << "Master: New word to master " << (*tmpwordcell).word << " with rank " << (*it2).rank << endl;
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
						////////cerr << "Master: New word to master " << (*tmpwordcell).word << " with rank " << (*tmpslavecell).rank << endl;
					}					
					nextrank = (nextrank+1) % NUMTASKS;
					nextrank = (nextrank == 0 ? 1 : nextrank); 
				}
				sprintf(numstr, "%d", ranka);
				////////cerr << "Master: Sending " << (previousword + SEPARATORM + line + SEPARATORM + numstr + SEPARATORM) << " to " << rankp << endl;
				sendMessage(previousword + SEPARATORM + line + SEPARATORM + numstr + SEPARATORM, rankp);
			}
      previousword = line;	  
    }
    fin.close();
    //////////////////////cerr << "Master: Sending end of the book " << (previousword + SEPARATOR + ENDWORD + SEPARATOR + "0" + SEPARATOR) 
		//<< " to " << rankp << endl;
    sendMessage(previousword + SEPARATORM + ENDWORD + SEPARATORM + "0" + SEPARATORM, ranka);
  }
  closedir(dp);

	string coremessage,corestr;
	size_t pos1;
	for(vector<slavecell>::iterator it2 = slavetable.begin(); it2 < slavetable.end(); it2++){
		sendMessage("<get-cores>",(*it2).rank);
		coremessage = receiveMessage();
		//////////////cerr << "Master core message" << coremessage << endl;
		pos1 = coremessage.find("¬");
    corestr = coremessage.substr(0, pos1);
		stringstream convertcore(corestr);
    convertcore >> (*it2).cores;
		//////////////cerr << "Master - rank: " << (*it2).rank <<" - cores:"<< (*it2).cores << endl;
		//////////////cerr <<"\t words: ";
		//for(vector<wordcell>::iterator it3 = (*it2).words.begin(); it3 < (*it2).words.end(); it3++){
		//	////////////cerr << (*it3).word<<", ";
		//}
		//////////////cerr <<"END slavetable"<< endl;
		
	}
	
  //////////////////////////cerr << "Master: Matrix ended.\n";
};

void calculateAndSync() {
  //////////////////////////cerr << "Master: Calculating and sync for " << NUMTASKS << " tasks" << endl;
  for (int i=1; i < NUMTASKS; i++)  {
    //////////////////////////cerr << "Master: Sending end construct to " << i << endl;
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
  ////////////////////////cerr << "Master: Calculated probabilities and sync after barrier" << endl;
};

void runBooks() {
  float randinit;
  char numstr[21]; //Note: Enough to hold all numbers up to 64-bits  
  ////////////////////////cerr << "Master: Start to create books " << NUMBOOKS << endl;
  for(int i = 0; i < NUMBOOKS; i++) {
    randinit = (((double) rand()) / ((double)RAND_MAX));
    ////////////////////////cerr << "Master: Creating book number " << i << endl;
    for(vector<routecell>::iterator it1 = routetable.begin(); it1 < routetable.end(); it1++) {
      if ((randinit = randinit - (*it1).prob) < 0) {
				sprintf(numstr, "%d", i);
				////////////cerr << "Master: Sending " << (*it1).word << " to " << (*it1).rank << endl;
				sendMessage((*it1).word + SEPARATORM + numstr + SEPARATORM + "0" + SEPARATORM, (*it1).rank);
				//WARRNING: sendMessage((*it1).word + SEPARATOR + numstr + SEPARATOR + "0" + SEPARATOR + "0" + SEPARATOR, 0);
				break;
      };      
    }
  }
	////////////cerr << "Master: End run books " << endl;
};

void proccessBooks() {
  //Init books
  int inum = 0;
  int numcpu = sysconf(_SC_NPROCESSORS_ONLN);
  ////////////cerr << "Master: The numbers of cores is " << numcpu << endl;
  bool eend = false;
  vector<pthread_t*> poolthreds(0);
  ////////////cerr << "Master: Writing " << (*books).size() << " books" << endl;
  for (vector<book>::iterator it1 = (*books).begin(); it1 < (*books).end(); it1++) {
    ////////////cerr << "Master: Init book " << inum << endl;
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
  //////////cerr << "Master: Starting the process of thread creation" << endl;
  while (true) {
		////cerr << "Master: Checking to change" << endl;
		sem_wait(&(changemutex));
		if (tochange) {
			stringstream stream;
			stream << slavelow->rank;
			sendMessage("<column-change>" + wordbusy->word + SEPARATORM + stream.str() + SEPARATORM, slavebusy->rank);			
			tochange = false;
			tomigrate = true;
		}
		sem_post(&(changemutex));
		////cerr << "Master: After check to change" << endl;
		sem_wait(&(messagebuffer.fullmutex));
		////cerr << "Master: After check if the master is full" << endl;
    sem_wait(&(messagebuffer.buffermutex));
    ////cerr << "Master: Check if must resive or what" << endl;
    if (messagebuffer.twork == 0 && messagebuffer.check) {
      cerr << "Master: Waiting for messages." << endl;
      sem_post(&(messagebuffer.buffermutex));
      message = new string(receiveMessage());
			cerr << "Master: Received message blocker " << *message << endl;
    }
    else if (messagebuffer.twork == 0) {
      ////cerr << "Master: Check if has end." << endl;
      messagebuffer.check = true;
      eend = true;
      for (vector<book>::iterator it1 = (*books).begin(); it1 < (*books).end(); it1++) {
				//////////////cerr << "Master: Compare size and sec " << (*it1).words.size() << " - " << (*it1).size << endl;
				(*it1).end = (*it1).words.size() == (*it1).size;
				eend = (*it1).end && eend;
      }
      sem_post(&(messagebuffer.buffermutex));
      if (eend) {
				////cerr << "Master: Breake loop!" << endl;
				break;
			}
      else {
				////cerr << "Master: Not end, retry" << endl;
				sem_post(&(messagebuffer.fullmutex));
				continue;
			}
    }
    else {
      ////cerr << "Master: Check if are incoming message." << endl;
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
					cerr << "Master: Waiting for end work" << endl;
					sem_wait(&(waitingworkmutexm));
					cerr << "Master: Un locker for wait end work" << endl;
				}
				sem_post(&(messagebuffer.fullmutex));
				continue;
      }
			////cerr << "Master: Master Recive hurry." << *message << endl;
    }
		cerr << "Master: Recived message " << *message << endl;
		if (tomigrate && (*message).find_last_of(SEPARATORM) >= 13 && (*message).substr(0, 14).compare("<column-ready>") == 0) {
			sem_post(&(messagebuffer.fullmutex));
			//cerr << "Master : Column ready " << slavebusy->rank << endl;
			sendMessage("<column-ready>", slavebusy->rank);
		}
		else if (tomigrate && (*message).find_last_of(SEPARATORM) >= 16 && (*message).substr(0, 17).compare("<column-disabled>") == 0) {
			sem_post(&(messagebuffer.fullmutex));
			//cerr << "Master: Column disable" << endl;
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
			//cerr << "Master: End broadcast, continue with my life" << endl;
		}
		else if ((*message).find_last_of(SEPARATORM) >= 11 && (*message).substr(0, 12).compare("<word-wrong>") == 0) {
			sem_post(&(messagebuffer.fullmutex));
			//cerr << "Master : Word wrong " << *message << endl;
			int pos1 = (*message).find(SEPARATORM);
			string wrongword = (*message).substr(11, pos1-10);
			wrongword = wrongword.substr(1, wrongword.length());
			wrongword = wrongword.substr(0, wrongword.length()-1);
			int endSep = (*message).find(SEPARATORM, (*message).find(SEPARATORM, pos1+1)+1);
			//cerr << "Master: The wrong word is " << wrongword << " and the endSep is " << endSep << endl;
			bool isin = false;
			for (vector<slavecell>::iterator it1 = slavetable.begin(); it1 < slavetable.end(); it1++) {
				for (vector<wordcell>::iterator it2 = (*it1).words.begin(); it2 < (*it1).words.end(); it2++) {
					if ((*it2).word == wrongword) {
						string mess = (*message).substr(12, endSep-10);
						//cerr << "Master: Sending for the new boss of the word before trash " << mess << " with rank " << (*it1).rank << endl;
						//mess = mess.substr(1, mess.length());
						//mess = mess.substr(0, mess.length()-1);
						//cerr << "Master: Sending for the new boss of the word " << mess << " with rank " << (*it1).rank << endl;
						sendMessage(mess, (*it1).rank);
						isin = true;
						break;
					}
				}
				if (isin)
					break;
			}
		}
		else {
			////cerr << "Master: Recive common message " << *message << endl;
			sem_wait(&(messagebuffer.buffermutex));
			messagebuffer.queuemessage.push_back(*message);
			messagebuffer.twork++;
			sem_post(&(messagebuffer.emptymutex));
			sem_post(&(messagebuffer.buffermutex));
		}
	}
  //////////cerr << "Master: Send finish to slaves." << endl;
  string resume = RESUMESLAVE;
  for (int i1=1; i1 < NUMTASKS; i1++) {
    sendMessage(resume + SEPARATORM, i1);
  }
  for (vector<book>::iterator it1 = (*books).begin(); it1 < (*books).end(); it1++) {
    cerr << "Master: Book number " << (*it1).number << endl;
    //for (int i1 = 0; i1 < (*it1).words.size(); i1++) {
      ////////cerr << (*it1).words[i1] << " ";
    //}
    ////cerr << endl;
    ////cerr << endl;
    ////cerr << endl;
    ////cerr << endl;
  }
};

void master(int ntasks, const char* pathbooks, int nbooks) {
  DIR *dp;
  NUMTASKS = ntasks;
  //////////////cerr << "Master: Number of tasks created " << ntasks << ", and " << nbooks << " books." << endl;
  NUMBOOKS = nbooks;
  books = new vector<book>(NUMBOOKS);
  dp = opendir(pathbooks);
  if (dp == NULL) {
    ////////////////////////cerr << "Error opening " << pathbooks << endl;
    return;
  }
  string spath(pathbooks);
  srand(time(NULL));
  createMatrix(dp, spath);
	cerr << "Master: Calculating and synchronized." << endl;
  calculateAndSync();
  cerr << "Master: Start to run books." << endl;
  //  for (vector<routecell>::iterator it2 = routetable.begin(); it2 < routetable.end(); it2++) {
  //    ////////////////////////cerr << "\tprob - " << (*it2).prob << "\tword - " << (*it2).word << "\trank - " << (*it2).rank << endl;
  //  }
  runBooks();
	cerr << "Master: Processing books..." << endl;
	proccessBooks();
	cerr << "Master: Finish." << endl;
	//for (vector<slavecell>::iterator it1 = slavetable.begin(); it1 < slavetable.end(); it1++) {
		////////////cerr << "Master: Count Slave " << (*it1).count << " and rank " << (*it1).rank << endl;
		//for (vector<wordcell>::iterator it2 = (*it1).words.begin(); it2 < (*it1).words.end(); it2++) {
			////////////cerr << "Master: Word " << (*it2).word << endl;
			////////////cerr << "Master: Count " << (*it2).count << endl;
		//}
  //}
	MPI_Finalize();
  //err << "Master: End." << endl;
  return;
};
