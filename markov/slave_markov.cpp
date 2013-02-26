#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
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
#define STOP_CONSTRUCT "STOP";
#define SEPARATOR_CHAR "¬";

using namespace std;

struct routecell {
  string word;
  double prob;
  int rank;
};

struct column {
  string word;
  vector<routecell*> nextWords;
};

struct inbuffer {
  int top;
  vector<string> queuemessage;
  sem_t emptymutex;
  sem_t fullmutex;
  sem_t buffermutex;
};

struct threadMessage{
	string slaveMessage;
	string masterMessage;
	int slaveRank;
	bool wrong;
};

struct outbuffer {
  int top;
  vector<threadMessage> queuemessage;
  sem_t emptymutex;
  sem_t fullmutex;
  sem_t buffermutex;
};

//Constants.
string SEPARATORS = SEPARATOR_CHAR;

//Global variables.
vector<column> columns;
int rank;
int twork = 0;
inbuffer bufferin;
outbuffer bufferout;
sem_t tworkmutex;
sem_t waitingworkmutex;
bool waitingWork = false;
pthread_t self;
int numcpu = sysconf( _SC_NPROCESSORS_ONLN );

void addWord(string word);
string createMessage(string word,int bookNum, int seqNum);
void readBookMessage(string message,string &word, int &bookNum, int &seqNum);
void readColumnMessage(string message,string &wordRequested,string &wordToGo,int &lrank) ;
routecell* searchNextWord(string word);
routecell addWordToColumn(string word, int lrank);
void calculateAndSyncSlave();
void *slaveThread(void* param);

void slave(int grank) { 
	string message;
  string masterMessage;
  string slaveMessage;
	string busyWord, lowRank;
	vector<column>::iterator busyColumn;
	bool construct = true;	
	routecell* cell;
	rank = grank;
	int toread, slaveRank;
	srand(time(NULL) + rank);
	int pos1, pos2, pos3, intLowRank;
	while (construct) {
		message = receiveMessage();
		if (message.substr(0, 16).compare("<stop-construct>") == 0) {
			construct = false;	
		}
		else if(message.substr(0, 11).compare("<get-cores>") == 0){
 			stringstream ss;
  		ss << numcpu;
			masterMessage = ss.str() + "¬";
			sendMessage(masterMessage, 0);
		}
		else {
			addWord(message);
		}
	}
	calculateAndSyncSlave();
   
	vector<pthread_t*> poolthreds(0);
   bufferout.top = numcpu;
   bufferin.top = numcpu;
	 
   sem_init(&(bufferout.buffermutex), 0, 1);
   sem_init(&(bufferin.buffermutex), 0, 1);
   sem_init(&(bufferin.emptymutex), 0, 0);
   sem_init(&(bufferout.fullmutex), 0, numcpu);
   sem_init(&(bufferin.fullmutex), 0, numcpu);
	 sem_init(&(tworkmutex), 0, 1);
   sem_init(&(waitingworkmutex), 0, 0);
   pthread_t* tid;
   for (int i = 0; i < numcpu; i++) {
     tid = new pthread_t(); 
     pthread_create(tid, NULL, &slaveThread, NULL);
     poolthreds.push_back(tid);
   }
	bool wrong;
	while(true) {
		sem_wait(&(bufferout.buffermutex));
		if(bufferout.queuemessage.size() != 0) {
			sem_post(&(bufferout.fullmutex));
			slaveMessage = bufferout.queuemessage.front().slaveMessage;
			masterMessage = bufferout.queuemessage.front().masterMessage;
			slaveRank = bufferout.queuemessage.front().slaveRank;
			wrong = bufferout.queuemessage.front().wrong;
			bufferout.queuemessage.erase(bufferout.queuemessage.begin());
			sem_post(&(bufferout.buffermutex));
			if (wrong) {
				sendMessage(slaveMessage, 0);
			}
			else {
				stringstream ss; 
				ss << rank;
				masterMessage += ss.str() + "¬";
				sendMessage(masterMessage, 0);
				sendMessage(slaveMessage, slaveRank);// 0 rank of slave
			}
			continue;	
		}
		sem_post(&(bufferout.buffermutex));
		sem_wait(&(tworkmutex));
		sem_wait(&(bufferin.buffermutex));
		if(bufferin.queuemessage.size() == 0 && twork == 0) {
			sem_post(&(bufferin.buffermutex));
			sem_post(&(tworkmutex));
			if (bufferout.queuemessage.size() != 0) {
				continue;
			}
			message = receiveMessage();
		}
		else {
			if (bufferout.queuemessage.size() != 0) {
				sem_post(&(bufferin.buffermutex));
				sem_post(&(tworkmutex));
				continue;
			}
			sem_post(&(bufferin.buffermutex));
			sem_post(&(tworkmutex));
			message = receiveMessageHurry(&toread);
		  if (!toread) {
				sem_wait(&(tworkmutex));
				sem_wait(&(bufferout.buffermutex));
				if (twork == 0 || bufferout.queuemessage.size()) {
					sem_post(&(bufferout.buffermutex));
					sem_post(&(tworkmutex));
				}
				else {
					waitingWork = true;
					sem_post(&(bufferout.buffermutex));
					sem_post(&(tworkmutex));
					sem_wait(&(waitingworkmutex));
				}
				continue;
			}
		}      
	  if (message.find_last_of(SEPARATORS) >= 13 && message.substr(0, 14).compare("<resume-slave>") == 0) {
	    break;
	  }
		else if (message.find_last_of(SEPARATORS) >= 14 && message.substr(0, 15).compare("<column-change>") == 0) {
			pos1 = message.find(SEPARATORS);
			busyWord = message.substr(14, pos1-13);
			busyWord = busyWord.substr(1, busyWord.length());
			busyWord = busyWord.substr(0, busyWord.length()-1);
			pos2 = message.find(SEPARATORS, pos1+1);
			lowRank = message.substr(pos1+1, pos2-pos1);
			lowRank = lowRank.substr(1, lowRank.length());
			lowRank = lowRank.substr(0, lowRank.length()-1);
			stringstream stream(lowRank);
			stringstream* probStream;
			stringstream* rankStream;
			stream >> intLowRank;
			sendMessage("<column-send>" + busyWord + SEPARATORS , intLowRank);
			for(vector<column>::iterator it8 = columns.begin(); it8 < columns.end(); it8++){
				for (vector<routecell*>::iterator it2 = (*it8).nextWords.begin(); it2 < (*it8).nextWords.end(); it2++) {
				}
			}
			for (vector<column>::iterator it1 = columns.begin(); it1 < columns.end(); it1++) {
				if ((*it1).word == busyWord) {
					busyColumn = it1;
					for(vector<routecell*>::iterator it2 = (*it1).nextWords.begin(); it2 < (*it1).nextWords.end(); it2++) {
						probStream = new stringstream();
						(*probStream) << (*it2)->prob;
						rankStream = new stringstream();
						(*rankStream) << (*it2)->rank;
						sendMessage("<column-word>" + (*it2)->word + SEPARATORS + (*probStream).str() + SEPARATORS + (*rankStream).str() + SEPARATORS, intLowRank);
						delete probStream;
						delete rankStream;
					}
					break;
				}
			}
			sendMessage("<column-end>", intLowRank);
		}
		else if (message.find_last_of(SEPARATORS) >= 12 && message.substr(0, 13).compare("<column-send>") == 0) {
			pos1 = message.find(SEPARATORS);
			busyWord = message.substr(12, pos1-11);
			busyWord = busyWord.substr(1, busyWord.length());
			busyWord = busyWord.substr(0, busyWord.length()-1);
			column newColumn;
			newColumn.word = busyWord;
			columns.push_back(newColumn);
		}
		else if (message.find_last_of(SEPARATORS) >= 12 && message.substr(0, 13).compare("<column-word>") == 0) {
			string probString;
			double probDouble;
			string rankString;
			int rankInt;
			string wordString;
			for (vector<column>::iterator it1 = columns.begin(); it1 < columns.end(); it1++) {
				if ((*it1).word == busyWord) {
					routecell*  newCell = new routecell();
					pos1 = message.find(SEPARATORS);
					wordString = message.substr(12, pos1-11);
					wordString = wordString.substr(1, wordString.length());
					wordString = wordString.substr(0, wordString.length()-1);
					pos2 = message.find(SEPARATORS, pos1+1);
					probString = message.substr(pos1+1, pos2-pos1);
					probString = probString.substr(1, probString.length());
					probString = probString.substr(0, probString.length()-1); 
					stringstream probStream(probString);
					probStream >> probDouble;
					pos3 = message.find(SEPARATORS, pos2+1);
					rankString = message.substr(pos2+1, pos3-pos2);
					rankString = rankString.substr(1, rankString.length());
					rankString = rankString.substr(0, rankString.length()-1);
					stringstream rankStream(rankString);
					rankStream >> rankInt;
					newCell->word = wordString;
					newCell->rank = rankInt;
					newCell->prob = probDouble;
					(*it1).nextWords.push_back(newCell);
					break;
				}
			}
		}
		else if (message.find_last_of(SEPARATORS) >= 11 && message.substr(0, 12).compare("<column-end>") == 0) {
			for(vector<column>::iterator it1 = columns.begin(); it1 < columns.end(); it1++){
				for (vector<routecell*>::iterator it2 = (*it1).nextWords.begin(); it2 < (*it1).nextWords.end(); it2++) {
				}
			}
			sendMessage("<column-ready>", 0);
		}
		else if (message.find_last_of(SEPARATORS) >= 13 && message.substr(0, 14).compare("<column-ready>") == 0) {
			sem_wait(&(tworkmutex));
			if (twork == 0) {
				sem_post(&(tworkmutex));
			}
			else {
				waitingWork = true;
				sem_post(&(tworkmutex));
				sem_wait(&(waitingworkmutex));
			}
			columns.erase(busyColumn);
			for(vector<column>::iterator it1 = columns.begin(); it1 < columns.end(); it1++){
				for (vector<routecell*>::iterator it2 = (*it1).nextWords.begin(); it2 < (*it1).nextWords.end(); it2++) {
				}
			}
			sendMessage("<column-disabled>", 0);
		}
		else if (message.find_last_of(SEPARATORS) >= 17 && message.substr(0, 18).compare("<column-broadcast>") == 0) {
			string bword;
			string bstringrank;
			int brank;
			pos1 = message.find(SEPARATORS);
			bword = message.substr(17, pos1-16);
			bword = bword.substr(1, bword.length());
			bword = bword.substr(0, bword.length()-1);
			pos2 = message.find(SEPARATORS, pos1+1);
			bstringrank = message.substr(pos1+1, pos2-pos1);
			bstringrank = bstringrank.substr(1, bstringrank.length());
			bstringrank = bstringrank.substr(0, bstringrank.length()-1);
			stringstream stream(bstringrank);
			stream >> brank;
			for (vector<column>::iterator it1 = columns.begin(); it1 < columns.end(); it1++) {
				for (vector<routecell*>::iterator it2 = (*it1).nextWords.begin(); it2 < (*it1).nextWords.end(); it2++) {
					if ((*it2)->word == bword) {
						(*it2)->rank = brank;
						break;
					}
				}
			}
		}
		else {
			sem_wait(&(bufferin.fullmutex));
			sem_wait(&(tworkmutex));
			sem_wait(&(bufferin.buffermutex));
			twork++;
			bufferin.queuemessage.push_back(message);
			sem_post(&(bufferin.emptymutex));
	    sem_post(&(bufferin.buffermutex));
			sem_post(&(tworkmutex));
	  }	
	}
	MPI_Finalize();
	return;
}

void *slaveThread(void* param) {
	self = pthread_self();
  string message;
  string masterMessage;
  string slaveMessage;
  string word;
  int bookNum;
  int seqNum;
  routecell* cell;
	threadMessage messageSend;
	bool wrong;
  while(true) {
		sem_wait(&(bufferin.emptymutex));
    sem_wait(&(bufferin.buffermutex));
		sem_post(&(bufferin.fullmutex));
		message = bufferin.queuemessage.front();
		bufferin.queuemessage.erase(bufferin.queuemessage.begin());
    sem_post(&(bufferin.buffermutex));
    readBookMessage(message, word, bookNum, seqNum);
    cell = searchNextWord(word);
		
		masterMessage = cell == NULL ? "" : createMessage(word, bookNum, seqNum);
		slaveMessage  = cell == NULL ? "<word-wrong>" + message : createMessage(cell->word, bookNum, seqNum + 1);
		messageSend.slaveMessage = slaveMessage;
		messageSend.masterMessage = masterMessage;
		messageSend.slaveRank = cell == NULL ? 0 : cell->rank;
		messageSend.wrong = cell == NULL;

		sem_wait(&(bufferout.fullmutex));
		sem_wait(&(tworkmutex));
		sem_wait(&(bufferout.buffermutex));		
		bufferout.queuemessage.push_back(messageSend);
	 	twork--;
		if (waitingWork && (twork % numcpu == 0)) {
			waitingWork = false;
			sem_post(&(waitingworkmutex));
		}
   	sem_post(&(bufferout.buffermutex));
		sem_post(&(tworkmutex));
  }
}

void addWord(string word) {
  bool notInWord1 = true;
  bool notInWord2 = true;
  string word1;
  string word2;
  int lrank;
  routecell* rc;
  
  readColumnMessage(word, word1, word2, lrank);
  for (vector<column>::iterator it1 = columns.begin(); it1 < columns.end(); it1++) {
    if(word1.compare((*it1).word)==0) {
      notInWord1 = false;
      for(vector<routecell*>::iterator it2 = (*it1).nextWords.begin(); it2 < (*it1).nextWords.end(); it2++) {
	if(word2.compare((*it2)->word) == 0) {
	  (*it2)->prob += 1;
	  notInWord2 = false;
	  break;
	}
      }
      if(notInWord2) {
				rc = new routecell();
				(*rc).word = word2;
				(*rc).rank = lrank;
				(*rc).prob = 1;
				(*it1).nextWords.push_back(rc);
      }
      break;
    }
  }
  if(notInWord1) {
    column* c = new column();
    (*c).word = word1;
    rc = new routecell();
    (*rc).word = word2;
    (*rc).rank = lrank;
    (*rc).prob = 1;
    (*c).nextWords.push_back(rc);
    columns.push_back(*c);
  }
  
  column col;
  for (vector<column>::iterator it1 = columns.begin(); it1 < columns.end(); it1++) {
    if(word1.compare((*it1).word) == 0) {
      col = (*it1);
    }
  }
}

string createMessage(string word, int bookNum, int seqNum) {
  stringstream ss1;
  ss1 << bookNum;
  string bookStr = ss1.str();
  stringstream ss2;
  ss2<< seqNum;
  string seqStr = ss2.str();
  string returnStr = word;
  returnStr += "¬";
  returnStr += bookStr;
  returnStr += "¬";
  returnStr += seqStr;
  returnStr += "¬";  
  return returnStr;
}

    void readBookMessage(string message,string &word, int &bookNum, int &seqNum) {
      size_t pos1;
      size_t pos2;
      size_t pos3;
      string seqStr;
      string bookStr;

      pos1 = message.find("¬");
      word = message.substr(0, pos1);
      pos2 = message.find("¬", pos1+1);
      bookStr = message.substr(pos1+1, pos2-(pos1));
      bookStr = bookStr.substr(1, bookStr.length());
      bookStr = bookStr.substr(0, bookStr.length()-1);
      pos3 = message.find("¬",pos2+1);
      seqStr = message.substr(pos2+1,pos3-(pos2));
      seqStr = seqStr.substr(1, seqStr.length());
      seqStr = seqStr.substr(0, seqStr.length()-1);      
    
      stringstream convertSeq(seqStr);
      convertSeq >> seqNum;
      stringstream convertBook(bookStr);
      convertBook >> bookNum;
    }

    void readColumnMessage(string message, string &wordRequested, string &wordToGo, int &lrank) {
	    size_t pos1;
	    size_t pos2;
	    size_t pos3;
	    string rankStr;

	    pos1 = message.find("¬");
	    wordRequested = message.substr(0, pos1);
	    pos2 = message.find("¬", pos1 + 1);
	    wordToGo = message.substr(pos1+1, pos2-(pos1));
	    wordToGo = wordToGo.substr(1, wordToGo.length());
	    wordToGo = wordToGo.substr(0, wordToGo.length()-1);
	    pos3 = message.find("¬", pos2 + 1);
	    rankStr = message.substr(pos2+1,pos3-(pos2));
	    rankStr = rankStr.substr(1, rankStr.length());
	    rankStr = rankStr.substr(0, rankStr.length()-1);
	    stringstream convertRank(rankStr);
	    convertRank >> lrank;
    }

routecell* randomWord(vector<routecell*> nextWords){
  double randinit = (((double) rand()) / ((double)RAND_MAX));
  double howmuch = 0;
  for(vector<routecell*>::iterator it = nextWords.begin(); it < nextWords.end(); it++) {
    howmuch += (*it)->prob;
  }
  for(vector<routecell*>::iterator it = nextWords.begin(); it < nextWords.end(); it++) {
    if ((randinit = (randinit - (*it)->prob)) <= 0) {
      return (*it);
    }
  }
}

routecell* searchNextWord(string word) {
  for (vector<column>::iterator it1 = columns.begin(); it1 < columns.end(); it1++) {
    if(word.compare((*it1).word) == 0) {
      return randomWord((*it1).nextWords);
    }
  }
	return NULL;
}

void calculateAndSyncSlave() {
	for(vector<column>::iterator it1 = columns.begin(); it1 < columns.end(); it1++){
		double totalwords = 0;
		for (vector<routecell*>::iterator it2 = (*it1).nextWords.begin(); it2 < (*it1).nextWords.end(); it2++) {
		  totalwords += (*it2)->prob;
		}
		for (vector<routecell*>::iterator it2 = (*it1).nextWords.begin(); it2 < (*it1).nextWords.end(); it2++) {
			(*it2)->prob = (*it2)->prob / totalwords;
		}
		vector<routecell*> unsort = (*it1).nextWords;
		vector<routecell*> sorted(0);
		bool isIn = false;
		for (vector<routecell*>::iterator it3 = unsort.begin(); it3 < unsort.end(); it3++) {
		  isIn = false;
		  for (vector<routecell*>::iterator it4 = sorted.begin(); it4 < sorted.end(); it4++) {
		    if ((*it4)->prob > (*it3)->prob) {
		      sorted.insert(it4, *it3);
		      isIn = true;
		      break;
		    }
		  }
		  if (!isIn) {
		    sorted.push_back(*it3);
		  }
		}
		(*it1).nextWords = sorted;
	}
	
	
	MPI_Barrier(MPI_COMM_WORLD);
}
