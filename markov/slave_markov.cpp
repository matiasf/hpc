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
#define STOP_CONSTRUCT "STOP";

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
  pthread_mutex_t emptymutex;
  pthread_mutex_t fullmutex;
  pthread_mutex_t buffermutex;
};

struct threadMessage{
	string slaveMessage;
	string masterMessage;
	int slaveRank;
};

struct outbuffer {
  int top;
  vector<threadMessage> queuemessage;
  pthread_mutex_t emptymutex;
  pthread_mutex_t fullmutex;
  pthread_mutex_t buffermutex;
};




vector<column> columns;
int rank;
int twork;
inbuffer bufferin;
outbuffer bufferout;
pthread_mutex_t tworkmutex;

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
	bool construct = true;	
	routecell* cell;
	rank = grank;
	int toread, slaveRank;
	srand(time(NULL) + rank);
  

	while (construct) {
		message = receiveMessage();
		if (message.substr(0, 16).compare("<stop-construct>") == 0) {
			construct = false;	
		}
		else {
			addWord(message);
		}
	}
	calculateAndSyncSlave();
	
   int numcpu = sysconf( _SC_NPROCESSORS_ONLN );
   vector<pthread_t*> poolthreds(0);
   bufferout.top = numcpu;
   bufferin.top = numcpu;
   twork = 0;
   pthread_mutex_init(&(bufferout.buffermutex), NULL);
   pthread_mutex_init(&(bufferin.buffermutex), NULL);
   pthread_mutex_init(&(bufferout.emptymutex), NULL);
   pthread_mutex_init(&(bufferin.emptymutex), NULL);
   pthread_mutex_init(&(bufferout.fullmutex), NULL);
   pthread_mutex_init(&(bufferin.fullmutex), NULL);
	 pthread_mutex_init(&(tworkmutex), NULL);
   pthread_mutex_lock(&(bufferout.emptymutex));
   pthread_mutex_lock(&(bufferin.emptymutex));
	 pthread_mutex_lock(&(bufferout.fullmutex));
   pthread_mutex_lock(&(bufferin.fullmutex));
   pthread_t* tid;
   for (int i = 0; i < numcpu; i++) {
     tid = new pthread_t(); 
     pthread_create(tid, NULL, &slaveThread, NULL);
     poolthreds.push_back(tid);
   }

	while(true) {
		pthread_mutex_lock(&(bufferout.buffermutex));
		if(bufferout.queuemessage.size() != 0){
			//cerr << "Slave " << rank << ": sending message" << endl;
			if(bufferout.queuemessage.size() == bufferout.top){
				pthread_mutex_unlock(&(bufferout.fullmutex));
			}

			slaveMessage = bufferout.queuemessage.front().slaveMessage;
			//cerr << "Slave " << rank << ": Slave message - " << slaveMessage << " to " << cell->rank << endl;
			//bufferout.queuemessage.front().erase(bufferout.queuemessage.front().begin());
			masterMessage = bufferout.queuemessage.front().masterMessage;
			slaveRank = bufferout.queuemessage.front().slaveRank;
			//cerr << "Slave " << rank << ": Master message - " << masterMessage << endl;
			bufferout.queuemessage.erase(bufferout.queuemessage.begin());
			pthread_mutex_unlock(&(bufferout.buffermutex)); 
			sendMessage(masterMessage, 0);
	    sendMessage(slaveMessage, slaveRank);// 0 rank of slave
			continue;
			
		}
		pthread_mutex_unlock(&(bufferout.buffermutex));

		pthread_mutex_lock(&(bufferin.buffermutex));
		pthread_mutex_lock(&(tworkmutex));

		if(bufferin.queuemessage.size() == 0 && twork == 0){
			//cerr << "Slave " << rank << ": receive blocking" << endl;
			pthread_mutex_unlock(&(tworkmutex));
			pthread_mutex_unlock(&(bufferin.buffermutex));
			message = receiveMessage();
		}
    else if (bufferin.queuemessage.size() == bufferin.top) {
			//cerr << "Slave " << rank << ": full bufferin" << endl;
			pthread_mutex_unlock(&(tworkmutex));
    	pthread_mutex_unlock(&(bufferin.buffermutex));
    	pthread_mutex_lock(&(bufferin.fullmutex));
			continue;
    }
		else{
			////cerr << "Slave " << rank << ": hurrymessage" << endl;
			pthread_mutex_unlock(&(tworkmutex));
			pthread_mutex_unlock(&(bufferin.buffermutex));
			message = receiveMessageHurry(&toread);
		  if(!toread){
				////cerr << "Slave " << rank << ": no hurrymessage" << endl;
				continue;
			}
		}      
	  //cerr << "Slave " << rank << ": Recived message " << message << endl;
	  if (message.compare("BALANCE") == 0) {
	    //TODO work overload
	  }
	  else if (message.substr(0, 14).compare("<resume-slave>") == 0) {
	    //////////////////cerr << "Slave: End slave." << endl;
	    break;
	  }
	  else {
			//cerr << "Slave " << rank << ": inserting word" << endl;
			pthread_mutex_lock(&(bufferin.buffermutex));
			twork++;
			if(bufferin.queuemessage.size()==0){
				//cerr << "Slave " << rank << ": waking up emptymutex" << endl;
				pthread_mutex_unlock(&(bufferin.emptymutex));
			}
			bufferin.queuemessage.push_back(message);
	    pthread_mutex_unlock(&(bufferin.buffermutex));
	  }	
	}

	////////////cerr << "Slave" << rank << ": Before barrier." << endl;
	MPI_Finalize();
	////////////cerr << "Slave" << rank << ": End " << endl;
	return;
}

void *slaveThread(void* param) {
  string message;
  string masterMessage;
  string slaveMessage;
  string word;
  int bookNum;
  int seqNum;
  routecell* cell;
	threadMessage messageSend;

  while(true) {
    pthread_mutex_lock(&(bufferin.buffermutex));
		//cerr << "Slave - Thread " << rank << ": locked bufferin.buffermutex" << endl;
    if (bufferin.queuemessage.size() == 0) {
			//cerr << "Slave - Thread " << rank << ": Waiting for message" << endl;
      pthread_mutex_unlock(&(bufferin.buffermutex));
      pthread_mutex_lock(&(bufferin.emptymutex));
      continue;
    }
		if(bufferin.queuemessage.size() == bufferin.top){
			pthread_mutex_unlock(&(bufferin.fullmutex));
		}
		message = bufferin.queuemessage.front();
		bufferin.queuemessage.erase(bufferin.queuemessage.begin());
		
    pthread_mutex_unlock(&(bufferin.buffermutex));
    readBookMessage(message, word, bookNum, seqNum);
	 	//cerr << "Slave - Thread" << rank << ": Read book message" << endl;
    cell = searchNextWord(word);
    masterMessage = createMessage(word, bookNum, seqNum);
    slaveMessage  = createMessage(cell->word, bookNum, seqNum + 1);
		messageSend.slaveMessage = slaveMessage;
		messageSend.masterMessage = masterMessage;
		messageSend.slaveRank = cell->rank;
    ////cerr << "Slave" << rank << ": Master message - " << masterMessage << " to " << 0 << endl;
    //cerr << "Slave - Thread" << rank << ": Slave message - " << slaveMessage << " to " << cell->rank << endl;
	 
  retry:
	 pthread_mutex_lock(&(bufferout.buffermutex));
	 if (bufferout.queuemessage.size() == bufferout.top) {
			//cerr << "Slave - Thread" << rank << ": Full buffer out - trying to insert" << endl;
      pthread_mutex_unlock(&(bufferout.buffermutex));
      pthread_mutex_lock(&(bufferout.fullmutex));
		goto retry; 
    }
    if(bufferout.queuemessage.size()==0){
			//cerr << "Slave - Thread" << rank << ": Empty buffer out - release masterthread" << endl;
	 	  pthread_mutex_unlock(&(bufferout.emptymutex));	
    }
		
		bufferout.queuemessage.push_back(messageSend);
		pthread_mutex_lock(&(tworkmutex));
	 	twork--;
		pthread_mutex_unlock(&(tworkmutex));
   	pthread_mutex_unlock(&(bufferout.buffermutex));
    
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
  ////////cerr << "Slave" << rank << ": Rank " << lrank << endl;
  ////////cerr << "Slave" << rank << ": Word 1 " << word1 << endl;
  ////////cerr << "Slave" << rank << ": Word 2 " << word2 << endl;
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
    ////////cerr << "Slave" << rank << ": Adding word " << word1 << " and goes to " << word2 << " with rank " << lrank << endl;
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
    ////////cerr << "Slave" << rank << ": Checking word " << (*it1).word << endl;
    if(word1.compare((*it1).word) == 0) {
      ////////cerr << "Slave" << rank << ": Finded column " << (*it1).word << endl;
      col = (*it1);
    }
  }
  for(vector<routecell*>::iterator it2 = col.nextWords.begin(); it2 < col.nextWords.end(); it2++) {
    ////////cerr << "\tSlave" << rank << ": On column have word " << (*it2)->word << " rank " << (*it2)->rank << " and prob " << (*it2)->prob << endl;
  }
  ////////cerr << endl;
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

      ////cerr << "Slave: Read book message " << message << endl;
      pos1 = message.find("¬");
      word = message.substr(0, pos1);
      ////cerr << "Slave: Read book word " << word << endl;
      pos2 = message.find("¬", pos1+1);
      bookStr = message.substr(pos1+1, pos2-(pos1));
      ////cerr << "Slave: Read book bookStr " << bookStr << endl;
      bookStr = bookStr.substr(1, bookStr.length());
      bookStr = bookStr.substr(0, bookStr.length()-1);
      ////cerr << "Slave: Read book bookStr " << bookStr << endl;
      pos3 = message.find("¬",pos2+1);
      seqStr = message.substr(pos2+1,pos3-(pos2));
      ////cerr << "Slave: Read book seqStr " << seqStr << endl;
      seqStr = seqStr.substr(1, seqStr.length());
      seqStr = seqStr.substr(0, seqStr.length()-1);      
      ////cerr << "Slave: Read book seqStr " << seqStr << endl;
      ////cerr << "Slave: pos1 - " << pos1 << " pos2 - " << pos2 << endl;
    
      stringstream convertSeq(seqStr);
      convertSeq >> seqNum;
      stringstream convertBook(bookStr);
      convertBook >> bookNum;
      ////////////////////cerr << "Slave: word - " << word << " seq - " << seqNum << " book - " << bookNum << endl;
    }

    void readColumnMessage(string message, string &wordRequested, string &wordToGo, int &lrank) {
	    size_t pos1;
	    size_t pos2;
	    size_t pos3;
	    string rankStr;

	    ////////////////////cerr << endl;	
	    ////////////////////cerr << "Slave: readColumnMessage message : " << message << endl;
	    ////////////////////cerr << "Slave: readColumnMessage message length: " << message.length() << endl;
	    pos1 = message.find("¬");
	    wordRequested = message.substr(0, pos1);
	    ////////////////////cerr << "Slave: readColumnMessage wr: " << wordRequested << endl;
	    pos2 = message.find("¬", pos1 + 1);
	    wordToGo = message.substr(pos1+1, pos2-(pos1));
	    ////////////////////cerr << "Slave: readColumnMessage wtg: " << wordToGo << endl;
	    wordToGo = wordToGo.substr(1, wordToGo.length());
	    wordToGo = wordToGo.substr(0, wordToGo.length()-1);
	    ////////////////////cerr << "Slave: readColumnMessage wtg: " << wordToGo << endl;
	    ////////////////////cerr << "Slave: pos1 " << pos1 << " pos2 " << pos2 << endl;
	    pos3 = message.find("¬", pos2 + 1);
	    rankStr = message.substr(pos2+1,pos3-(pos2));
	    ////////////////////cerr << "Slave: readColumnMessage rs " << rankStr << endl;
	    rankStr = rankStr.substr(1, rankStr.length());
	    rankStr = rankStr.substr(0, rankStr.length()-1);
	    ////////////////////cerr << "Slave: readColumnMessage rs " << rankStr << endl;
	    stringstream convertRank(rankStr);
	    convertRank >> lrank;
    }

routecell* randomWord(vector<routecell*> nextWords){
  double randinit = (((double) rand()) / ((double)RAND_MAX));
  //////cerr << "Slave" << rank << ": Size vector " << nextWords.size() << endl;
  double howmuch = 0;
  for(vector<routecell*>::iterator it = nextWords.begin(); it < nextWords.end(); it++) {
    howmuch += (*it)->prob;
  }
  //////cerr << "Slave" << rank << ": How much " << howmuch << endl;
  for(vector<routecell*>::iterator it = nextWords.begin(); it < nextWords.end(); it++) {
    ////////cerr << "Slave" << rank << ": Current rand " << randinit << endl;
    //////////cerr << "Slave" << rank << ": Prob " << (*it).prob << endl;
    //////////cerr << "Slave" << rank << ": Word " << (*it).word << endl;
    if ((randinit = (randinit - (*it)->prob)) <= 0) {
      //////cerr << "Slave" << rank << ": Rank to send " << (*it)->rank << endl;
      return (*it);
    }
  }
  //////cerr << "Slave" << rank << ": Se pico tuti!" << endl;
}

routecell* searchNextWord(string word) {
  //////cerr << "Slave" << rank << ": Serching..." << endl;
  for (vector<column>::iterator it1 = columns.begin(); it1 < columns.end(); it1++) {
    //////cerr << "Slave" << rank << ": Compare " << word << " " << (*it1).word << endl;
    if(word.compare((*it1).word) == 0) {
      return randomWord((*it1).nextWords);
    }
  }
}

void calculateAndSyncSlave() {
	for(vector<column>::iterator it1 = columns.begin(); it1 < columns.end(); it1++){
		double totalwords = 0;
		////////cerr << "Slave" << rank << ": The vector of the word " << (*it1).word << " have size " << (*it1).nextWords.size() << endl;
		for (vector<routecell*>::iterator it2 = (*it1).nextWords.begin(); it2 < (*it1).nextWords.end(); it2++) {
		  ////////cerr << "Slave" << rank << ": The word " << (*it2)->word << " goes to rank " << (*it2)->rank << endl; 
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
		////////cerr << "Slave" << rank << ": The vector of the word " << (*it1).word << " have size " << (*it1).nextWords.size() << endl;
	}
	////////cerr << "Slave: Printing columns" << endl;
	for(vector<column>::iterator it1 = columns.begin(); it1 < columns.end(); it1++){
	  ////////cerr << "word - " << (*it1).word << endl;
	  for (vector<routecell*>::iterator it2 = (*it1).nextWords.begin(); it2 < (*it1).nextWords.end(); it2++) {
	    ////////cerr << "\tprob - " << (*it2)->prob << " word - " << (*it2)->word << " rank - " << (*it2)->rank << endl;
	  }
	}	
	
	MPI_Barrier(MPI_COMM_WORLD);
	//////////////////cerr << "Slave: Calculated probabilities and sync after barrier" << endl;
}
