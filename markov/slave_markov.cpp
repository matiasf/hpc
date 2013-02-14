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
  pthread_mutex_t emptymutex;
  pthread_mutex_t fullmutex;
  pthread_mutex_t buffermutex;
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
  pthread_mutex_t emptymutex;
  pthread_mutex_t fullmutex;
  pthread_mutex_t buffermutex;
};

//Constants.
string SEPARATORS = SEPARATOR_CHAR;

//Global variables.
vector<column> columns;
int rank;
int twork;
inbuffer bufferin;
outbuffer bufferout;
pthread_mutex_t tworkmutex;
pthread_mutex_t waitingworkmutex;
bool waitingWork = false;

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

  int numcpu = sysconf( _SC_NPROCESSORS_ONLN );

	//cerr << "Slave - " << rank << ": Constructing columns of the Matrix." << endl;
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
	////cerr << "Slave - " << rank << ": After calculate and sync" << endl;
   
   vector<pthread_t*> poolthreds(0);
   bufferout.top = numcpu;
   bufferin.top = numcpu;
   twork = 0;
   pthread_mutex_init(&(bufferout.buffermutex), NULL);
   pthread_mutex_init(&(bufferin.buffermutex), NULL);
   pthread_mutex_init(&(bufferin.emptymutex), NULL);
   pthread_mutex_init(&(bufferout.fullmutex), NULL);
   pthread_mutex_init(&(bufferin.fullmutex), NULL);
	 pthread_mutex_init(&(tworkmutex), NULL);
   pthread_mutex_init(&(waitingworkmutex), NULL);
	 pthread_mutex_lock(&(waitingworkmutex));
   pthread_mutex_lock(&(bufferin.emptymutex));
	 pthread_mutex_lock(&(bufferout.fullmutex));
   pthread_mutex_lock(&(bufferin.fullmutex));
   pthread_t* tid;
   for (int i = 0; i < numcpu; i++) {
     tid = new pthread_t(); 
     pthread_create(tid, NULL, &slaveThread, NULL);
     poolthreds.push_back(tid);
   }
	//cerr << "Slave - " << rank << ": Start to process messages." << endl;
	bool wrong;
	while(true) {
		pthread_mutex_lock(&(bufferout.buffermutex));
		if(bufferout.queuemessage.size() != 0) {
			////cerr << "Slave " << rank << ": sending message" << endl;
			if(bufferout.queuemessage.size() == bufferout.top){
				pthread_mutex_unlock(&(bufferout.fullmutex));
			}			
			slaveMessage = bufferout.queuemessage.front().slaveMessage;
			masterMessage = bufferout.queuemessage.front().masterMessage;
			slaveRank = bufferout.queuemessage.front().slaveRank;
			wrong = bufferout.queuemessage.front().wrong;
			bufferout.queuemessage.erase(bufferout.queuemessage.begin());
			pthread_mutex_unlock(&(bufferout.buffermutex));
			if (wrong) {
				cerr << "Slave " << rank << ": Sending retry " << slaveMessage << endl;
				sendMessage(slaveMessage, 0);
			}
			else {
				cerr << "Slave " << rank << ": Sending " << slaveMessage << endl;
				stringstream ss; 
				ss << rank;
				masterMessage += ss.str() + "¬";
				sendMessage(masterMessage, 0);
				sendMessage(slaveMessage, slaveRank);// 0 rank of slave
			}
			continue;	
		}
		pthread_mutex_unlock(&(bufferout.buffermutex));

		pthread_mutex_lock(&(tworkmutex));
		pthread_mutex_lock(&(bufferin.buffermutex));
		if(bufferin.queuemessage.size() == 0 && twork == 0){
			//cerr << "Slave - " << rank << ": Waiting for messages." << endl;
			pthread_mutex_unlock(&(bufferin.buffermutex));
			pthread_mutex_unlock(&(tworkmutex));
			message = receiveMessage();
		}
    else if (bufferin.queuemessage.size() == bufferin.top) {
			//cerr << "Slave - " << rank << ": Buffer is full, waiting for threads to end." << endl;
			pthread_mutex_unlock(&(bufferin.buffermutex));
			pthread_mutex_unlock(&(tworkmutex));
    	pthread_mutex_lock(&(bufferin.fullmutex));
			continue;
    }
		else{
			//cerr << "Slave - " << rank << ": Checking for message while buffer is not full" << endl;
			pthread_mutex_unlock(&(bufferin.buffermutex));
			pthread_mutex_unlock(&(tworkmutex));
			message = receiveMessageHurry(&toread);
		  if(!toread) {
				pthread_mutex_lock(&(tworkmutex));//FIXME Esto es para poder colocar la bandera que dice que estoy esperando para que terimnene de tabajar
				if (twork == 0) {
					pthread_mutex_unlock(&(tworkmutex));
					continue;
				}
				else {
					pthread_mutex_unlock(&(tworkmutex));
					pthread_mutex_lock(&(waitingworkmutex));
				}
			}
		}      
	  //cerr << "Slave - " << rank << ": Recived message " << message << endl;
	  if (message.find_last_of(SEPARATORS) >= 13 && message.substr(0, 14).compare("<resume-slave>") == 0) {
	    cerr << "Slave - " << rank << ": End slave." << endl;
	    break;
	  }
		else if (message.find_last_of(SEPARATORS) >= 14 && message.substr(0, 15).compare("<column-change>") == 0) {
			cerr << "Slave - " << rank << ": Column to change advice." << endl;
			pos1 = message.find(SEPARATORS);
			busyWord = message.substr(16, pos1-15);
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
				}
				break;
			}
			sendMessage("<column-end>", intLowRank);
		}
		else if (message.find_last_of(SEPARATORS) >= 12 && message.substr(0, 13).compare("<column-send>") == 0) {
			cerr << "Slave - " << rank << ": Starting to recive a column to migrate." << endl;
			pos1 = message.find(SEPARATORS);
			busyWord = message.substr(14, pos1-13);
			busyWord = busyWord.substr(1, busyWord.length());
			busyWord = busyWord.substr(0, busyWord.length()-1);
			column newColumn;
			newColumn.word = busyWord;
			columns.push_back(newColumn);
		}
		else if (message.find_last_of(SEPARATORS) >= 12 && message.substr(0, 13).compare("<column-word>") == 0) {
			cerr << "Slave - " << rank << ": Word of the column." << endl;
			string probString;
			double probDouble;
			string rankString;
			int rankInt;
			string wordString;
			for (vector<column>::iterator it1 = columns.begin(); it1 < columns.end(); it1++) {
				if ((*it1).word == busyWord) {
					routecell newCell;
					pos1 = message.find(SEPARATORS);
					wordString = message.substr(14, pos1-13);
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
					newCell.word = wordString;
					newCell.rank = rankInt;
					newCell.prob = probDouble;
					(*it1).nextWords.push_back(&newCell);
					break;
				}
			}
		}
		else if (message.find_last_of(SEPARATORS) >= 11 && message.substr(0, 12).compare("<column-end>") == 0) {
			cerr << "Slave - " << rank << ": Column ended." << endl;
			sendMessage("<column-ready>", 0);
		}
		else if (message.find_last_of(SEPARATORS) >= 13 && message.substr(0, 14).compare("<column-ready>") == 0) {
			cerr << "Slave - " << rank << ": Column migrated, process ended." << endl;
			pthread_mutex_lock(&(tworkmutex));
			if (twork != 0) {
				waitingWork = true;
				pthread_mutex_unlock(&(tworkmutex));
				pthread_mutex_lock(&(waitingworkmutex));
			}
			else {
				pthread_mutex_unlock(&(tworkmutex));
			}
			//FIXME: The erase column is not freeing the memory of nextWords
			columns.erase(busyColumn);
			sendMessage("<column-disabled>", 0);
		}
		else if (message.find_last_of(SEPARATORS) >= 17 && message.substr(0, 18).compare("<column-broadcast>") == 0) {
			cerr << "Slave - " << rank << ": A column was changed." << endl;
			string bword;
			string bstringrank;
			int brank;
			pos1 = message.find(SEPARATORS, 19);
			bword = message.substr(19, pos1-18);
			bword = bword.substr(1, bword.length());
			bword = bword.substr(0, bword.length()-1);
			//////////////////cerr << "Master: Num book " << booknum << endl;
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
			cerr << "Slave - " << rank << ": Recived normal message " << message << endl;
			pthread_mutex_lock(&(bufferin.buffermutex));
			twork++;
			if(bufferin.queuemessage.size()==0){
				cerr << "Slave - " << rank << ": Waking up a thread to process message" << endl;
				pthread_mutex_unlock(&(bufferin.emptymutex));
			}
			bufferin.queuemessage.push_back(message);
	    pthread_mutex_unlock(&(bufferin.buffermutex));
	  }	
	}

	////////////////////cerr << "Slave" << rank << ": Before barrier." << endl;
	MPI_Finalize();
	////////////////////cerr << "Slave" << rank << ": End " << endl;
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
	bool wrong;
  while(true) {
    pthread_mutex_lock(&(bufferin.buffermutex));
		//////////cerr << "Slave - Thread " << rank << ": locked bufferin.buffermutex" << endl;
    if (bufferin.queuemessage.size() == 0) {
			cerr << "Slave - Thread " << rank << ": Waiting for messages on buffer" << endl;
      pthread_mutex_unlock(&(bufferin.buffermutex));
      pthread_mutex_lock(&(bufferin.emptymutex));
      cerr << "Slave - Thread " << rank << ": Message on buffer, going process" << endl;
			continue;
    }
		if(bufferin.queuemessage.size() == bufferin.top){
			pthread_mutex_unlock(&(bufferin.fullmutex));
		}
		message = bufferin.queuemessage.front();
		bufferin.queuemessage.erase(bufferin.queuemessage.begin());
		
    pthread_mutex_unlock(&(bufferin.buffermutex));
    readBookMessage(message, word, bookNum, seqNum);
	 	//////////cerr << "Slave - Thread" << rank << ": Read book message" << endl;
    cell = searchNextWord(word);
		masterMessage = cell == NULL ? "" : createMessage(word, bookNum, seqNum);
		slaveMessage  = cell == NULL ? "<word-wrong>" + message : createMessage(cell->word, bookNum, seqNum + 1);
		messageSend.slaveMessage = slaveMessage;
		messageSend.masterMessage = masterMessage;
		messageSend.slaveRank = cell == NULL ? 0 : cell->rank;
		messageSend.wrong = cell == NULL;
    ////////////cerr << "Slave" << rank << ": Master message - " << masterMessage << " to " << 0 << endl;
    //////////cerr << "Slave - Thread" << rank << ": Slave message - " << slaveMessage << " to " << cell->rank << endl;
	 
  retry:
	 pthread_mutex_lock(&(bufferout.buffermutex));
	 if (bufferout.queuemessage.size() == bufferout.top) {
			//////////cerr << "Slave - Thread" << rank << ": Full buffer out - trying to insert" << endl;
      pthread_mutex_unlock(&(bufferout.buffermutex));
      pthread_mutex_lock(&(bufferout.fullmutex));
			goto retry; 
    }
	 /*if(bufferout.queuemessage.size()==0){
			//////////cerr << "Slave - Thread" << rank << ": Empty buffer out - release masterthread" << endl;
	 	  pthread_mutex_unlock(&(bufferout.emptymutex));	
			}*/
		
		bufferout.queuemessage.push_back(messageSend);
		pthread_mutex_lock(&(tworkmutex));
	 	twork--;
		if (waitingWork && twork == 0) {
			waitingWork = false;
			pthread_mutex_unlock(&(waitingworkmutex));
		}
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
  ////////////////cerr << "Slave" << rank << ": Rank " << lrank << endl;
  ////////////////cerr << "Slave" << rank << ": Word 1 " << word1 << endl;
  ////////////////cerr << "Slave" << rank << ": Word 2 " << word2 << endl;
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
    ////////////////cerr << "Slave" << rank << ": Adding word " << word1 << " and goes to " << word2 << " with rank " << lrank << endl;
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
    ////////////////cerr << "Slave" << rank << ": Checking word " << (*it1).word << endl;
    if(word1.compare((*it1).word) == 0) {
      ////////////////cerr << "Slave" << rank << ": Finded column " << (*it1).word << endl;
      col = (*it1);
    }
  }
  for(vector<routecell*>::iterator it2 = col.nextWords.begin(); it2 < col.nextWords.end(); it2++) {
    ////////////////cerr << "\tSlave" << rank << ": On column have word " << (*it2)->word << " rank " << (*it2)->rank << " and prob " << (*it2)->prob << endl;
  }
  ////////////////cerr << endl;
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

      ////////////cerr << "Slave: Read book message " << message << endl;
      pos1 = message.find("¬");
      word = message.substr(0, pos1);
      ////////////cerr << "Slave: Read book word " << word << endl;
      pos2 = message.find("¬", pos1+1);
      bookStr = message.substr(pos1+1, pos2-(pos1));
      ////////////cerr << "Slave: Read book bookStr " << bookStr << endl;
      bookStr = bookStr.substr(1, bookStr.length());
      bookStr = bookStr.substr(0, bookStr.length()-1);
      ////////////cerr << "Slave: Read book bookStr " << bookStr << endl;
      pos3 = message.find("¬",pos2+1);
      seqStr = message.substr(pos2+1,pos3-(pos2));
      ////////////cerr << "Slave: Read book seqStr " << seqStr << endl;
      seqStr = seqStr.substr(1, seqStr.length());
      seqStr = seqStr.substr(0, seqStr.length()-1);      
      ////////////cerr << "Slave: Read book seqStr " << seqStr << endl;
      ////////////cerr << "Slave: pos1 - " << pos1 << " pos2 - " << pos2 << endl;
    
      stringstream convertSeq(seqStr);
      convertSeq >> seqNum;
      stringstream convertBook(bookStr);
      convertBook >> bookNum;
      ////////////////////////////cerr << "Slave: word - " << word << " seq - " << seqNum << " book - " << bookNum << endl;
    }

    void readColumnMessage(string message, string &wordRequested, string &wordToGo, int &lrank) {
	    size_t pos1;
	    size_t pos2;
	    size_t pos3;
	    string rankStr;

	    ////////////////////////////cerr << endl;	
	    ////////////////////////////cerr << "Slave: readColumnMessage message : " << message << endl;
	    ////////////////////////////cerr << "Slave: readColumnMessage message length: " << message.length() << endl;
	    pos1 = message.find("¬");
	    wordRequested = message.substr(0, pos1);
	    ////////////////////////////cerr << "Slave: readColumnMessage wr: " << wordRequested << endl;
	    pos2 = message.find("¬", pos1 + 1);
	    wordToGo = message.substr(pos1+1, pos2-(pos1));
	    ////////////////////////////cerr << "Slave: readColumnMessage wtg: " << wordToGo << endl;
	    wordToGo = wordToGo.substr(1, wordToGo.length());
	    wordToGo = wordToGo.substr(0, wordToGo.length()-1);
	    ////////////////////////////cerr << "Slave: readColumnMessage wtg: " << wordToGo << endl;
	    ////////////////////////////cerr << "Slave: pos1 " << pos1 << " pos2 " << pos2 << endl;
	    pos3 = message.find("¬", pos2 + 1);
	    rankStr = message.substr(pos2+1,pos3-(pos2));
	    ////////////////////////////cerr << "Slave: readColumnMessage rs " << rankStr << endl;
	    rankStr = rankStr.substr(1, rankStr.length());
	    rankStr = rankStr.substr(0, rankStr.length()-1);
	    ////////////////////////////cerr << "Slave: readColumnMessage rs " << rankStr << endl;
	    stringstream convertRank(rankStr);
	    convertRank >> lrank;
    }

routecell* randomWord(vector<routecell*> nextWords){
  double randinit = (((double) rand()) / ((double)RAND_MAX));
  //////////////cerr << "Slave" << rank << ": Size vector " << nextWords.size() << endl;
  double howmuch = 0;
  for(vector<routecell*>::iterator it = nextWords.begin(); it < nextWords.end(); it++) {
    howmuch += (*it)->prob;
  }
  //////////////cerr << "Slave" << rank << ": How much " << howmuch << endl;
  for(vector<routecell*>::iterator it = nextWords.begin(); it < nextWords.end(); it++) {
    ////////////////cerr << "Slave" << rank << ": Current rand " << randinit << endl;
    //////////////////cerr << "Slave" << rank << ": Prob " << (*it).prob << endl;
    //////////////////cerr << "Slave" << rank << ": Word " << (*it).word << endl;
    if ((randinit = (randinit - (*it)->prob)) <= 0) {
      //////////////cerr << "Slave" << rank << ": Rank to send " << (*it)->rank << endl;
      return (*it);
    }
  }
  //////////////cerr << "Slave" << rank << ": Se pico tuti!" << endl;
}

routecell* searchNextWord(string word) {
  //////////////cerr << "Slave" << rank << ": Serching..." << endl;
  for (vector<column>::iterator it1 = columns.begin(); it1 < columns.end(); it1++) {
    //////////////cerr << "Slave" << rank << ": Compare " << word << " " << (*it1).word << endl;
    if(word.compare((*it1).word) == 0) {
      return randomWord((*it1).nextWords);
    }
  }
	return NULL;
}

void calculateAndSyncSlave() {
	for(vector<column>::iterator it1 = columns.begin(); it1 < columns.end(); it1++){
		double totalwords = 0;
		////////////////cerr << "Slave" << rank << ": The vector of the word " << (*it1).word << " have size " << (*it1).nextWords.size() << endl;
		for (vector<routecell*>::iterator it2 = (*it1).nextWords.begin(); it2 < (*it1).nextWords.end(); it2++) {
		  ////////////////cerr << "Slave" << rank << ": The word " << (*it2)->word << " goes to rank " << (*it2)->rank << endl; 
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
		////////////////cerr << "Slave" << rank << ": The vector of the word " << (*it1).word << " have size " << (*it1).nextWords.size() << endl;
	}
	////////////////cerr << "Slave: Printing columns" << endl;
	for(vector<column>::iterator it1 = columns.begin(); it1 < columns.end(); it1++){
	  ////////////////cerr << "word - " << (*it1).word << endl;
	  for (vector<routecell*>::iterator it2 = (*it1).nextWords.begin(); it2 < (*it1).nextWords.end(); it2++) {
	    ////////////////cerr << "\tprob - " << (*it2)->prob << " word - " << (*it2)->word << " rank - " << (*it2)->rank << endl;
	  }
	}	
	
	MPI_Barrier(MPI_COMM_WORLD);
	//////////////////////////cerr << "Slave: Calculated probabilities and sync after barrier" << endl;
}
