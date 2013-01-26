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
  vector<routecell> nextWords;
};

vector<column> columns;
int rank;

void addWord(string word);
string createMessage(string word,int bookNum, int seqNum);
void readBookMessage(string message,string &word, int &bookNum, int &seqNum);
void readColumnMessage(string message,string &wordRequested,string &wordToGo,int &lrank) ;
routecell searchNextWord(string word);
routecell addWordToColumn(string word, int lrank);
void calculateAndSyncSlave();

void slave(int grank) {
	string message;
	string masterMessage;
	string slaveMessage;
	string word;
	int bookNum;
	int seqNum;
	bool construct = true;	
	routecell cell;
	rank = grank;
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
	//cerr << "Slave" << rank << ": Start to create books." << endl;
	////////cerr << "Slave" << rank << ": Printing columns of " << rank << endl;
	//for(vector<column>::iterator it1 = columns.begin(); it1 < columns.end(); it1++){
	////////cerr << "word" << rank << " - " << (*it1).word << endl;
	//for (vector<routecell>::iterator it2 = (*it1).nextWords.begin(); it2 < (*it1).nextWords.end(); it2++) {
	////////cerr << "\tprob - " << (*it2).prob << "\tword - " << (*it2).word << "\trank - " << (*it2).rank << endl;
	//}
	//}	

	while(true) {      
	  message = receiveMessage();
	  //////////cerr << "Slave: Comparation !!!" << message.substr(0, 14) << "!!!" << endl;
	  if (message.compare("BALANCE") == 0) {
	    //TODO work overload
	  }
	  else if (message.substr(0, 14).compare("<resume-slave>") == 0) {
	    //////////cerr << "Slave: End slave." << endl;
	    break;
	  }
	  else {
	    //cerr << "Slave" << rank << ": Recive word " << message << endl;
	    readBookMessage(message, word, bookNum, seqNum);
	    //cerr << "Slave" << rank << ": Read book message" << endl;
	    cell = searchNextWord(word);
	    //cerr << "Slave" << rank << ": Search word" << endl;
	    masterMessage = createMessage(word, bookNum, seqNum);
	    //cerr << "Slave" << rank << ": Create Message with " << cell.word << endl;
	    slaveMessage  = createMessage(cell.word, bookNum, seqNum + 1);
	    //cerr << "Slave" << rank << ": Master message - " << masterMessage << " to " << 0 << endl;
	    //cerr << "Slave" << rank << ": Slave message - " << slaveMessage << " to " << cell.rank << endl;
	    sendMessage(masterMessage, 0);
	    sendMessage(slaveMessage, cell.rank);// 0 rank of slav
	  }	
	}
	////cerr << "Slave" << rank << ": Before barrier." << endl;
	MPI_Finalize();
	////cerr << "Slave" << rank << ": End " << endl;
	return;
}

void addWord(string word) {
  bool notInWord1 = true;
  bool notInWord2 = true;
  string word1;
  string word2;
  int lrank;
  routecell rc;
  
  readColumnMessage(word, word1, word2, lrank);
  cerr << "Slave" << rank << ": Rank " << lrank << endl;
  cerr << "Slave" << rank << ": Word 1 " << word1 << endl;
  cerr << "Slave" << rank << ": Word 2 " << word2 << endl;
  for (vector<column>::iterator it1 = columns.begin(); it1 < columns.end(); it1++) {
    if(word1.compare((*it1).word)==0) {
      notInWord1 = false;
      for(vector<routecell>::iterator it2 = (*it1).nextWords.begin(); it2 < (*it1).nextWords.end(); it2++) {
	if(word2.compare((*it2).word)==0) {
	  (*it2).prob += 1;
	  notInWord2 = false;
	  break;
	}
      }		
      if(notInWord2) {
	rc = addWordToColumn(word2, lrank);
	(*it1).nextWords.push_back(rc);
      }
      break;
    }
  }
  if(notInWord1) {
    cerr << "Slave" << rank << ": Adding word " << word1 << " and goes to " << word2 << " with rank " << lrank << endl;
    column* c = new column();
    (*c).word = word1;
    rc = addWordToColumn(word2, lrank);
    (*c).nextWords.push_back(rc);
    columns.push_back(*c);
  }
  
  column col;
  for (vector<column>::iterator it1 = columns.begin(); it1 < columns.end(); it1++) {
    if(word1.compare((*it1).word) == 0) {
      col = (*it1);
    }
  }
  cerr << "Slave" << rank << " atendend word: " << col.word << endl;
  cerr << "Slave" << rank << " goes to :" << endl;
  for(vector<routecell>::iterator it3 = col.nextWords.begin(); it3 < col.nextWords.end(); it3++) {
    cerr<< "\t word: " << (*it3).word << " - rank: " << (*it3).rank << " - prob:" << (*it3).prob << endl; 
  }
  cerr << endl;
}

    string createMessage(string word, int bookNum, int seqNum) {
	    stringstream ss;
	    ss << bookNum;
	    string bookStr = ss.str();
	    ss << seqNum;
	    string seqStr = ss.str();
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
      //      //////////cerr << "Slave: pos1 - " << pos1 << " pos2 - " << pos2 << endl;
    
      stringstream convertSeq(seqStr);
      convertSeq >> seqNum;
      stringstream convertBook(bookStr);
      convertBook >> bookNum;
      ////////////cerr << "Slave: word - " << word << " seq - " << seqNum << " book - " << bookNum << endl;
    }

    void readColumnMessage(string message, string &wordRequested, string &wordToGo, int &lrank) {
	    size_t pos1;
	    size_t pos2;
	    size_t pos3;
	    string rankStr;

	    ////////////cerr << endl;	
	    ////////////cerr << "Slave: readColumnMessage message : " << message << endl;
	    ////////////cerr << "Slave: readColumnMessage message length: " << message.length() << endl;
	    pos1 = message.find("¬");
	    wordRequested = message.substr(0, pos1);
	    ////////////cerr << "Slave: readColumnMessage wr: " << wordRequested << endl;
	    pos2 = message.find("¬", pos1 + 1);
	    wordToGo = message.substr(pos1+1, pos2-(pos1));
	    ////////////cerr << "Slave: readColumnMessage wtg: " << wordToGo << endl;
	    wordToGo = wordToGo.substr(1, wordToGo.length());
	    wordToGo = wordToGo.substr(0, wordToGo.length()-1);
	    ////////////cerr << "Slave: readColumnMessage wtg: " << wordToGo << endl;
	    ////////////cerr << "Slave: pos1 " << pos1 << " pos2 " << pos2 << endl;
	    pos3 = message.find("¬", pos2 + 1);
	    rankStr = message.substr(pos2+1,pos3-(pos2));
	    ////////////cerr << "Slave: readColumnMessage rs " << rankStr << endl;
	    rankStr = rankStr.substr(1, rankStr.length());
	    rankStr = rankStr.substr(0, rankStr.length()-1);
	    ////////////cerr << "Slave: readColumnMessage rs " << rankStr << endl;
	    stringstream convertRank(rankStr);
	    convertRank >> lrank;
    }

routecell randomWord(vector<routecell> nextWords){
  double randinit = (((double) rand()) / ((double)RAND_MAX));
  //cerr << "Slave" << rank << ": Size vector " << nextWords.size() << endl;
  double howmuch = 0;
  for(vector<routecell>::iterator it = nextWords.begin(); it < nextWords.end(); it++) {
    howmuch += (*it).prob;
  }
  //cerr << "Slave" << rank << ": How much " << howmuch << endl;
  for(vector<routecell>::iterator it = nextWords.begin(); it < nextWords.end(); it++) {
    //cerr << "Slave" << rank << ": Current rand " << randinit << endl;
    //cerr << "Slave" << rank << ": Prob " << (*it).prob << endl;
    //cerr << "Slave" << rank << ": Word " << (*it).word << endl;
    if ((randinit = (randinit - (*it).prob)) <= 0) {
      return (*it);
    }
  }
  //cerr << "Slave" << rank << ": Se pico tuti!" << endl;
}

routecell searchNextWord(string word) {
  ////cerr << "Slave" << rank << ": Serching..." << endl;
  for (vector<column>::iterator it1 = columns.begin(); it1 < columns.end(); it1++) {
    ////cerr << "Slave" << rank << ": Compare " << word << " " << (*it1).word << endl;
    if(word.compare((*it1).word) == 0) {
      ////cerr << "Slave" << rank << ": The word have a rank of " << (*it1).rank << endl;
      return randomWord((*it1).nextWords);
    }
  }
}

routecell addWordToColumn(string word, int lrank) {
  cerr << "Slave" << rank << ": Rank to put " << lrank << endl;
  routecell c;
  c.word = word;
  c.rank = lrank;
  c.prob = 1;
  cerr << "Slave" << rank << ": Rank on cellrank " << c.rank << endl;
  return c;
}

void calculateAndSyncSlave() {
	for(vector<column>::iterator it1 = columns.begin(); it1 < columns.end(); it1++){
		double totalwords = 0;
		//cerr << "Slave" << rank << ": The vector of the word " << (*it1).word << " have size " << (*it1).nextWords.size() << endl;
		for (vector<routecell>::iterator it2 = (*it1).nextWords.begin(); it2 < (*it1).nextWords.end(); it2++) {
		  //cerr << "Slave" << rank << ": The word " << (*it2).word << " goes to rank " << (*it2).rank << endl; 
		  totalwords += (*it2).prob;
		}
		for (vector<routecell>::iterator it2 = (*it1).nextWords.begin(); it2 < (*it1).nextWords.end(); it2++) {
			(*it2).prob = (*it2).prob / totalwords;
		}
		vector<routecell> unsort = (*it1).nextWords;
		vector<routecell> sorted(0);
		bool isIn = false;
		for (vector<routecell>::iterator it3 = unsort.begin(); it3 < unsort.end(); it3++) {
		  isIn = false;
		  for (vector<routecell>::iterator it4 = sorted.begin(); it4 < sorted.end(); it4++) {
		    if ((*it4).prob > (*it3).prob) {
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
		//cerr << "Slave" << rank << ": The vector of the word " << (*it1).word << " have size " << (*it1).nextWords.size() << endl;
	}
	////////////cerr << "Slave: Printing columns" << endl;
	//for(vector<column>::iterator it1 = columns.begin(); it1 < columns.end(); it1++){
	////////////cerr << "word - " << (*it1).word << endl;
	//  for (vector<routecell>::iterator it2 = (*it1).nextWords.begin(); it2 < (*it1).nextWords.end(); it2++) {
	//    //////////cerr << "\tprob - " << (*it2).prob << "\tword - " << (*it2).word << endl;
	//  }
	//}	
	
	MPI_Barrier(MPI_COMM_WORLD);
	//////////cerr << "Slave: Calculated probabilities and sync after barrier" << endl;
}
