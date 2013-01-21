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
  float prob;
  int rank;
};

struct column {
  string word;
  vector<routecell> nextWords;
};

vector<column> columns;

void addWord(string word);
string createMessage(string word,int bookNum, int seqNum);
void readBookMessage(string message,string &word, int &bookNum, int &seqNum);
void readColumnMessage(string message,string &wordRequested,string &wordToGo,int &rank) ;
routecell searchNextWord(string word);
routecell addWordToColumn(string word, int rank);
void calculateAndSyncSlave();

void slave(int rank) {
	string message;
	string masterMessage;
	string slaveMessage;
	string word;
	int bookNum;
	int seqNum;
	bool construct = true;	
	routecell cell;

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
	cout << "Slave: Start to create books." << endl;
	while(true) {      
	  message = receiveMessage();
	  cout << "Slave: Comparation !!!" << message.substr(0, 14) << "!!!" << endl;
	  if (message.compare("BALANCE") == 0) {
	    //TODO work overload
	  }
	  else if (message.substr(0, 14).compare("<resume-slave>") == 0) {
	    cout << "Slave: End slave." << endl;
	    break;
	  }
	  else {
	    cout << "Slave: Recive word " << message << endl;
	    readBookMessage(message, word, bookNum, seqNum);
	    cell = searchNextWord(word);
	    masterMessage = createMessage(word, bookNum, seqNum);
	    slaveMessage  = createMessage(cell.word, bookNum, seqNum + 1);
	    cout << "Slave: Master message - " << masterMessage << endl;
	    cout << "Slave: Slave message - " << slaveMessage << endl;
	    sendMessage(masterMessage, 0);
	    sendMessage(slaveMessage, cell.rank);// 0 rank of slav
	  }	
	}
	return;
}

    void addWord(string word) {
	    bool notInWord1 = true;
	    bool notInWord2 = true;
	    string word1;
	    string word2;
	    int rank;
	    routecell rc;

	    readColumnMessage(word, word1, word2, rank);
	    cout << "Slave: Rank " << rank << endl;
	    cout << "Slave: Word 1 " << word1 << endl;
	    cout << "Slave: Word 2 " << word2 << endl;
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
				    rc = addWordToColumn(word2,rank);
				    ((*it1).nextWords.push_back(rc));

			    }
			    break;
		    }
	    }
	    if(notInWord1) {
		    column* c = new column();
		    (*c).word = word1;
		    columns.push_back(*c);
		    rc = addWordToColumn(word2,rank);
		    columns.back().nextWords.push_back(rc);
	    }

	    column col;
	    for (vector<column>::iterator it1 = columns.begin(); it1 < columns.end(); it1++) {
		    if(word1.compare((*it1).word)==0) {
			    col = (*it1);
		    }
	    }
	    cout << "Slave " << rank << " atendend word: " << col.word << endl;

	    cout << "Slave " << rank << " goes to : ";

	    for(vector<routecell>::iterator it3 = col.nextWords.begin(); it3 < col.nextWords.end(); it3++) {
		    cout<< " word: " << (*it3).word << " - " << "rank: " << (*it3).rank << "\t"; 
	    }
	    cout << endl;
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

      cout << "Slave: Read book message " << message << endl;
      pos1 = message.find("¬");
      word = message.substr(0, pos1);
      cout << "Slave: Read book word " << word << endl;
      pos2 = message.find("¬", pos1+1);
      bookStr = message.substr(pos1+1, pos2-(pos1));
      cout << "Slave: Read book bookStr " << bookStr << endl;
      bookStr = bookStr.substr(1, bookStr.length());
      bookStr = bookStr.substr(0, bookStr.length()-1);
      cout << "Slave: Read book bookStr " << bookStr << endl;
      pos3 = message.find("¬",pos2+1);
      seqStr = message.substr(pos2+1,pos3-(pos2));
      cout << "Slave: Read book seqStr " << seqStr << endl;
      seqStr = seqStr.substr(1, seqStr.length());
      seqStr = seqStr.substr(0, seqStr.length()-1);      
      cout << "Slave: Read book seqStr " << seqStr << endl;
      //      cout << "Slave: pos1 - " << pos1 << " pos2 - " << pos2 << endl;
    
      stringstream convertSeq(seqStr);
      convertSeq >> seqNum;
      stringstream convertBook(bookStr);
      convertBook >> bookNum;
      cout << "Slave: word - " << word << " seq - " << seqNum << " book - " << bookNum << endl;
    }

    void readColumnMessage(string message, string &wordRequested, string &wordToGo, int &rank) {
	    size_t pos1;
	    size_t pos2;
	    size_t pos3;
	    string rankStr;

	    //cout << endl;	
	    //cout << "Slave: readColumnMessage message : " << message << endl;
	    //cout << "Slave: readColumnMessage message length: " << message.length() << endl;
	    pos1 = message.find("¬");
	    wordRequested = message.substr(0, pos1);
	    //cout << "Slave: readColumnMessage wr: " << wordRequested << endl;
	    pos2 = message.find("¬", pos1 + 1);
	    wordToGo = message.substr(pos1+1, pos2-(pos1));
	    //cout << "Slave: readColumnMessage wtg: " << wordToGo << endl;
	    wordToGo = wordToGo.substr(1, wordToGo.length());
	    wordToGo = wordToGo.substr(0, wordToGo.length()-1);
	    //cout << "Slave: readColumnMessage wtg: " << wordToGo << endl;
	    //cout << "Slave: pos1 " << pos1 << " pos2 " << pos2 << endl;
	    pos3 = message.find("¬", pos2 + 1);
	    rankStr = message.substr(pos2+1,pos3-(pos2));
	    //cout << "Slave: readColumnMessage rs " << rankStr << endl;
	    rankStr = rankStr.substr(1, rankStr.length());
	    rankStr = rankStr.substr(0, rankStr.length()-1);
	    //cout << "Slave: readColumnMessage rs " << rankStr << endl;
	    stringstream convertRank(rankStr);
	    convertRank >> rank;
    }

routecell randomWord(vector<routecell> nextWords){
  float randtmp;
  float randinit = rand() / RAND_MAX;
  for(vector<routecell>::iterator it = nextWords.begin(); it < nextWords.end(); it++) {
    if ((randtmp = randinit - (*it).prob) < 0) {
      return (*it);
    };
  }
}

routecell searchNextWord(string word) {
  for (vector<column>::iterator it1 = columns.begin(); it1 < columns.end(); it1++) {
    //cout << "Slave: Compare " << word << " " << (*it1).word << endl;
    if(word.compare((*it1).word) == 0) {
      //cout << "Slave: Finded word " << (*it1).word << endl;
      return randomWord((*it1).nextWords);
    }
  }
}

routecell addWordToColumn(string word, int rank) {
	routecell* c = new routecell();
	(*c).word = word;
	(*c).rank = rank;
	(*c).prob = 1;
	return *c;
}

void calculateAndSyncSlave() {
	
	for(vector<column>::iterator it1 = columns.begin(); it1 < columns.end(); it1++){
		int totalwords = 0;
		for (vector<routecell>::iterator it2 = (*it1).nextWords.begin(); it2 < (*it1).nextWords.end(); it2++) {
			totalwords += (*it2).prob;
		}
		for (vector<routecell>::iterator it2 = (*it1).nextWords.begin(); it2 < (*it1).nextWords.end(); it2++) {
			(*it2).prob = (*it2).prob / totalwords;
		}
		vector<routecell> unsort = (*it1).nextWords;
		vector<routecell> sorted(unsort.size());
		bool isIn = false;
		for (vector<routecell>::iterator it3 = unsort.begin(); it3 < unsort.end(); it3++) {
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
	}
	
	
	MPI_Barrier(MPI_COMM_WORLD);
	cout << "Slave: Calculated probabilities and sync after barrier" << endl;
}
