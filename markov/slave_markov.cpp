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
using namespace std;

#define INIT_WORD "INIT";
#define END_WORD "END";
#define STOP_CONSTRUCT "STOP";

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
void addWordToColumn(vector<routecell> nextWords,string word, int rank);
void calculateAndSyncSlave();

string receiveMessage(){
	return "";
}

void sendMessage(string masterMessage, int rank){
}

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
		if (message.compare("STOP") == 0) {
			construct = false;	
		}
		else {
			addWord(message);
		}
	}
	calculateAndSyncSlave();
	
	while(true){
		message = receiveMessage();
		if (message.compare("BALANCE") == 0) {

		}
		else if (message.compare("THE_END") == 0) {

		}
		else {
			readBookMessage(message,word,bookNum,seqNum);
			cell = searchNextWord(word);
			masterMessage = createMessage(word,bookNum,seqNum);
			slaveMessage  = createMessage(word,bookNum,seqNum+1);
			sendMessage(masterMessage, cell.rank);
			sendMessage(slaveMessage, 0);
		}	
	}
}

void addWord(string word) {
	bool notInWord1 = true;
	bool notInWord2 = true;
	string word1;
	string word2;
	int rank;

	readColumnMessage(word,word1,word2,rank);
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
				addWordToColumn((*it1).nextWords,word2,rank);
			}
			break;
		}
	}
	if(notInWord1) {
		//TODO
		column* c = (struct column *) malloc(sizeof(struct column));
		(*c).word = word1;
		columns.push_back(*c);
		addWordToColumn(columns.back().nextWords,word2,rank);
	}
}

string createMessage(string word,int bookNum, int seqNum) {
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

	return returnStr;
}

void readBookMessage(string message,string &word, int &bookNum, int &seqNum) {
	size_t pos1;
	size_t pos2;
	string seqStr;
	string bookStr;

	pos1 = message.find("¬");
	seqStr = message.substr (0,pos1-1);
	pos2 = message.find("¬",pos1+1);
	word = message.substr (pos1+1,pos2);
	bookStr = message.substr (pos2+1);

	stringstream convertSeq(seqStr);
	convertSeq >> seqNum;
	stringstream convertBook(bookStr);
	convertSeq >> bookNum;
}

void readColumnMessage(string message,string &wordRequested,string &wordToGo,int &rank) {
	size_t pos1;
	size_t pos2;
	string rankStr;

	pos1 = message.find("¬");
	wordRequested = message.substr (0,pos1-1);
	pos2 = message.find("¬",pos1+1);
	wordToGo = message.substr (pos1+1,pos2);
	rankStr = message.substr (pos2+1);

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
		if(word.compare((*it1).word)==0) {
			return randomWord((*it1).nextWords);
		}
	}
}

void addWordToColumn(vector<routecell> nextWords,string word, int rank) {
	routecell* c = (struct routecell *) malloc(sizeof(struct routecell));;
	(*c).word = word;
	(*c).rank = rank;
	(*c).prob = 1;
	nextWords.push_back(*c);
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
}
