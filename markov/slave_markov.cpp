#include <boost/lexical_cast.hpp>

void slave(int rank) {
	char* word;
	boolean construct = true;
	vector<column> columns;
	while (construct) {
		word = receiveMessage();
		if (strcmp(word, STOP_CONSTRUCT) == 0) {
			construct = false;	
		}
		else {
			addWord(word,columns);
		}
	}

	calculateProbabilities(columns);
	
	while(){

		message = receiveMessage();
		
		if (strcmp(word, BALANCE) == 0) {
		}
		else if (strcmp(word, THE_END) == 0) {
		}
		else {
			readMessage(message,seqNum,word,bookNum);
			cell = searchNextWord(word,columns);
			masterMessage = createMessage(seqNum,word,bookNum);
			slaveMessage  = createMessage(seqNum+1,word,bookNum);
			sendMessage(masterMessage, masterMessage.length(), cell.rank);
			sendMessage(slaveMessage, slaveMessage.length(), MASTER_RANK);
		}	
	}
}

void addWord(word,columns) {
	
	notInWord1 = true;
	notInWord2 = true;
	readMessage(word,word1,word2);
	for (iterator it1 = columns.begin(); it1 < columns.end(); it1++) {
		if(word1.compare(*it1.word)==0) {
			notInWord1 = false;
			for(iterator it2 = nextwords.begin(); it2 < nextwords.end(); it2++) {
				if(word2.compare(*it2.word)==0) {
					*it2.value += 1;
					notInWord2 = false;
					break;
				}
			}		
			if(notInWord2) {
				addWordToColumn(word2,*it1.nextwords);
			}
			break;
		}
	}
	if(notInWorld1) {
		columns.pushback(algoAhi);
		addWordToColumn(word2,columns.back());
	}
}

string createMessage(int seqNum,string word,int bookNum) {
	return boost::lexical_cast<string>(seqNum)++'¬'++word++'¬'++boost::lexical_cast<string>(seqNum);
}

void readMessage(string message,int seqNum,string word,int bookNum) {
	size_t pos;

	pos1 = message.find("¬");
	seqStr = str.substr (0,pos1-1);
	pos2 = message.find("¬",pos1+1);
	word = str.substr (pos1+1,pos2);
	bookStr = str.substr (pos2+1);

	seqNum = boost::lexical_cast<int>(seqStr);
	bookNum = boost::lexical_cast<int>(bookStr);

}

routecell searchNextWord(word,columns) {
	for (iterator it1 = columns.begin(); it1 < columns.end(); it1++) {
		if(word1.compare(*it1.word)==0) {
			return meteEseRandom(*it1);
		}
	}
}

void addWordToColumn(word2,nextwords) {
	routecell c = new routecell();
	c.word = word2;
	c.rank = ????;
	nextwords.push_back(c);
}
