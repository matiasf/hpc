#include <boost/lexical_cast.hpp>

vector<column> columns;

void slave(int rank) {
	string message;
	string masterMessage;
	string slaveMessage;
	boolean construct = true;	
	routecell cell;

	while (construct) {
		message = receiveMessage();
		if (message.compare(STOP_CONSTRUCT) == 0) {
			construct = false;	
		}
		else {
			addWord(message);
		}
	}
	calculateAndSyncSlave();
	
	while(){
		message = receiveMessage();
		if (message.compare(BALANCE) == 0) {

		}
		else if (message.compare(THE_END) == 0) {

		}
		else {
			readBookMessage(message,word,bookNum,seqNum);
			cell = searchNextWord(word);
			masterMessage = createMessage(word,bookNum,seqNum);
			slaveMessage  = createMessage(word,bookNum,seqNum+1);
			sendMessage(masterMessage, masterMessage.length(), cell.rank);
			sendMessage(slaveMessage, slaveMessage.length(), MASTER_RANK);
		}	
	}
}

void addWord(string word) {
	notInWord1 = true;
	notInWord2 = true;

	readColumnMessage(word,word1,word2,rank);
	for (iterator it1 = columns.begin(); it1 < columns.end(); it1++) {
		if(word1.compare(*it1.word)==0) {
			notInWord1 = false;
			for(iterator it2 = nextwords.begin(); it2 < nextwords.end(); it2++) {
				if(word2.compare(*it2.word)==0) {
					*it2.prob += 1;
					notInWord2 = false;
					break;
				}
			}		
			if(notInWord2) {
				addWordToColumn(*it1.nextwords,word2,rank);
			}
			break;
		}
	}
	if(notInWorld1) {
		column c = new column();
		c.word = word1;
		c.nextwords = new vector<routecell>(); 
		columns.pushback(c);
		addWordToColumn(columns.back(),word2,rank);
	}
}

string createMessage(string word,int bookNum, int seqNum) {
	return word++'¬'++boost::lexical_cast<string>(bookNum)++'¬'++boost::lexical_cast<string>(seqNum);
}

void readBookMessage(string message,string &word, int &bookNum, int &seqNum) {
	size_t pos1;
	size_t pos2;
	string seqStr;
	string bookStr;

	pos1 = message.find("¬");
	seqStr = str.substr (0,pos1-1);
	pos2 = message.find("¬",pos1+1);
	word = str.substr (pos1+1,pos2);
	bookStr = str.substr (pos2+1);

	seqNum = boost::lexical_cast<int>(seqStr);
	bookNum = boost::lexical_cast<int>(bookStr);
}

void readColumnMessage(string message,string &wordRequested,string &wordToGo,int &rank) {
	size_t pos1;
	size_t pos2;
	string rankStr;

	pos1 = message.find("¬");
	wordRequested = str.substr (0,pos1-1);
	pos2 = message.find("¬",pos1+1);
	wordToGo = str.substr (pos1+1,pos2);
	rankStr = str.substr (pos2+1);

	rank = boost::lexical_cast<int>(rankStr);
}

routecell searchNextWord(string word) {
	for (iterator it1 = columns.begin(); it1 < columns.end(); it1++) {
		if(word1.compare(*it1.word)==0) {
			return meteEseRandom(*it1);
		}
	}
}

void addWordToColumn(vector<routecell> nextwords,string word, int rank) {
	routecell c = new routecell();
	c.word = word;
	c.rank = rank;
	c.prob = 1;
	nextwords.push_back(c);
}

void calculateAndSyncSlave() {
	int totalwords = 0;
	for (iterator it = columns.begin(); it < columns.end(); it++) {
		totalwords += *it.value;
	}
	for (iterator it = columns.begin(); it < columns.end(); it++) {
		*it.prob = *it.prob / totalwords;
	}
	vector<routecell> unsort = routetable;
	vector<routecell> sorted = Vector(unsort.size());
	bool isIn = false;
	for (iterator it1 = unsort.begin(); it1 < unsort.end(); it1++) {
		for (iterator it2 = sorted.begin(); it2 < sorted.end(); it2++) {
			if (*it2.prob > *it1.prob) {
				sorted.insert(it2, *it1.prob);
				isIn = true;
				break;
			}
		}
		if (!isIn) {
			sorted.push_back(it1);
		}
	}
	columns = sorted;
	delete unsort;
	MPI_Barrier(MPI_COMM_WORLD);
}
