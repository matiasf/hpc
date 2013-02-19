#include <string>
#include <iostream>

#include "mpi.h"

using namespace std;

struct split
{
  enum empties_t { empties_ok, no_empties };
};

void sendMessage(string message, int rank) {
  MPI_Request request;
	int size;
	MPI_Pack_size(message.size(), MPI_CHAR, MPI_COMM_WORLD, &size);
	void* buf = operator new(size + MPI_BSEND_OVERHEAD);
	MPI_Buffer_attach(buf, size + MPI_BSEND_OVERHEAD);
  //cout << "Utils: Message size to send is " << message.size() << endl; 
  MPI_Bsend((char*)message.c_str(), message.size(), MPI_CHAR, rank, 1, MPI_COMM_WORLD);
	MPI_Buffer_detach(&buf, &size);
	operator delete(buf);
}

string receiveMessage() {
  char word[216];
  MPI_Recv(&word, 216, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  //cout << "Utils: Recived word " << word << endl;
  string result(word);
  return result; 
}

string receiveMessageHurry(int *toread) {
  //cerr << "Utils: Checking channel ..." << endl;
  MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, toread, MPI_STATUS_IGNORE);
  if (*toread) {
    //cerr << "Utils: We have a message" << endl;
    char word[216];
    MPI_Recv(&word, 216, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    string result(word);
    return result;
  }
  //cerr << "Utils: No message!" << endl;
  return "";
}
