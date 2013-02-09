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
  //cout << "Utils: Message size to send is " << message.size() << endl;
  char* tmpmessage; 
  MPI_Isend((char*)message.c_str(), message.size(), MPI_CHAR, rank, rank, MPI_COMM_WORLD, &request);
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
