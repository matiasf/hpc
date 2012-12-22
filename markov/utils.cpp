#include <string>

#include "mpi.h"

#define BUFFER_MESSAGE 216;

using namespace std;

struct split
{
  enum empties_t { empties_ok, no_empties };
};

void sendMessage(string message, int rank) {
  MPI_Request request;
  MPI_Isend(&message, message.size(), MPI_CHAR, rank, rank, MPI_COMM_WORLD, &request);
}

string receiveMessage() {
  char* word;
  MPI_Status status;
  MPI_Recv(word, 216, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_SOURCE, MPI_COMM_WORLD, &status);
  string str(word);
  return str; 
}
