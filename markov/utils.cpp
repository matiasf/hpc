#include "mpi.h"

int sendMessage(string message, int rank) {
  MPI_Request request;
  return MPI_Isend(message.c_str(), message.size(), MPI_CHAR, rank, rank, MPI_COMM_WORLD, &request);
}

string receiveMessage() {
	MPI_Recv(word, BUFFER_MESSAGE, MPI_CHAR, MPI_ANY_SOURCE,MPI_ANY_SOURCE, MPI_COMM_WORLD, &status);
	return string(word); 
}
