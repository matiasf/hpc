#ifndef UTILS_H
#define UTILS_H

#include <string>

using namespace std;

int sendMessage(string message, int rank);

string receiveMessage();

#endif
