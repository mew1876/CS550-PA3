#include "rpc/server.h"
#include "rpc/client.h"
#include "rpc/rpc_error.h"
#include <iostream>
#include <vector>
#include <string>
#include <array>
#include <unordered_map>
#include <map>
#include <unordered_set>
#include <set>
#include <algorithm>
#include <mutex>
#include <condition_variable>

void query(int sender, std::array<int, 2> messageId, int TTL, std::string fileName);
void queryHit(int sender, std::array<int, 2> messageId, int TTL, std::string fileName, std::vector<int> leaves);
void invalidate(std::array<int, 2> messageId, int TTL, std::string fileName, int versionNumber);
void add(int leafId, std::string fileName);
rpc::client* getClient(int id);
void leafReady();
void end();
void ping();
void dumpIndex();

int id, nSupers, nChildren, startTTL;
std::unordered_map<int, rpc::client*> neighborClients;
std::unordered_map<int, rpc::client*> leafClients;

std::unordered_map<std::string, std::vector<int>> fileIndex;
std::map<std::array<int, 2>, std::unordered_set<int>> queryHistory;
std::set<std::array<int, 2>> invalidateHistory;
std::set<std::array<int, 2>> queryHitIds;

int readyCount = 0;
bool canEnd;
std::mutex countLock;
std::mutex historyLock;
std::mutex invalidateLock;
std::mutex indexLock;
std::condition_variable ready;

int main(int argc, char* argv[]) {
	//Parse args for ID, nChildren, neighbors
	if (argc < 3) {
		return -1;
	}
	id = std::stoi(argv[0]);
	nSupers = std::stoi(argv[1]);
	nChildren = std::stoi(argv[2]);
	startTTL = std::stoi(argv[3]);
	//Start server for file registrations, pings, ready signals, queries, queryhits, and end signal
	rpc::server server(8000 + id);
	server.bind("ready", &leafReady);
	server.bind("add", &add);
	server.bind("query", &query);
	server.bind("queryHit", &queryHit);
	server.bind("ping", &ping);
	server.bind("invalidate", &invalidate);
	server.bind("end", &end);
	server.async_run(4);
	std::cout << "Im a super with ID " << id << std::endl;
	//Create clients for neighbors once they're online
	for (int i = 4; i < argc; i++) {
		int neighborId = std::stoi(argv[i]);
		rpc::client *neighborClient = new rpc::client("localhost", 8000 + neighborId);
		neighborClient->set_timeout(1000);
		//Ping server until it responds
		while (true) {
			try {
				neighborClient->call("ping");
				break;
			}
			catch (rpc::timeout &t) {
				//Ping timed out, try restarting client
				delete neighborClient;
				neighborClient = new rpc::client("localhost", 8000 + neighborId);
				neighborClient->set_timeout(1000);
				t; //Silence warning
			}
		}
		neighborClient->clear_timeout();
		neighborClients.insert({ neighborId, neighborClient });
	}
	//Wait for all children to give ready signal
	std::unique_lock<std::mutex> unique(countLock);
	ready.wait(unique, [] { return readyCount >= nChildren; });
	std::cout << "----- Children Ready -----" << std::endl;
	//Send ready signal to system
	rpc::client sysClient("localhost", 8000);
	sysClient.call("ready");
	//Wait for end signal
	ready.wait(unique, [] { return canEnd; });
	for (auto client : neighborClients) {
		delete client.second;
	}
	for (auto client : leafClients) {
		delete client.second;
	}
}

void query(int sender, std::array<int, 2> messageId, int TTL, std::string fileName) {
	historyLock.lock();
	//std::cout << sender << " wants " << fileName << ". Is it in ";
	//If we've seen the message before, skip query handling
	std::unordered_set<int> &senders = queryHistory[messageId];
	//for (auto file : fileIndex) {
	//	std::cout << file.first << " ";
	//}
	std::cout << std::endl;
	if (senders.empty()) {
		//Add new sender to history
		senders.insert(sender);
		historyLock.unlock();
		//Check own index for the file
		indexLock.lock();
		const auto indexEntry = fileIndex.find(fileName);
		if (indexEntry != fileIndex.end()) {
			//Reply with queryHit
			//std::cout << "File found! Replying to " << sender << " about " << fileName << std::endl;
			//std::cout << "hit" << std::endl;
			getClient(sender)->async_call("queryHit", id, messageId, startTTL, fileName, indexEntry->second);
			queryHitIds.insert(messageId);
		}
		else if (TTL - 1 > 0) {
			//Forward query to neighbors
			//std::cout << "miss" << std::endl;
			//std::cout << "Forwarding query for " << fileName << " to neighbors: ";
			for (auto &neighbor : neighborClients) {
				if (neighbor.first != sender) {
					//std::cout << " " << neighbor.first;
					neighbor.second->async_call("query", id, messageId, TTL - 1, fileName);
				}
			}
			//std::cout << std::endl;
		}
		indexLock.unlock();
	}
	else {
		//std::cout << "skipped. messageId: " << messageId[0] << " " << messageId[1] << std::endl;
		//Add new sender to history
		senders.insert(sender);
		historyLock.unlock();
	}
}

void queryHit(int sender, std::array<int, 2> messageId, int TTL, std::string fileName, std::vector<int> leaves) {
	historyLock.lock();
	std::cout << sender << ">>" << fileName << ">> ";
	if (queryHitIds.find(messageId) == queryHitIds.end()) {
		//std::cout << "Propagating hit for " << fileName << " to: ";
		const auto senders = queryHistory.find(messageId);
		if (senders != queryHistory.end() && TTL - 1 > 0) {
			//Forward queryHit to anyone who sent query with messageId
			for (int querySenderId : senders->second) {
				if (senders->first[0] != sender) {
					std::cout << querySenderId << " ";
					getClient(querySenderId)->async_call("queryHit", id, messageId, TTL - 1, fileName, leaves);
				}
			}
		}
		queryHitIds.insert(messageId);
	}
	std::cout << "|" << std::endl;
	historyLock.unlock();
}

void invalidate(std::array<int, 2> messageId, int TTL, std::string fileName, int versionNumber) {
	invalidateLock.lock();
	if (invalidateHistory.find(messageId) == invalidateHistory.end()) {
		invalidateHistory.insert(messageId);
		invalidateLock.unlock();
		for (auto client : leafClients) {
			client.second->async_call("invalidate", messageId, TTL - 1, fileName, versionNumber);
		}
		if (TTL - 1 > 0) {
			for (auto client : neighborClients) {
				client.second->async_call("invalidate", messageId, TTL - 1, fileName, versionNumber);
			}
		}
	}
	else {
		invalidateLock.unlock();
	}
}

void add(int leafId, std::string fileName) {
	indexLock.lock();
	std::cout << "File registered: " << leafId << " has " << fileName << std::endl;
	std::vector<int> &leaves = fileIndex[fileName];
	if (std::find(leaves.begin(), leaves.end(), leafId) == leaves.end()) {
		leaves.push_back(leafId);
	}
	indexLock.unlock();
}

rpc::client* getClient(int clientId) {
	//Return a client - neighbor or leaf
	//std::cout << "Client: " << clientId;
	const auto neighborIter = neighborClients.find(clientId);
	if (neighborIter != neighborClients.end()) {
		//std::cout << "neighbor" << std::endl;
		return neighborIter->second;
	}
	const auto leafIter = leafClients.find(clientId);
	if (leafIter != leafClients.end()) {
		//std::cout << "leaf" << std::endl;
		return leafIter->second;
	}
	//If the client doesn't exist yet, we assume its a leaf
	rpc::client *client = new rpc::client("localhost", 8000 + clientId);
	leafClients.insert({ clientId, client });
	//std::cout << "new" << std::endl;
	return leafClients.find(clientId)->second;
}

void leafReady() {
	countLock.lock();
	std::cout << "leaf ready" << std::endl;
	readyCount++;
	countLock.unlock();
	ready.notify_one();
}

void end() {
	canEnd = true;
	ready.notify_one();
}

void ping() {}

void dumpIndex() {
	std::cout << "Dump:" << std::endl;
	for (auto tuple : fileIndex) {
		std::cout << tuple.first << ": ";
		for (auto IdIter : tuple.second) {
			std::cout << IdIter << " ";
		}
		std::cout << std::endl;
	}
}