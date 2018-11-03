#include "rpc/server.h"
#include "rpc/client.h"
#include "rpc/rpc_error.h"
#include "rpc/this_server.h"
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
void invalidate(std::array<int, 2> messageId, int masterId, int TTL, std::string fileName, int versionNumber);
void add(int leafId, std::string fileName, int version);
rpc::client* getClient(int id);
void leafReady();
void end();
void ping();
void dumpIndex();
void updateVersion(int leafId, std::string fileName, int version);
void checkVersion(int sender, std::string fileName, int version);
void fileOutOfDate(std::string fileName);

int id, nSupers, nChildren, startTTL;
bool push = false, pull = false;
std::unordered_map<int, rpc::client*> neighborClients;
std::unordered_map<int, rpc::client*> leafClients;

std::unordered_map<std::string, std::vector<int>> fileIndex;
std::unordered_map<std::string, std::unordered_map<int, std::pair<int,bool>>> fileVersionIndex; // fileName -> {leafID -> (version,isValid)}
//std::unordered_map<std::string, std::vector<int>> invalidFiles;
std::map<std::array<int, 2>, std::unordered_set<int>> queryHistory;
std::set<std::array<int, 2>> invalidateHistory;

int readyCount = 0;
bool canEnd;
std::mutex countLock;
std::mutex historyLock;
std::mutex invalidateLock;
std::mutex indexLock;
std::mutex printlock;
std::condition_variable ready;

int main(int argc, char* argv[]) {
	//Parse args for ID, nChildren, neighbors
	if (argc < 4) {
		return -1;
	}
	id = std::stoi(argv[0]);
	nSupers = std::stoi(argv[1]);
	nChildren = std::stoi(argv[2]);
	startTTL = std::stoi(argv[3]);
	int mode = std::stoi(argv[4]);
	if (mode == 1 || mode == 3) {
		push = true;
	}
	if (mode == 2 || mode == 3) {
		pull = true;
	}
	//Start server for file registrations, pings, ready signals, queries, queryhits, and end signal
	rpc::server server(8000 + id);
	server.bind("ready", &leafReady);
	server.bind("add", &add);
	server.bind("query", &query);
	server.bind("queryHit", &queryHit);
	server.bind("ping", &ping);
	server.bind("invalidate", &invalidate);
	server.bind("end", &end);
	server.bind("updateVersion", &updateVersion);
	server.bind("fileOutOfDate", &fileOutOfDate);
	server.bind("checkVersion", &checkVersion);
	server.bind("stop_server", []() {
		rpc::this_server().stop();
	});
	server.async_run(4);
	std::cout << "Im a super with ID " << id << std::endl;
	//Create clients for neighbors once they're online
	for (int i = 5; i < argc; i++) {
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
	ready.wait(unique, [] { return canEnd && false; });
	//std::this_thread::sleep_for(std::chrono::milliseconds(5000));
	//Wait for own server to end gracefully
	rpc::client selfClient("localhost", 8000 + id);
	selfClient.call("stop_server");
	//Free clients
	for (auto client : neighborClients) {
		delete client.second;
	}
	for (auto client : leafClients) {
		delete client.second;
	}
	std::cout << "dead" << std::endl;
}

void query(int sender, std::array<int, 2> messageId, int TTL, std::string fileName) {
	historyLock.lock();
	printlock.lock();
	std::cout << sender << " wants " << fileName << std::endl;
	printlock.unlock();
	std::unordered_set<int> &senders = queryHistory[messageId];
	if (senders.empty()) {
		//Add new sender to history
		senders.insert(sender);
		historyLock.unlock();
		//Check own index for the file
		indexLock.lock();
		const auto indexEntry = fileIndex.find(fileName);
		if (indexEntry != fileIndex.end()) {
			//Reply with queryHit
			printlock.lock();
			std::cout << "File found! Replying to " << sender << " about " << fileName << " at: ";
			for (auto entry : indexEntry->second) {
				std::cout << entry << " ";
			}
			std::cout << std::endl;
			printlock.unlock();
			//std::cout << "hit" << std::endl;
			getClient(sender)->async_call("queryHit", id, messageId, startTTL, fileName, indexEntry->second);
		}
		if (TTL - 1 > 0) {
			//Forward query to neighbors
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
	printlock.lock();
	std::cout << "Forwarding queryhit for " << fileName<< " from " << sender << " to: ";
	const auto senders = queryHistory.find(messageId);
	if (senders != queryHistory.end() && TTL - 1 > 0) {
		//Forward queryHit to anyone who sent query with messageId
		for (int querySenderId : senders->second) {
			if (senders->first[0] != sender) {
				std::cout << querySenderId << " ";
				getClient(querySenderId)->async_call("queryHit", id, messageId, TTL - 1, fileName, leaves);
			}
			std::cout << std::endl;
		}
	}
	printlock.unlock();
	historyLock.unlock();
}

void invalidate(std::array<int, 2> messageId, int masterId, int TTL, std::string fileName, int versionNumber) {
	//std::cout << "Forwarding invalidate for " << fileName << std::endl;
	invalidateLock.lock();
	if (invalidateHistory.find(messageId) == invalidateHistory.end()) {
		invalidateHistory.insert(messageId);
		invalidateLock.unlock();
		// send invalidate to leaves
		for (auto client : leafClients) {
			client.second->async_call("invalidate", messageId, masterId, TTL - 1, fileName, versionNumber);
		}
		// send invalidate to neighbors
		if (TTL - 1 > 0) {
			for (auto client : neighborClients) {
				client.second->async_call("invalidate", messageId, masterId, TTL - 1, fileName, versionNumber);
			}
		}
	}
	else {
		invalidateLock.unlock();
	}
}

void add(int leafId, std::string fileName, int version) {
	indexLock.lock();
	std::cout << "File registered: " << leafId << " has " << fileName << std::endl;
	std::vector<int> &leaves = fileIndex[fileName];

	fileVersionIndex[fileName][leafId].first = version;
	fileVersionIndex[fileName][leafId].second = true;
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

void updateVersion(int leafId, std::string fileName, int version) {
	indexLock.lock();
	const auto mapEntry = fileVersionIndex.find(fileName); // iterator, {leafId -> (version,isValid)}
	if (mapEntry != fileVersionIndex.end()) {
		// found fileName in fileVersionIndex
		const auto pairEntry = (mapEntry->second).find(leafId);
		if (pairEntry != (mapEntry->second).end()) {
			// found leafID, update the version and validity
			auto &fileVersion = (pairEntry->second).first;
			fileVersion = version;
			auto &isFileValid = (pairEntry->second).second;
			isFileValid = true;
		}
	}
	indexLock.unlock();
}

void checkVersion(int sender, std::string fileName, int version) {
	indexLock.lock();
	int newestVersion = -1;
	const auto mapEntry = fileVersionIndex.find(fileName); // iterator, {leafId -> (version,isValid)}
	if (mapEntry != fileVersionIndex.end()) {
		// found fileName in fileVersionIndex
		int newestVersion = -1;
		for (auto const &pairEntry : (mapEntry->second)) {
			auto &fileVersion = (pairEntry.second).first;
			if (fileVersion > newestVersion) {
				newestVersion = fileVersion;
			}
		}
		if (newestVersion > version) {
			getClient(sender)->async_call("fileOutOfDate", fileName);
		}
	}
	indexLock.unlock();
}

void fileOutOfDate(std::string fileName) {
	const auto &indexEntry = fileIndex.find(fileName);
	for (auto const &leafNodeID : (indexEntry->second)) {
		getClient(leafNodeID)->async_call("invalidate", fileName);
	}
}