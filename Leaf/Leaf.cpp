#include "rpc/server.h"
#include "rpc/client.h"
#include "rpc/this_handler.h"
#include "rpc/this_server.h"
#include "rpc/rpc_error.h"
#include <iostream>
#include <string>
#include <fstream>
#include <ctime>
#include <array>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <thread>

#define PUSH false
#define PULL false
#define PULL2 true

void queryHit(int sender, std::array<int, 2> messageId, int TTL, std::string fileName, std::vector<int> leaves);
void invalidate(std::array<int, 2> messageId, int masterId, int TTL, std::string fileName, int versionNumber);
void downloadFile(std::vector<int> sources, std::string fileName);
std::pair<std::vector<uint8_t>, int> obtain(std::string fileName);
rpc::client* getClient(int clientId);
void start();
void end();
std::string getPath();

int id, superId, nSupers, startTTL;
bool isExtra;
int nextMessageId = 0;
int pendingQueries = 0;
int valid = 0, invalid = 0;
std::unordered_map<std::string, std::array<int, 2>> retrievedFiles;
std::unordered_set<std::string> invalidFiles;
std::unordered_map<std::string, int> ownFiles;
std::unordered_map<int, rpc::client*> leafClients;
rpc::client *superClient;

bool canStart = false, canEnd = false;
std::mutex waitLock;
std::mutex queryCount;
std::mutex clientsLock;
std::mutex versionLock;
std::mutex metricLock;
std::mutex printlock;
std::condition_variable ready;

int main(int argc, char* argv[]) {
	//Parse args for ID, files to start with, files to request
	if (argc < 5) {
		return -1;
	}
	id = std::stoi(argv[0]);
	superId = std::stoi(argv[1]);
	nSupers = std::stoi(argv[2]);
	startTTL = std::stoi(argv[3]);
	isExtra = std::stoi(argv[4]);
	std::cout << "Im a leaf with ID " << id << " and my super's ID is " << superId << std::endl;
	//Start server for start, obtain, and end signals
	rpc::server server(8000 + id);
	server.bind("start", &start);
	server.bind("queryHit", &queryHit);
	server.bind("obtain", &obtain);
	server.bind("invalidate", &invalidate);
	server.bind("end", &end);
	server.bind("stop_server", []() {
		rpc::this_server().stop();
	});
	server.async_run(4);
	//Create super client
	superClient = new rpc::client("localhost", 8000 + superId);
	superClient->set_timeout(1000);
	//Ping server until it responds
	while (true) {
		try {
			std::cout << "Pinging" << std::endl;
			superClient->call("ping");
			std::cout << "Ping was successful" << std::endl;
			break;
		}
		catch (rpc::timeout &t) {
			//Ping timed out, try restarting client
			delete superClient;
			superClient = new rpc::client("localhost", 8000 + superId);
			superClient->set_timeout(1000);
			t; //Silence warning
		}
	}
	superClient->clear_timeout();
	//Create init files & add to super index
	CreateDirectory("Leaves", NULL);
	CreateDirectory(getPath().c_str(), NULL);
	int argIndex;
	for (argIndex = 5; argIndex < argc; argIndex++) {
		if (strcmp(argv[argIndex], std::string("requests").c_str()) == 0) {
			argIndex++;
			break;
		}
		std::string fileName(argv[argIndex]);
		std::ofstream file(getPath() + fileName);
		file << "Created by leaf " << id << std::endl;
		std::srand(unsigned int(std::time(nullptr)));
		for (int i = 0; i < argIndex * 1024; i++) {
			file << char((std::rand() % 95) + 32);
		}
		file.close();
		ownFiles.insert({ fileName, 0 });
		superClient->call("add", id, fileName, 0);
	}
	//Send ready signal to super
	superClient->call("ready");
	//Wait for start signal
	std::unique_lock<std::mutex> unique(waitLock);
	ready.wait(unique, [] { return canStart; });
	//Make file requests
	for (; argIndex < argc; argIndex++) {
		std::string fileName(argv[argIndex]);
		printlock.lock();
		std::cout << "Querying for " << fileName << std::endl;
		printlock.unlock();
		std::array<int, 2> messageId = { id, nextMessageId++ };
		//std::cout << "mId: " << messageId[0] << " " << messageId[1] << std::endl;
		superClient->async_call("query", id, messageId, startTTL, fileName);
		queryCount.lock();
		pendingQueries++;
		queryCount.unlock();
	}
	ready.wait(unique, [] { return pendingQueries == 0; });
	//Send complete signal to system
	rpc::client sysClient("localhost", 8000);
	//Report metrics
	if (isExtra) {
		sysClient.call("metrics", valid, invalid);
	}
	sysClient.call("complete");
	//Make 'updates' to random ownFiles
	std::srand(unsigned int(std::time(nullptr) + id));
	std::cout << "Starting to make random file updates" << std::endl;
	while (!canEnd) {
		if (ownFiles.empty()) {
			break;
		}
		const auto &file = std::next(std::begin(ownFiles), std::rand() % ownFiles.size());
		if (file == ownFiles.end()) {
			break;
		}
		file->second++; // increment version number
		if (PUSH) {
			//send push message to super
			printlock.lock();
			std::cout << "Pushing invalidate for version " << file->second << " of " << file->first << std::endl;
			printlock.unlock();
			std::array<int, 2> messageId = { id, nextMessageId++ };
			superClient->async_call("invalidate", messageId, id, startTTL, file->first, file->second);
		}
		if (PULL2) {
			//send updateVersion message to super
			superClient->async_call("updateVersion", id, file->first, file->second);
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(std::rand() % 1000));
	}
	delete superClient;
	//Wait for kill signal
	ready.wait(unique, [] { return canEnd && false; });
	//Wait for own server to end gracefully
	rpc::client selfClient("localhost", 8000 + id);
	selfClient.call("stop_server");
	if (isExtra) {
		std::cin.get();
	}
}

void queryHit(int sender, std::array<int, 2> messageId, int TTL, std::string fileName, std::vector<int> leaves) {
	printlock.lock();
	std::cout << "queryhit for " << fileName << " from " << sender << std::endl;
	printlock.unlock();
	//Try to copy file from each source in another thread until success
	std::thread(downloadFile, leaves, fileName).detach();
}

void invalidate(std::array<int, 2> messageId, int masterId, int TTL, std::string fileName, int versionNumber) {
	versionLock.lock();
	const auto &fileIter = retrievedFiles.find(fileName);
	if (fileIter != retrievedFiles.end() && fileIter->second[0] < versionNumber) {
		printlock.lock();
		std::cout << "Re-Downloading " << fileName << " version " << versionNumber << std::endl;
		//Update version number preemptively to block repeat invalidations
		fileIter->second[0] = versionNumber;
		//Mark file as invalid
		invalidFiles.insert(fileName);
		std::cout << "invalidated " << fileName << std::endl;
		printlock.unlock();
		//Download file from master
		std::vector<int> sourceIds = { masterId };
		std::thread(downloadFile, sourceIds, fileName).detach();
	}
	versionLock.unlock();
	superClient->async_call("updateVersion", id, fileName, versionNumber);
}

void downloadFile(std::vector<int> sources, std::string fileName) {
	for (unsigned int i = 0; i < sources.size(); i++) {
		try {
			printlock.lock();
			std::cout << "Sending file request to " << sources[i] << " for " << fileName << std::endl;
			printlock.unlock();
			//Download file
			std::pair<std::vector<uint8_t>, int> response = getClient(sources[i])->call("obtain", fileName).as<std::pair<std::vector<uint8_t>, int>>();
			std::vector<uint8_t> bytes = response.first;
			int versionNumber = response.second;
			versionLock.lock();
			bool isValid = false;
			bool fresh = false;
			if (invalidFiles.find(fileName) == invalidFiles.end()) {
				isValid = true;
			}
			if (retrievedFiles.find(fileName) == retrievedFiles.end()) {
				fresh = true;
			}
			printlock.lock();
			std::cout << "Read " << fileName << " from " << sources[i] << std::endl;
			printlock.unlock();
			if (fresh || !isValid) {
				//Copy downloaded file
				std::ofstream destination(getPath() + fileName, std::ios::binary);
				destination.write((char *)bytes.data(), bytes.size());
				printlock.lock();
				std::cout << "Wrote " << fileName << " from " << sources[i] << std::endl;
				printlock.unlock();
				if (fresh) {
					//Add file to file records
					retrievedFiles.insert({ fileName, std::array<int, 2>({ versionNumber, sources[i] }) });
					superClient->call("add", id, fileName, versionNumber);
					//Decrement pending query count
					queryCount.lock();
					pendingQueries--;
					queryCount.unlock();
					ready.notify_one();
				}
				if (!isValid) {
					//Revalidate file locally
					invalidFiles.erase(fileName);
					printlock.lock();
					std::cout << "revalidated " << fileName << std::endl;
					printlock.unlock();
				}
			}
			versionLock.unlock();
			printlock.lock();
			std::cout << "Pending: " << pendingQueries << std::endl;
			printlock.unlock();
			metricLock.lock();
			valid++;
			metricLock.unlock();
			break;
		}
		catch (rpc::rpc_error &e) {
			printlock.lock();
			std::cout << "Error downloading " << fileName << " from " << sources[i] << ": " << e.what() << std::endl;
			printlock.unlock();
			metricLock.lock();
			invalid++;
			metricLock.unlock();
		}
		catch (...) {
			std::cout << "something else broke" << std::endl;
			std::cin.get();
		}
	}
}

std::pair<std::vector<uint8_t>, int> obtain(std::string fileName) {
	//TODO: If pull consistency enabled, check version against owner and error is out-of-date
	//std::cout << "Obtain request for " << fileName << std::endl;
	if (invalidFiles.find(fileName) != invalidFiles.end()) {
		rpc::this_handler().respond_error("File out of date");
		return {};
	}
	//Returns specified file as a vector of bytes
	try {
		std::ifstream file(getPath() + fileName, std::ios::binary);
		file.unsetf(std::ios::skipws);

		std::streampos fileSize;
		file.seekg(0, std::ios::end);
		fileSize = file.tellg();
		file.seekg(0, std::ios::beg);
		std::vector<uint8_t> bytes;
		bytes.reserve(unsigned int(fileSize));
		bytes.insert(bytes.begin(),
			std::istream_iterator<uint8_t>(file),
			std::istream_iterator<uint8_t>());
		//Get version number to return
		versionLock.lock();
		int version = -1;
		auto ownIter = ownFiles.find(fileName);
		if (ownIter != ownFiles.end()) {
			//We are the original owner of the file
			version = ownIter->second;
		}
		else {
			auto retrievedIter = retrievedFiles.find(fileName);
			if (retrievedIter != retrievedFiles.end()) {
				//We're holding the file, but aren't the owner
				version = retrievedIter->second[0];
			}
		}
		versionLock.unlock();
		return std::pair<std::vector<uint8_t>, int>(bytes, version);
	}
	catch (...) {
		rpc::this_handler().respond_error("Error reading file");
		return {};
	}
}

rpc::client* getClient(int clientId) {
	//Return a client for clientId
	auto leafIter = leafClients.find(clientId);
	if (leafIter != leafClients.end()) {
		//std::cout << "leaf" << std::endl;
		return leafIter->second;
	}
	//If the client doesn't exist yet make a new one
	rpc::client *client = new rpc::client("localhost", 8000 + clientId);
	leafClients.insert({ clientId, client });
	//TODO: sometimes the insertion doesnt actually happen and causes total failure o_O
	return client;
}


void start() {
	canStart = true;
	ready.notify_one();
}

void end() {
	canEnd = true;
	ready.notify_one();
}

std::string getPath() {
	return "Leaves/Leaf " + std::to_string(id) + "/";
}