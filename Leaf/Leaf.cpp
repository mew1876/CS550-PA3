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
#include <set>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <thread>

void queryHit(int sender, std::array<int, 2> messageId, int TTL, std::string fileName, std::vector<int> leaves);
void invalidate(std::array<int, 2> messageId, int masterId, int TTL, std::string fileName, int versionNumber);
void downloadFile(std::vector<int> sources, std::string fileName);
void obtain(int sender, std::string fileName);
void receive(std::string fileName, std::vector<uint8_t> bytes, int version, int masterId);
bool upToDate(std::string fileName, int version);
rpc::client* getClient(int clientId);
void start();
void end();
std::string getPath();

int id, superId, nSupers, startTTL;
bool isExtra;
bool push = false, pull1 = false, pull2 = false;
int nextMessageId = 0;
int pendingQueries = 0;
int valid = 0, invalid = 0;
std::unordered_map<std::string, std::array<int, 2>> retrievedFiles;
std::unordered_set<std::string> invalidFiles;
std::unordered_map<std::string, int> ownFiles;
std::unordered_map<int, rpc::client*> leafClients;
std::vector<std::thread> downloadThreads;
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
	if (argc < 6) {
		return -1;
	}
	id = std::stoi(argv[0]);
	superId = std::stoi(argv[1]);
	nSupers = std::stoi(argv[2]);
	startTTL = std::stoi(argv[3]);
	isExtra = std::stoi(argv[4]);
	int mode = std::stoi(argv[5]);
	if (mode == 1 || mode == 3) {
		push = true;
	}
	if (mode == 2 || mode == 3) {
		pull1 = true;
	}
	if (mode == 4) {
		pull2 = true;
	}
	std::cout << "Im a leaf with ID " << id << " and my super's ID is " << superId << std::endl;
	//Start server for start, obtain, and end signals
	rpc::server server(8000 + id);
	server.bind("start", &start);
	server.bind("queryHit", &queryHit);
	server.bind("obtain", &obtain);
	server.bind("receive", &receive);
	server.bind("invalidate", &invalidate);
	server.bind("upToDate", &upToDate);
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
	for (argIndex = 6; argIndex < argc; argIndex++) {
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
		file->second++;
		if (push) {
			//send push message to super
			printlock.lock();
			std::cout << "Pushing invalidate for version " << file->second << " of " << file->first << std::endl;
			printlock.unlock();
			std::array<int, 2> messageId = { id, nextMessageId++ };
			try {
				superClient->async_call("invalidate", messageId, id, startTTL, file->first, file->second);
			}
			catch (...) {
				std::cout << "Error pushing invalidate" << std::endl;
			}
		}
		std::cout << "Updated " << file->first << " to version " << file->second << std::endl;
		std::this_thread::sleep_for(std::chrono::milliseconds(std::rand() % 1000));
	}
	std::cout << "wait for kill" << std::endl;
	//Report metrics
	metricLock.lock();
	if (!isExtra) {
		valid = 0;
	}
	sysClient.call("metrics", valid, invalid);
	metricLock.unlock();
	//Wait for kill signal
	ready.wait(unique, [] { return canEnd && false; });
	//Wait for own server to end gracefully
	printlock.lock();
	std::cout << "collecting threads" << std::endl;
	printlock.unlock();
	for (std::thread& thread : downloadThreads) {
		thread.join();
	}
	printlock.lock();
	std::cout << "got threads" << std::endl;
	printlock.unlock();
	std::this_thread::sleep_for(std::chrono::milliseconds(5000));
	delete superClient;
	rpc::client selfClient("localhost", 8000 + id);
	selfClient.call("stop_server");
	for (auto client : leafClients) {
		delete client.second;
	}
	std::cout << "dead" << std::endl;
}

void queryHit(int sender, std::array<int, 2> messageId, int TTL, std::string fileName, std::vector<int> leaves) {
	printlock.lock();
	std::cout << "queryhit for " << fileName << " from " << sender << std::endl;
	printlock.unlock();
	//Try to copy file from each source in another thread until success
	std::thread dlThread = std::thread(downloadFile, leaves, fileName);
	downloadThreads.push_back(std::move(dlThread));
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
		std::thread dlThread = std::thread(downloadFile, sourceIds, fileName);
		downloadThreads.push_back(std::move(dlThread));
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
			getClient(sources[i])->async_call("obtain", id, fileName);
		}
		catch (rpc::rpc_error &e) {
			printlock.lock();
			std::cout << "Error downloading " << fileName << " from " << sources[i] << ": " << e.what() << std::endl;
			printlock.unlock();
		}
		catch (...) {
			std::cout << "something else broke" << std::endl;
			std::cin.get();
		}
	}
}

void obtain(int sender, std::string fileName) {
	//TODO: If pull consistency enabled, check version against owner and error is out-of-date
	printlock.lock();
	std::cout << "Obtain request for " << fileName << std::endl;
	printlock.unlock();
	if (invalidFiles.find(fileName) != invalidFiles.end()) {
		metricLock.lock();
		invalid++;
		metricLock.unlock();
		rpc::this_handler().respond_error("File out of date");
		return;
	}
	//Get version number to return
	versionLock.lock();
	int version = -1;
	int master = -1;
	auto ownIter = ownFiles.find(fileName);
	if (ownIter != ownFiles.end()) {
		//We are the original owner of the file
		version = ownIter->second;
		master = id;
		versionLock.unlock();
	}
	else {
		auto retrievedIter = retrievedFiles.find(fileName);
		if (retrievedIter != retrievedFiles.end()) {
			versionLock.unlock();
			//We're holding the file, but aren't the owner
			printlock.lock();
			std::cout << "Checking version of " << fileName << std::endl;
			printlock.unlock();
			if (!pull1 || getClient(retrievedIter->second[1])->call("upToDate", fileName, retrievedIter->second[0]).as<bool>()) {
				printlock.lock();
				std::cout << "File up to date" << std::endl;
				printlock.unlock();
				version = retrievedIter->second[0];
				master = retrievedIter->second[1];
			}
			else {
				printlock.lock();
				std::cout << "File out of date" << std::endl;
				printlock.unlock();
				//Mark file as invalid
				invalidFiles.insert(fileName);
				//Download file from master
				std::vector<int> sourceIds = { retrievedIter->second[1] };
				std::thread dlThread = std::thread(downloadFile, sourceIds, fileName);
				downloadThreads.push_back(std::move(dlThread));
				//return error
				metricLock.lock();
				invalid++;
				metricLock.unlock();
				rpc::this_handler().respond_error("File out of date");
				return;
			}
		}
		else {
			versionLock.unlock();
		}
	}
	std::cout << "Getting bytes to return for " << fileName << std::endl;
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
		//return std::pair<std::vector<uint8_t>, std::array<int, 2>>(bytes, std::array<int, 2>({ version, master }));
		getClient(sender)->async_call("receive", fileName, bytes, version, master);
	}
	catch (...) {
		metricLock.lock();
		invalid++;
		metricLock.unlock();
		rpc::this_handler().respond_error("Error reading file");
		return;
	}
}

void receive(std::string fileName, std::vector<uint8_t> bytes, int version, int masterId) {
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
	std::cout << "Downloaded " << fileName << std::endl;
	printlock.unlock();
	if (fresh || !isValid) {
		//Copy downloaded file
		std::ofstream destination(getPath() + fileName, std::ios::binary);
		destination.write((char *)bytes.data(), bytes.size());
		if (fresh) {
			//Add file to file records
			retrievedFiles.insert({ fileName, std::array<int, 2>({ version, masterId }) });
			superClient->call("add", id, fileName, version);
			//Decrement pending query count
			queryCount.lock();
			pendingQueries--;
			queryCount.unlock();
			//Increment valid counter
			metricLock.lock();
			valid++;
			metricLock.unlock();
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
}

bool upToDate(std::string fileName, int version) {
	printlock.lock();
	std::cout << "Someone is asking about version " << version << " of " << fileName << std::endl;
	printlock.unlock();
	auto ownIter = ownFiles.find(fileName);
	if (ownIter != ownFiles.end()) {
		return ownIter->second <= version;
	}
	return true;
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