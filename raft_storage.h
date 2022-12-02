#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <iostream>
#include <fstream>
using namespace std;
template <typename command>
class raft_storage {
public:
    raft_storage(const string &file_dir);
    ~raft_storage();

    string file_path;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role = follower;
    int current_term = 0;
    // Lab3: Your code here
    void save_local();
    int voteFor = -1; 
    long long int last_received_RPC_time = -1; 
    int commitIndex = 0;
    int lastApplied = 0;

    vector<int> nextIndex;
    vector<int> matchIndex;

    vector<log_entry<command>> logs;

    void set_role(int r) {
        unique_lock<mutex> lock(mtx);
        role = (raft_role)r;
    }
    void set_current_term(int c) {
        unique_lock<mutex> lock(mtx);
        current_term = c;
    }
    void set_vote_for(int v) {
        unique_lock<mutex> lock(mtx);
        voteFor = v;
    }
    void set_last_received_RPC_time(long long int l) {
        unique_lock<mutex> lock(mtx);
        last_received_RPC_time = l;
    }
    void set_commit_index(int c) {
        unique_lock<mutex> lock(mtx);
        commitIndex = c;
    }
    void set_last_applied(int l) {
        unique_lock<mutex> lock(mtx);
        lastApplied = l;
    }
    void set_next_index(vector<int> n) {
        unique_lock<mutex> lock(mtx);
        nextIndex = n;
    }
    void set_match_index(vector<int> m) {
        unique_lock<mutex> lock(mtx);
        matchIndex = m;
    }
    void set_logs(vector<log_entry<command>> l) {
        unique_lock<mutex> lock(mtx);
        logs = l;
    }

private:
    mutex mtx;
    // Lab3: Your code here
};

template <typename command>
raft_storage<command>::raft_storage(const string &dir) {
    // Lab3: Your code here
    unique_lock<mutex> lock(mtx);
    file_path = dir + string("/log.txt");
    ifstream in(file_path);
    if (!in.is_open()) return;
    int role_int, nextIndexVecSize, matchIndexVecSize;
    in >> role_int >> current_term >> voteFor >> last_received_RPC_time >> commitIndex >> lastApplied;
    in >> nextIndexVecSize >> matchIndexVecSize;
    role = (raft_role)role_int;
    for(int i = 0; i < nextIndexVecSize; i++) {
        int index; 
        in >> index;
        nextIndex.push_back(index);
    }
    for(int i = 0; i < matchIndexVecSize; i++) {
        int index; 
        in >> index;
        matchIndex.push_back(index);
    }
    int log_size; 
    in >> log_size;
    for (int i = 0; i < log_size; i++) {
        int line_size, received_term;
        in >> line_size >> received_term;
        command cmd;
        char* cmd_string = (char*)malloc(sizeof(char) * line_size);

        for (int j = 0; j < line_size; j++) {
            int tmp; 
            in >> tmp;
            cmd_string[j] = (char)tmp;
        }
        cmd.deserialize(cmd_string, line_size);
        logs.push_back(log_entry<command>(received_term, cmd));

    }
    in.close();

}

template <typename command>
raft_storage<command>::~raft_storage() {
    // Lab3: Your code here
    save_local();
}


template<typename command>
void raft_storage<command>::save_local() {
    // Your code here
    unique_lock<mutex> lock(mtx);
    ofstream out(file_path.c_str());
    if (!out.is_open())return;
    out << (int)role << endl << current_term << endl << voteFor << endl 
    << last_received_RPC_time << endl << commitIndex << endl << lastApplied << endl
    << nextIndex.size() << endl << matchIndex.size() << endl;
    for (int i = 0; i < nextIndex.size(); i++)out << nextIndex[i] << endl;
    out << endl;
    for (int i = 0; i < matchIndex.size(); i++)out << matchIndex[i] << endl;
    out << logs.size() << endl;
    for (int i = 0; i < logs.size(); i++) {
        log_entry<command> tmp_log_entry = logs[i];
        int cmd_size = tmp_log_entry.cmd.size();
        char *cmd_buf = (char*)malloc(sizeof(char) * cmd_size);
        tmp_log_entry.cmd.serialize(cmd_buf, cmd_size);
        out << cmd_size << " " << tmp_log_entry.term << endl;
        for (int j = 0; j < cmd_size; j++) {
            out << (int)(cmd_buf[j]) << " ";
        }
        out << endl;
        free(cmd_buf);
    }
    out.close();
}


//     // Lab3: Your code here


// template <typename command>
// raft_storage<command>::raft_storage(const string &dir) {
//     // Lab3: Your code here

//     for (int i = 0; i < log_size; i++) {
//         int line_size, received_term;
//         in >> line_size >> received_term;
//         command cmd;
//         char* cmd_string = (char*)malloc(sizeof(char) * line_size);

//         for (int j = 0; j < line_size; j++) {
//             int tmp; 
//             in >> tmp;
//             cmd_string[j] = (char)tmp;
//         }
//         cmd.deserialize(cmd_string, line_size);
//         logs.push_back(log_entry<command>(received_term, cmd));
//     }
//     in.close();
// }



// template<typename command>
// void raft_storage<command>::save_local() {
//     // Your code here
//     unique_lock<mutex> lock(mtx);
//     ofstream out(file_path.c_str());
//     if (!out.is_open()) return;
    
//     out << (int)role << endl << current_term << endl << voteFor << endl 
//     << last_received_RPC_time << endl << commitIndex << endl << lastApplied << endl
//     << nextIndex.size() << endl << matchIndex.size() << endl;
//     for (int i = 0; i < (int)nextIndex.size(); i++) out << nextIndex[i] << endl;
//     out << endl;
//     for (int i = 0; i < (int)matchIndex.size(); i++)out << matchIndex[i] << endl;
//     out << logs.size() << endl;
//     for (int i = 0; i < (int)logs.size(); i++) {
//         log_entry<command> tmp_log_entry = logs[i];
//         int cmd_size = tmp_log_entry.cmd.size();
//         char *cmd_buf = (char*)malloc(sizeof(char) * cmd_size);
//         tmp_log_entry.cmd.serialize(cmd_buf, cmd_size);
//         out << cmd_size << " " << tmp_log_entry.term << endl;
//         for (int j = 0; j < cmd_size; j++) {
//             out << (int)(cmd_buf[j]) << " ";
//         }
//         out << endl;
//         free(cmd_buf);
//     }
//     out.close();
// }
#endif // raft_storage_h