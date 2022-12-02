#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>
#include <numeric>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"
using namespace std;
template<typename state_machine, typename command>
class raft {

static_assert(is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
static_assert(is_base_of<raft_command, command>(), "command must inherit from raft_command");

friend class thread_pool;

#define RAFT_LOG(fmt, args...) \
    do { \
        auto now = \
        chrono::duration_cast<chrono::milliseconds>(\
            chrono::system_clock::now().time_since_epoch()\
        ).count();\
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
    } while(0);

public:
    raft(
        rpcs* rpc_server,
        vector<rpcc*> rpc_clients,
        int idx, 
        raft_storage<command>* storage,
        state_machine* state    
    );
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node. 
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped(). 
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false. 
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    mutex mtx;                     // A big lock to protect the whole data structure
    ThrPool* thread_pool;
    raft_storage<command>* storage;              // To persist the raft log
    state_machine* state;  // The state machine that applies the raft log, e.g. a kv store

    rpcs* rpc_server;               // RPC server to recieve and handle the RPC requests
    vector<rpcc*> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                     // The index of this node in rpc_clients, start from 0

    atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;

    thread* background_election;
    thread* background_ping;
    thread* background_commit;
    thread* background_apply;

    // Your code here:
    const int timeout_heartbeat = 150;
    const int timeout_commit = 10;
    const int timeout_apply = 10;
    const int timeout_election = 10;
    const int timeout_candidate_election_timeout = 1000;

    int voteFor = -1; 
    long long int last_received_RPC_time = -1; 
    int commitIndex = 0;
    int lastApplied = 0;
    vector<int> nextIndex;
    vector<int> matchIndex;
    vector<log_entry<command>> logs;
    state_machine snapshot;

private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply& reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply& reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply& reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args& arg, const install_snapshot_reply& reply);

    // My code here:
    void try_commit_to_follower(int follower_id);
    void start_election();
    void ping_all();
    void change_commit_index(int useless);
    void ping(int target);
    void set_role(raft_role r);
    void set_current_term(int t);
    void set_vote_for(int v);
    void set_last_received_RPC_time(long long int l);
    void set_commit_index(int c);
    void set_last_applied(int l);
    void set_next_index();
    void set_match_index();
    void set_logs();

private:
    bool is_stopped();
    int num_nodes() {return rpc_clients.size();}

    // background workers    
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:
    int living_servers;
    int dead_servers;
    int voting_me_servers;

};

template<typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs* server, vector<rpcc*> clients, int idx, raft_storage<command> *storage, state_machine *state) :
    storage(storage),
    state(state),   
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    stopped(false),
    role(follower),
    current_term(0),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr)
{
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here: 
    // Do the initialization
}

template<typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;    
    storage->save_local();//My code here
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    term = current_term;
    return role == leader;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Your code here:
    RAFT_LOG("start");

    role = (raft_role)(int)storage->role;
    current_term = storage->current_term;
    voteFor = storage->voteFor;
    last_received_RPC_time = storage->last_received_RPC_time;
    commitIndex = storage->commitIndex;
    nextIndex = storage->nextIndex;
    matchIndex = storage->matchIndex;
    lastApplied = storage->lastApplied;
    logs = storage->logs;
    for (int i = 0; i < lastApplied; i++) state->apply_log(logs[i].cmd);

    this->background_election = new thread(&raft::run_background_election, this);
    this->background_ping = new thread(&raft::run_background_ping, this);
    this->background_commit = new thread(&raft::run_background_commit, this);
    this->background_apply = new thread(&raft::run_background_apply, this);
    RAFT_LOG("start over : log size = %d", (int)logs.size());
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Your code here:
    if (role == leader) {
        unique_lock<mutex> lock(mtx);
        // RAFT_LOG("new_command start : %d", cmd.value);
        log_entry<command> logEntry(current_term, cmd);
        logs.push_back(logEntry);
        set_logs();
        index = logs.size();
        term = current_term;
        // RAFT_LOG("new_command over : index = %d", index);
        return true;
    }
    return false;
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Your code here:
    unique_lock<mutex> lock(mtx);
    storage->save_local();
    return true;
}

/******************************************************************

                         RPC Related

*******************************************************************/
template<typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply& reply) {
    // Your code here:
    unique_lock<mutex> lock(mtx);
    set_last_received_RPC_time(get_current_time());
    reply.term = current_term;
    if (args.term < current_term) reply.voteGranted = false;
    else {
        if (args.term > current_term) {
            set_vote_for(-1);
            set_role(follower);
            set_current_term(args.term);
        }
        if (voteFor == -1 || voteFor == args.candidateId) {
            if (logs.size()) {
                int lastLogIndex = logs.size();
                int lastLogTerm = logs[lastLogIndex - 1].term;
                if ((args.lastLogIndex > lastLogIndex) ||(args.lastLogTerm > lastLogTerm) || ((args.lastLogTerm == lastLogTerm) && (args.lastLogIndex == lastLogIndex))) {
                    set_vote_for(args.candidateId);
                    reply.voteGranted = true;
                    set_role(follower);
                }
                else reply.voteGranted = false;
            }
            else {
                set_vote_for(args.candidateId);
                reply.voteGranted = true;
                set_role(follower);
            }
        }
    }
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply) {
    // Your code here:
    unique_lock<mutex> lock(mtx);
    if (reply.term > current_term) {
        set_current_term(reply.term);
        set_role(follower);
        return;
    }
    living_servers++;
    if (reply.voteGranted) voting_me_servers++;
    if (role == candidate && voting_me_servers > num_nodes() / 2) {
        set_role(leader);
        nextIndex.clear();
        matchIndex.clear();
        for (int i = 0; i < rpc_clients.size(); i++) {
            nextIndex.push_back(logs.size() + 1);
            matchIndex.push_back(0);
        }
        set_next_index();
        set_match_index();
    }
    return;
}


template<typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply& reply) {
    // Your code here:
    unique_lock<mutex> lock(mtx);
    reply.term = current_term;
    if (arg.term < current_term){ 
        reply.success = false;
        return 0;
    }
    set_role(follower);
    set_current_term(arg.term);
    set_last_received_RPC_time(get_current_time());
    {
        if (arg.entries.size())
        set_role(follower);
        if (logs.size() < arg.prevLogIndex) {
            reply.success = false;
            return 0;
        }
        else if ((arg.prevLogIndex - 1) < 0) {}
        else if (logs[arg.prevLogIndex - 1].term != arg.prevLogTerm) { 
            logs.pop_back();
            set_logs();
            reply.success = false; 
            return 0;
        }
        while (logs.size() > arg.prevLogIndex && logs.size() > 0)logs.pop_back();
        for (log_entry<command> entry : arg.entries) logs.push_back(entry);
        set_logs();
        if (arg.leaderCommit > commitIndex)set_commit_index(min(arg.leaderCommit, (int)logs.size()));
        if (arg.entries.size())
        RAFT_LOG("leaderCommit = %d, logs.size() = %d, myCommitIndex = %d", arg.leaderCommit, (int)logs.size(), commitIndex);
        reply.success = true;
        return 0;
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply) {
    // Your code here:
    unique_lock<mutex> lock(mtx);
    if (reply.term > current_term) {
        set_current_term(reply.term);
        set_role(follower);
        return;
    }
    if (!reply.success) {
        nextIndex[target] -= 1;
        set_next_index();
    }
    else {
        if (arg.entries.size() == 0) {}
        else {
            int log_size = logs.size();
            matchIndex[target] = log_size;
            nextIndex[target] = log_size + 1;
            set_next_index(); 
            set_match_index();
            last_received_RPC_time = get_current_time();
        }
    }
    return;
}


template<typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply& reply) {
    // Your code here:
    reply.term = current_term;
    if (args.term < current_term)return 0;
    set_last_received_RPC_time(get_current_time());
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int target, const install_snapshot_args& arg, const install_snapshot_reply& reply) {
    // Your code here:
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
        dead_servers++;
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Check the liveness of the leader.
    // Work for followers and candidates.

    // Hints: You should record the time you received the last RPC.
    //        And in this function, you can compare the current time with it.
    //        For example:
    //        if (current_time - last_received_RPC_time > timeout) start_election();
    //        Actually, the timeout should be different between the follower (e.g. 300-500ms) and the candidate (e.g. 1s).

    srand(time(NULL));
    
    long long int timeout = timeout_candidate_election_timeout;//My code here

    while (true) {
        // RAFT_LOG("id: %d, term: %d, role: %d", my_id, current_term, role);
        if (is_stopped()) {
            RAFT_LOG("return");
            return;
        };
        // Your code here:
        raft_role current_role = role;
        if (current_role != leader && (get_current_time() - last_received_RPC_time) > timeout) start_election();
        this_thread::sleep_for(chrono::milliseconds(timeout_election));
        if (current_role == candidate) {
            timeout = 900 + rand() % 201;
        }
        else if (current_role == follower) {
            timeout = 300 + rand() % 201;
        }
    }    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Send logs/snapshots to the follower.
    // Only work for the leader.

    // Hints: You should check the leader's last log index and the follower's next log index.        
    
    while (true) {
        if (is_stopped()) return;
        // Your code here:

         if (role == leader) {
            int last_index = logs.size() - 1;
            if (last_index == -1) {}
            else {
                unique_lock<mutex> lock(mtx);
                log_entry<command> last_entry = logs[last_index];
                for(int i = 0; i < rpc_clients.size(); i++) {
                    if (i == my_id) continue;
                    thread_pool->addObjJob(this, &raft::try_commit_to_follower, i);
                }
                thread_pool->addObjJob(this, &raft::change_commit_index, 0);

            }
        }

        this_thread::sleep_for(chrono::milliseconds(timeout_commit));
    }    
    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Apply committed logs the state machine
    // Work for all the nodes.

    // Hints: You should check the commit index and the apply index.
    //        Update the apply index and apply the log if commit_index > apply_index

    
    while (true) {
        if (is_stopped()) return;
        // Your code here:

        {
            state->num_append_logs = 0;
            unique_lock<mutex> lock(mtx);
            if (commitIndex > lastApplied) {
                command cmd = logs[lastApplied].cmd;
                if (lastApplied <= logs.size() - 1) {
                    state->apply_log(logs[lastApplied].cmd);
                    set_last_applied(lastApplied + 1);
                    RAFT_LOG("applying log %d", lastApplied);
                }

            }
        }

        this_thread::sleep_for(chrono::milliseconds(timeout_apply));
    }    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Send empty append_entries RPC to the followers.

    // Only work for the leader.
    
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        if (role == leader) {
            for (int i = 0; i < num_nodes(); i++) {
                if (i == my_id) continue;
                thread_pool->addObjJob(this, &raft::ping, i);
            }
        }
        this_thread::sleep_for(chrono::milliseconds(timeout_heartbeat)); // Change the timeout here!
    }    
    return;
}


/******************************************************************

                        Other functions

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::set_role(raft_role r) {
    role = r;
    storage->set_role(r);
}
template<typename state_machine, typename command>
void raft<state_machine, command>::set_current_term(int t) {
    current_term = t;
    storage->set_current_term(t);
}
template<typename state_machine, typename command>
void raft<state_machine, command>::set_vote_for(int v) {
    voteFor = v;
    storage->set_vote_for(v);
}
template<typename state_machine, typename command>
void raft<state_machine, command>::set_last_received_RPC_time(long long int l) {
    last_received_RPC_time = l;
    storage->set_last_received_RPC_time(l);
}
template<typename state_machine, typename command>
void raft<state_machine, command>::set_commit_index(int c) {
    commitIndex = c;
    storage->set_commit_index(c);
    RAFT_LOG("commit index = %d", commitIndex);
}
template<typename state_machine, typename command>
void raft<state_machine, command>::set_last_applied(int l) {
    lastApplied = l;
    storage->set_last_applied(l);
}
template<typename state_machine, typename command>
void raft<state_machine, command>::set_next_index() {
    storage->set_next_index(nextIndex);
}
template<typename state_machine, typename command>
void raft<state_machine, command>::set_match_index() {
    storage->set_match_index(matchIndex);
}
template<typename state_machine, typename command>
void raft<state_machine, command>::set_logs() {
    storage->set_logs(logs);
}


template<typename state_machine, typename command>
void raft<state_machine, command>::start_election() {
    unique_lock<mutex> lock(mtx);
    living_servers = 1;
    dead_servers = 0;
    voting_me_servers = 1;
    set_last_received_RPC_time(get_current_time());
    set_current_term(current_term + 1);
    set_vote_for(my_id);
    set_role(candidate);
    for (int i = 0; i < rpc_clients.size(); i++) {
        if (i == my_id) continue;
        if (logs.size() == 0) {
            thread_pool->addObjJob(this, &raft::send_request_vote, i, request_vote_args(current_term, my_id, 0, 0));
        }
        else {
            log_entry<command> logEntry = logs[logs.size() - 1];
            thread_pool->addObjJob(this, &raft::send_request_vote, i, request_vote_args(current_term, my_id, (int)logs.size(), logEntry.term));
        }
        if (role != candidate) return;
    }
}


template<typename state_machine, typename command>
void raft<state_machine, command>::ping(int target) {
    int log_size = logs.size();
    append_entries_args<command> args;
    vector<log_entry<command>> entries;
    if (log_size == 0) {
        args = append_entries_args<command>(current_term, my_id, log_size, 0, entries, commitIndex);
    }
    else {
        log_entry<command> logEntry = logs[log_size - 1];
        args = append_entries_args<command>(current_term, my_id, log_size, logEntry.term, entries, commitIndex);
    }

    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, args, reply) == 0) {
        handle_append_entries_reply(target, args, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::try_commit_to_follower(int target) {
    int prevLogIndex, prevLogTerm; int log_size = logs.size();
    if (log_size == 0) {
        prevLogIndex = 0; prevLogTerm = 0;
        return;
    }
    int followerNextIndex = nextIndex[target];
    prevLogIndex = followerNextIndex - 1;
    if ((prevLogIndex - 1) < 0) {
        prevLogTerm = 0;
    }
    else {
        prevLogTerm = logs[prevLogIndex - 1].term;
    }
    if (log_size >= followerNextIndex) {
        vector<log_entry<command>> entries;
        for (int i = followerNextIndex; i <= log_size; i++) {
            entries.push_back(logs[i - 1]);
        }
        append_entries_args<command> args(current_term, my_id, prevLogIndex, prevLogTerm, entries, commitIndex);
        append_entries_reply reply;
        if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, args, reply) == 0) {
            handle_append_entries_reply(target, args, reply);
        } else {
            // RPC fails
        }
    }
}


template<typename state_machine, typename command>
void raft<state_machine, command>::change_commit_index(int useless) {
    unique_lock<mutex> lock(mtx);
    for (int N = commitIndex + 1; N <= logs.size(); N++) {
        int majority_count = 0;
        for (int i = 0; i < num_nodes(); i++) {
            if (i == my_id) {
                majority_count++;
                continue;
            }
            if ((matchIndex[i] >= N) && (logs[N - 1].term == current_term)) {
                majority_count++;

            }
        }
        if (majority_count > num_nodes() / 2) {
            set_commit_index(N);
        }
        else {
        }
    }
}

#endif // raft_h