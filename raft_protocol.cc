#include "raft_protocol.h"
using namespace std;

long long int get_current_time() {
    chrono::time_point< chrono::system_clock > now = chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    auto delt_time = chrono::duration_cast< chrono::milliseconds >( duration );
    long long int delt_time_count = delt_time.count();
    return delt_time_count;
}

marshall &operator<<(marshall &m, const request_vote_args &args) {
    // Lab3: Your code here
    m << args.term << args.candidateId << args.lastLogIndex << args.lastLogTerm;
    return m;
}
unmarshall &operator>>(unmarshall &u, request_vote_args &args) {
    // Lab3: Your code here
    u >> args.term >> args.candidateId >> args.lastLogIndex >> args.lastLogTerm;
    return u;
}

marshall &operator<<(marshall &m, const request_vote_reply &reply) {
    // Lab3: Your code here
    m << reply.term << reply.voteGranted;
    return m;
}

unmarshall &operator>>(unmarshall &u, request_vote_reply &reply) {
    // Lab3: Your code here
    u >> reply.term >> reply.voteGranted;
    return u;
}

marshall &operator<<(marshall &m, const append_entries_reply &args) {
    // Lab3: Your code here
    m << args.term << args.success;
    return m;
}

unmarshall &operator>>(unmarshall &m, append_entries_reply &args) {
    // Lab3: Your code here
    m >> args.term >> args.success;
    return m;
}

marshall &operator<<(marshall &m, const install_snapshot_args &args) {
    // Lab3: Your code here
    m << args.term << args.leaderId << args.lastIncludeIndex << args.lastIncludeTerm
        << args.offset << args.data << args.done;
    return m;
}

unmarshall &operator>>(unmarshall &u, install_snapshot_args &args) {
    // Lab3: Your code here
    u >> args.term >> args.leaderId >> args.lastIncludeIndex >> args.lastIncludeTerm
        >> args.offset >> args.data >> args.done;
    return u;
}

marshall &operator<<(marshall &m, const install_snapshot_reply &reply) {
    // Lab3: Your code here
    m << reply.term;
    return m;
}

unmarshall &operator>>(unmarshall &u, install_snapshot_reply &reply) {
    // Lab3: Your code here
    u >> reply.term;
    return u;
}