#include "chfs_state_machine.h"

chfs_command_raft::chfs_command_raft() {
    // Lab3: Your code here
    cmd_tp = CMD_NONE;
    buf = "";
    res->start = std::chrono::system_clock::now();
    res->buf = "";
}

chfs_command_raft::chfs_command_raft(const chfs_command_raft &cmd) :
    cmd_tp(cmd.cmd_tp), type(cmd.type),  id(cmd.id), buf(cmd.buf), res(cmd.res) {
    // Lab3: Your code here
}
chfs_command_raft::~chfs_command_raft() {
    // Lab3: Your code here
}

int chfs_command_raft::size() const{ 
    // Lab3: Your code here
    return 3 + 9 + buf.size();
}

void chfs_command_raft::serialize(char *buf_out, int size) const {
    // Lab3: Your code here
    buf_out[0] = (int)cmd_tp;
    buf_out[1] = (int)type;
    buf_out[2] = (int)id;
    std::string buf_size_string = std::to_string(buf.size());
    while (buf_size_string.size() < 9) {
        buf_size_string = "0" + buf_size_string;
    }
    memcpy(buf_out + 3, buf_size_string.c_str(), 9);
    //start from buf_out + 12
    memcpy(buf_out + 12, buf.c_str(), buf.size());
    return;
}

void chfs_command_raft::deserialize(const char *buf_in, int size) {
    // Lab3: Your code here
    cmd_tp = (command_type)buf_in[0];
    type = (uint32_t)buf_in[1];
    id = (extent_protocol::extentid_t)buf_in[2];
    char *buf_size_buf = (char*)malloc(sizeof(char) * 9);
    memcpy(buf_size_buf, buf_in + 3, 9);
    buf_size_buf[9] = 0;
    std::cout << "buf_size_buf = " << buf_size_buf << std::endl;
    int buf_size = atoi(buf_size_buf);
    std::cout << "bufsize = " << buf_size << std::endl;
    char *buf_buf = (char *)malloc(sizeof(char) * buf_size);
    memcpy(buf_buf, buf_in + 12, buf_size);
    buf_buf[buf_size] = 0;
    buf = std::string(buf_buf);
    return;
}

marshall &operator<<(marshall &m, const chfs_command_raft &cmd) {
    // Lab3: Your code here
    m << cmd.cmd_tp << cmd.type << cmd.id << cmd.buf;
    return m;
}

unmarshall &operator>>(unmarshall &u, chfs_command_raft &cmd) {
    // Lab3: Your code here
    int a, b, c;
    u >> a >> b >> c >> cmd.buf;
    cmd.cmd_tp = (chfs_command_raft::command_type)a;
    cmd.type = (uint32_t)b;
    cmd.id = (extent_protocol::extentid_t)c;
    return u;
}

void chfs_state_machine::apply_log(raft_command &cmd) {
    chfs_command_raft &chfs_cmd = dynamic_cast<chfs_command_raft &>(cmd);
    // Lab3: Your code here

    chfs_cmd.res->cv.notify_all();
    return;
}


