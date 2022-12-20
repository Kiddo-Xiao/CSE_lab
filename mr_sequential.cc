//
// A simple sequential MapReduce for WordCount
//

#include <string>
#include <sstream>
#include <fstream>
#include <iostream>
#include <vector>
#include <algorithm>
#include <map>

using namespace std;

#define is_valid(c) (c >= 'a' && c <= 'z') || (c >='A' && c <= 'Z')


typedef struct {
    string key;
    string val;
}
KeyVal;

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
vector <KeyVal> Map(const string &filename, const string &content) {
    // Your code goes here
    // Hints: split contents into an array of words.
    string str;
    vector <KeyVal> key_values;
    map<std::string, int> get_map;
    
    for (char i:content) {
        if (is_valid(i)) str += i;
        else if (str.size() > 0) {
            get_map[str] += 1;
            str = "";
        }
    }
    for (auto i = get_map.begin(); i != get_map.end(); i++) {
        KeyVal kv; 
        kv.key = i->first; 
        kv.val = to_string(i->second);
        key_values.push_back(kv);
    }
    return key_values;
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
string Reduce(const string &key, const vector <string> &values) {
    // Your code goes here
    // Hints: return the number of occurrences of the word.
    unsigned long long sum = 0;
    for (const string &v:values) sum += atoll(v.c_str());
    return to_string(sum);
}

int main(int argc, char **argv) {
    if (argc < 2) {
        cout << "Usage: mrsequential inputfiles...\n";
        exit(1);
    }

    vector <string> filename;
    vector <KeyVal> intermediate;

    //
    // read each input file,
    // pass it to Map,
    // accumulate the intermediate Map output.
    //

    for (int i = 1; i < argc; ++i) {

        string filename = argv[i];
        string content;

        // Read the whole file into the buffer.
        getline(ifstream(filename), content, '\0');

        vector <KeyVal> KVA = Map(filename, content);

        intermediate.insert(intermediate.end(), KVA.begin(), KVA.end());

    }

    //
    // a big difference from real MapReduce is that all the
    // intermediate data is in one place, intermediate[],
    // rather than being partitioned into NxM buckets.
    //

    sort(intermediate.begin(), intermediate.end(),
         [](KeyVal const &a, KeyVal const &b) {
             return a.key < b.key;
         });

    //
    // call Reduce on each distinct key in intermediate[],
    // and print the result to mr-out-0.
    //

    for (unsigned int i = 0; i < intermediate.size();) {
        unsigned int j = i + 1;
        for (; j < intermediate.size() && intermediate[j].key == intermediate[i].key;)
            j++;

        vector <string> values;
        for (unsigned int k = i; k < j; k++) {
            values.push_back(intermediate[k].val);
        }

        string output = Reduce(intermediate[i].key, values);
        printf("%s %s\n", intermediate[i].key.data(), output.data());

        i = j;
    }
    return 0;
}
