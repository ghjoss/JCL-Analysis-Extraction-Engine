#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <map>
#include <regex>
#include <algorithm>
#include <pqxx/pqxx>
#include <nlohmann/json.hpp>

using json = nlohmann::json;
using namespace std;

// Data Structures to hold parsed info
struct DataAllocation {
    string dd_name;
    int allocation_offset = 1;
    string dsn = "(work_ds)";
    string disp_status = "NEW";
    string disp_normal = "DELETE";
    string disp_abnormal = "DELETE";
    string unit = "";
    string vol_ser = "";
    bool is_dummy = false;
    string instream_ref = "";
    string lrecl = "";
    string blksize = "";
    string recfm = "";
    json dcb_attributes = json::object();
    map<string, string> raw_params;
};

struct JclStep {
    string step_name;
    string program_name;
    string proc_name;
    string parameters;
    string cond_logic;
    vector<DataAllocation> dds;
};

class JclProcessor {
private:
    json config;
    map<string, string> symbol_table;
    map<string, vector<string>> procedure_map;
    vector<string> lib_paths;

    string apply_symbolics(string stmt) {
        // Simple &VAR resolution (Double dot logic included)
        for (auto const& [key, val] : symbol_table) {
            string placeholder = "&" + key;
            size_t pos;
            while ((pos = stmt.find(placeholder + "..")) != string::npos)
                stmt.replace(pos, placeholder.length() + 2, val + ".");
            while ((pos = stmt.find(placeholder + ".")) != string::npos)
                stmt.replace(pos, placeholder.length() + 1, val);
            while ((pos = stmt.find(placeholder)) != string::npos)
                stmt.replace(pos, placeholder.length(), val);
        }
        return stmt;
    }

    string clean_line(string line) {
        if (line.length() > 72) line = line.substr(0, 72);
        line = regex_replace(line, regex("\\s+$"), "");
        if (line.rfind("//*", 0) == 0 || line.rfind("/*", 0) == 0 || line == "//") return "";
        return line;
    }

public:
    JclProcessor(json cfg) : config(cfg) {
        lib_paths.push_back(config["PATH"]);
        if (config.contains("LIB")) {
            for (auto& l : config["LIB"]) lib_paths.push_back(l);
        }
    }

    vector<string> preprocess(string filename) {
        vector<string> statements;
        ifstream file(filename);
        string line, current_stmt = "";
        bool is_continuing = false;

        while (getline(file, line)) {
            line = clean_line(line);
            if (line.empty()) continue;

            // Handle continuation logic
            bool ends_with_comma = (line.back() == ',');
            current_stmt += line;

            if (ends_with_comma) {
                is_continuing = true;
                continue;
            }

            string resolved = apply_symbolics(current_stmt);
            statements.push_back(resolved);
            current_stmt = "";
        }
        return statements;
    }

    vector<JclStep> parse(const vector<string>& stream) {
        vector<JclStep> steps;
        JclStep* current_step = nullptr;
        string last_dd_name = "";

        // Patterns to mimic Lark Grammar logic
        regex exec_regex(R"(//([A-Z0-9#@$]{1,8})?\s+EXEC\s+(PGM=|PROC=)?([A-Z0-9#@$]{1,8}))", regex::icase);
        regex dd_regex(R"(//([A-Z0-9#@$]{1,8})?\s+DD\s+(.*))", regex::icase);

        for (string stmt : stream) {
            smatch match;
            if (regex_search(stmt, match, exec_regex)) {
                JclStep s;
                s.step_name = match[1].str();
                string type = match[2].str();
                if (type.find("PGM") != string::npos) s.program_name = match[3].str();
                else s.proc_name = match[3].str();
                steps.push_back(s);
                current_step = &steps.back();
                last_dd_name = ""; // Reset for new step
            } 
            else if (regex_search(stmt, match, dd_regex) && current_step) {
                DataAllocation dd;
                string label = match[1].str();
                string params = match[2].str();

                if (!label.empty()) {
                    dd.dd_name = label;
                    last_dd_name = label;
                    dd.allocation_offset = 1;
                } else {
                    dd.dd_name = last_dd_name;
                    // Programmatically increment offset later in DB insert or track here
                }

                // Identify Virtual DSNs
                if (params.find("DUMMY") != string::npos) dd.dsn = "(dummy)";
                else if (params.find("*") != string::npos || params.find("DATA") != string::npos) dd.dsn = "(input stream)";
                else if (params.find("SYSOUT") != string::npos) dd.dsn = "(output stream)";
                
                // Extraction of DSN and Disp would happen here via parameter parsing...
                current_step->dds.push_back(dd);
            }
        }
        return steps;
    }
};

class DatabaseManager {
private:
    string conn_str;
public:
    DatabaseManager(json cfg) {
        conn_str = "host=localhost dbname=" + cfg["DATABASE"].get<string>() + 
                   " user=" + cfg["USER"].get<string>() + 
                   " password=" + cfg["PASSWORD"].get<string>();
    }

    void save(string project_name, vector<JclStep>& steps) {
        try {
            pqxx::connection c(conn_str);
            pqxx::work w(c);

            // 1. Get or Create Project ID
            c.prepare("get_proj", "INSERT INTO PROJECTS (project_name) VALUES ($1) ON CONFLICT (project_name) DO NOTHING RETURNING project_id");
            pqxx::result r = w.exec_prepared("get_proj", project_name);
            int project_id;
            if (r.empty()) {
                project_id = w.query_value<int>("SELECT project_id FROM PROJECTS WHERE project_name = " + w.quote(project_name));
            } else {
                project_id = r[0][0].as<int>();
            }

            // 2. Get Max Step ID for manual increment
            int step_counter = w.query_value<int>("SELECT COALESCE(MAX(step_id), 0) FROM STEPS WHERE project_id = " + to_string(project_id));

            for (auto& step : steps) {
                step_counter++;
                w.exec_params("INSERT INTO STEPS (project_id, step_id, step_name, program_name, proc_name, parameters) VALUES ($1, $2, $3, $4, $5, $6)",
                             project_id, step_counter, step.step_name, step.program_name, step.proc_name, step.parameters);

                int ds_counter = 0;
                map<string, int> concat_tracker;

                for (auto& dd : step.dds) {
                    ds_counter++;
                    int offset = ++concat_tracker[dd.dd_name];

                    w.exec_params("INSERT INTO DATA_ALLOCATIONS (project_id, step_id, ds_id, dd_name, allocation_offset, dsn, is_dummy) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                                 project_id, step_counter, ds_counter, dd.dd_name, offset, dd.dsn, dd.is_dummy);
                }
            }
            w.commit();
            cout << "Persistence successful for " << project_name << endl;
        } catch (const exception &e) {
            cerr << "DB Error: " << e.what() << endl;
        }
    }
};

int main() {
    try {
        ifstream cfg_file("config.json");
        json config;
        cfg_file >> config;

        JclProcessor engine(config);
        vector<string> stream = engine.preprocess(config["PATH"].get<string>() + "/" + config["FILE"].get<string>());
        vector<JclStep> steps = engine.parse(stream);

        DatabaseManager db(config);
        db.save(config["PROJECT"], steps);

    } catch (const exception &e) {
        cerr << "Critical Failure: " << e.what() << endl;
        return 1;
    }
    return 0;
}