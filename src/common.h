#ifndef COMMON_H
#define COMMON_H

#include <string>
#include <vector>
#include <sstream>

struct ColumnInfo {
    std::string name;
    std::string type;
};

struct QueryResult {
    std::vector<std::vector<std::string>> rows;
    std::vector<ColumnInfo> columns;
    size_t count = 0;  
    
    std::string to_json() const {
        std::ostringstream json;
        
        json << "{\"rows\":[";
        
        for (size_t i = 0; i < rows.size(); ++i) {
            json << "{";
            for (size_t j = 0; j < rows[i].size() && j < columns.size(); ++j) {
                json << "\"" << escape_json(columns[j].name) << "\":\"" << escape_json(rows[i][j]) << "\"";
                if (j < rows[i].size() - 1) {
                    json << ",";
                }
            }
            json << "}";
            if (i < rows.size() - 1) {
                json << ",";
            }
        }
        
        json << "],\"columns\":[";
        
        for (size_t i = 0; i < columns.size(); ++i) {
            json << "{\"name\":\"" << escape_json(columns[i].name) 
                 << "\",\"type\":\"" << escape_json(columns[i].type) << "\"}";
            if (i < columns.size() - 1) {
                json << ",";
            }
        }
        
        json << "],\"count\":" << count << "}";
        
        return json.str();
    }

private:
    static std::string escape_json(const std::string& str) {
        std::ostringstream oss;
        for (char c : str) {
            switch (c) {
                case '"': oss << "\\\""; break;
                case '\\': oss << "\\\\"; break;
                case '\b': oss << "\\b"; break;
                case '\f': oss << "\\f"; break;
                case '\n': oss << "\\n"; break;
                case '\r': oss << "\\r"; break;
                case '\t': oss << "\\t"; break;
                default: oss << c; break;
            }
        }
        return oss.str();
    }
};

#endif