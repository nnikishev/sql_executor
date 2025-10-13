#ifndef COMMON_H
#define COMMON_H

#include <string>
#include <vector>
#include <sstream>
#include <variant>

// Используем variant для хранения разных типов значений
using Value = std::variant<std::string, std::nullptr_t>;

// Структура для информации о колонке
struct ColumnInfo {
    std::string name;
    std::string type;
};

// Структура для результата запроса
struct QueryResult {
    std::vector<std::vector<Value>> rows;  // Теперь используем Value вместо std::string
    std::vector<ColumnInfo> columns;
    size_t count = 0;
    
    // Метод для преобразования в JSON строку
    std::string to_json() const {
        std::ostringstream json;
        
        json << "{\"rows\":[";
        
        // Обрабатываем строки
        for (size_t i = 0; i < rows.size(); ++i) {
            json << "{";
            for (size_t j = 0; j < rows[i].size() && j < columns.size(); ++j) {
                json << "\"" << escape_json(columns[j].name) << "\":";
                
                // Обрабатываем разные типы значений
                if (std::holds_alternative<std::nullptr_t>(rows[i][j])) {
                    json << "null";
                } else {
                    const std::string& str_val = std::get<std::string>(rows[i][j]);
                    json << "\"" << escape_json(str_val) << "\"";
                }
                
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
        
        // Обрабатываем колонки
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
    // Вспомогательная функция для экранирования JSON строк
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