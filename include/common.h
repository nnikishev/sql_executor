#ifndef COMMON_H
#define COMMON_H

#include <string>
#include <vector>
#include <variant>
#include <type_traits>

// Компиляторный якорь для static_assert
template<class> inline constexpr bool always_false = false;

// Используем variant для хранения разных типов значений
using Value = std::variant<
    std::nullptr_t,
    bool,
    int64_t,
    double,
    std::string
>;

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
        std::string json;
        json.reserve((rows.size() * columns.size() * 40) + 128);
        json += "{\"rows\":[";

        for (size_t i = 0; i < rows.size(); ++i) {
            json += "{";

            for (size_t j = 0; j < rows[i].size() && j < columns.size(); ++j) {
                json += "\"";
                json += escape_json(columns[j].name);
                json += "\":";

                std::visit([&](auto&& val) {
                    using T = std::decay_t<decltype(val)>;

                    // Обрабатываем разные типы значений
                    if constexpr (std::is_same_v<T, std::nullptr_t>) {
                        json += "null";
                    } else if constexpr (std::is_same_v<T, bool>) {
                        json += (val ? "true" : "false");
                    } else if constexpr (std::is_arithmetic_v<T>) {
                        json += std::to_string(val);
                    } else if constexpr (std::is_same_v<T, std::string>) {
                        json += "\"";
                        json += escape_json(val);
                        json += "\"";
                    } else {
                        static_assert(always_false<T>, "Необработанный тип в Value");
                    }
                }, rows[i][j]);

                if (j + 1 < rows[i].size()) json += ",";
            }

            json += "}";
            if (i + 1 < rows.size()) json += ",";
        }

        json += "],\"columns\":[";

        // Обрабатываем колонки
        for (size_t i = 0; i < columns.size(); ++i) {
            json += "{\"name\":\"";
            json += escape_json(columns[i].name);
            json += "\",\"type\":\"";
            json += escape_json(columns[i].type);
            json += "\"}";
            if (i + 1 < columns.size()) json += ",";
        }

        json += "],\"count\":";
        json += std::to_string(count);
        json += "}";

        return json;
    }

private:
    // Вспомогательная функция для экранирования JSON строк
    static std::string escape_json(const std::string& str) {
        std::string escaped;
        escaped.reserve(str.size());
        for (char c : str) {
            switch (c) {
                case '"': escaped += "\\\""; break;
                case '\\': escaped += "\\\\"; break;
                case '\b': escaped += "\\b"; break;
                case '\f': escaped += "\\f"; break;
                case '\n': escaped += "\\n"; break;
                case '\r': escaped += "\\r"; break;
                case '\t': escaped += "\\t"; break;
                default: escaped += c; break;
            }
        }
        return escaped;
    }
};

#endif // COMMON_H
