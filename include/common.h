#ifndef COMMON_H
#define COMMON_H

#include <string>
#include <vector>
#include <variant>
#include <type_traits>
#include <charconv>
#include <cstring>
#include <cstdio>

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

// -----------------------------------------------------------------------------
// Быстрый билдер строк — позволяет записывать данные в один буфер без лишних
// временных строк и реаллокаций
// -----------------------------------------------------------------------------
class FastStringBuilder {
    std::string s;
public:
    FastStringBuilder() = default;
    explicit FastStringBuilder(size_t reserve) { s.reserve(reserve); }

    void append(const char* data, size_t n) { s.append(data, n); }
    void append(std::string_view v) { s.append(v.data(), v.size()); }
    void push_back(char c) { s.push_back(c); }

    // Добавить строковый литерал (завершающийся нулём)
    void append_literal(const char* lit) { s.append(lit); }

    // Добавить число (целое или с плавающей точкой)
    template<typename T>
    void append_number(T value) {
        char buf[48];
        auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), value);
        if (ec == std::errc()) {
            s.append(buf, static_cast<size_t>(ptr - buf));
        } else {
            // fallback на случай, если to_chars не поддерживает тип (например, float в старом STL)
            int len = std::snprintf(buf, sizeof(buf), "%g", static_cast<double>(value));
            if (len > 0) s.append(buf, static_cast<size_t>(len));
        }
    }

    std::string& str() { return s; }
};

// -----------------------------------------------------------------------------
// Вспомогательные функции для экранирования JSON-строк
// -----------------------------------------------------------------------------

// Таблица простых escape-последовательностей
inline const char** get_escape_table() {
    static const char* table[256] = { nullptr };
    static bool inited = false;
    if (!inited) {
        inited = true;
        table[(unsigned char)'"']  = "\\\"";
        table[(unsigned char)'\\'] = "\\\\";
        table[(unsigned char)'\b'] = "\\b";
        table[(unsigned char)'\f'] = "\\f";
        table[(unsigned char)'\n'] = "\\n";
        table[(unsigned char)'\r'] = "\\r";
        table[(unsigned char)'\t'] = "\\t";
    }
    return table;
}

// Быстрая вставка двух шестнадцатеричных символов (для \u00XX)
inline void append_hex2(FastStringBuilder &b, unsigned char c) {
    static const char hex[] = "0123456789ABCDEF";
    char tmp[2] = { hex[c >> 4], hex[c & 0xF] };
    b.append(tmp, 2);
}

// Экранирование строки без кавычек ("")
// Копирует большие блоки "безопасных" байт за один append
inline void append_escaped_unquoted(FastStringBuilder &b, std::string_view s) {
    const char* data = s.data();
    const char* end = data + s.size();
    const char** table = get_escape_table();

    const char* p = data;
    while (p < end) {
        const char* q = p;
        // Найти первый символ, требующий экранирования
        while (q < end) {
            unsigned char uc = static_cast<unsigned char>(*q);
            if (uc < 0x20 || table[uc] != nullptr) break;
            ++q;
        }

        // Добавить блок безопасных байт
        if (q > p)
            b.append(p, static_cast<size_t>(q - p));

        if (q >= end) break;

        // Обработать спецсимвол
        unsigned char c = static_cast<unsigned char>(*q);
        const char* short_escape = table[c];
        if (short_escape) {
            b.append(short_escape, std::strlen(short_escape));
        } else {
            b.append_literal("\\u00");
            append_hex2(b, c);
        }

        p = q + 1;
    }
}

// Экранирование строки с добавлением кавычек ("escaped")
inline void append_quoted_escaped(FastStringBuilder &b, std::string_view s) {
    b.push_back('"');
    append_escaped_unquoted(b, s);
    b.push_back('"');
}

// -----------------------------------------------------------------------------
// Основная структура результата запроса с быстрым to_json()
// -----------------------------------------------------------------------------
struct QueryResult {
    std::vector<std::vector<Value>> rows;  // Строки результата
    std::vector<ColumnInfo> columns;       // Информация о колонках
    size_t count = 0;                      // Общее количество строк

    // Быстрая сериализация результата в JSON
    std::string to_json() const {
        // Предварительное резервирование памяти (для минимизации реаллокаций)
        size_t estimate = (rows.size() * columns.size() * 40) + 128;
        estimate += estimate / 5; // небольшой запас
        FastStringBuilder b(estimate);

        b.append_literal("{\"rows\":[");
        for (size_t i = 0; i < rows.size(); ++i) {
            b.push_back('{');
            const auto& row = rows[i];

            for (size_t j = 0; j < row.size() && j < columns.size(); ++j) {
                b.push_back('"');
                append_escaped_unquoted(b, columns[j].name);
                b.append_literal("\":");

                // Обрабатываем разные типы значения
                std::visit([&](auto&& val) {
                    using T = std::decay_t<decltype(val)>;

                    if constexpr (std::is_same_v<T, std::nullptr_t>) {
                        b.append_literal("null");
                    } else if constexpr (std::is_same_v<T, bool>) {
                        b.append_literal(val ? "true" : "false");
                    } else if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
                        b.append_number(val);
                    } else if constexpr (std::is_floating_point_v<T>) {
                        b.append_number(val);
                    } else if constexpr (std::is_same_v<T, std::string>) {
                        append_quoted_escaped(b, val);
                    } else {
                        static_assert(always_false<T>, "Необработанный тип в Value");
                    }
                }, row[j]);

                if (j + 1 < row.size())
                    b.push_back(',');
            }

            b.push_back('}');
            if (i + 1 < rows.size())
                b.push_back(',');
        }

        b.append_literal("],\"columns\":[");
        for (size_t i = 0; i < columns.size(); ++i) {
            b.append_literal("{\"name\":\"");
            append_escaped_unquoted(b, columns[i].name);
            b.append_literal("\",\"type\":\"");
            append_escaped_unquoted(b, columns[i].type);
            b.append_literal("\"}");
            if (i + 1 < columns.size())
                b.push_back(',');
        }

        b.append_literal("],\"count\":");
        b.append_number(count);
        b.push_back('}');

        return b.str();
    }
};

#endif // COMMON_H
