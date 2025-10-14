#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include "common.h"
#include <variant>

TEST_CASE("QueryResult JSON conversion", "[QueryResult]") {
    QueryResult result;

    result.columns.push_back({"id", "int"});
    result.columns.push_back({"name", "string"});
    result.columns.push_back({"active", "bool"});

    result.rows.push_back({int64_t(1), std::string("Alice"), true});
    result.rows.push_back({nullptr, std::string("Bob"), false});
    result.count = 2;

    std::string json = result.to_json();

    REQUIRE(json.find("\"id\":1") != std::string::npos);
    REQUIRE(json.find("\"name\":\"Alice\"") != std::string::npos);
    REQUIRE(json.find("\"active\":true") != std::string::npos);
    REQUIRE(json.find("null") != std::string::npos);
}

TEST_CASE("Value variant type checking", "[Value]") {
    Value v1 = int64_t(42);
    Value v2 = std::string("test");
    Value v3 = nullptr;
    Value v4 = true;

    REQUIRE(std::holds_alternative<int64_t>(v1));
    REQUIRE(std::holds_alternative<std::string>(v2));
    REQUIRE(std::holds_alternative<std::nullptr_t>(v3));
    REQUIRE(std::holds_alternative<bool>(v4));
}
