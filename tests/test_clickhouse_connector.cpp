#include "clickhouse_connector.h"
#include "common.h"
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>

TEST_CASE("ClickHouse connection", "[ClickHouseConnector]") {
    ClickHouseConnector conn;

    REQUIRE_FALSE(conn.is_connected());

    // Здесь лучше использовать тестовую ClickHouse или мок
    bool ok = conn.connect("127.0.0.1", 19000, "default", "default", "");
    if (ok) {
        REQUIRE(conn.is_connected());
        conn.disconnect();
        REQUIRE_FALSE(conn.is_connected());
    }
}

TEST_CASE("ClickHouse execute basic query", "[ClickHouseConnector]") {
    ClickHouseConnector conn;
    if (!conn.connect("127.0.0.1", 19000, "default", "default", "")) {
        WARN("Cannot connect to ClickHouse, skipping test");
        return;
    }

    auto result = conn.execute("SELECT 1 AS test");
    REQUIRE(result.count >= 1);
    REQUIRE(result.columns.size() == 1);
    REQUIRE(result.columns[0].name == "test");
    conn.disconnect();
}
