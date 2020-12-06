CREATE TABLE default.all_types (
    int8 Int8,
    int64 Int64,
    uint8 UInt8,
    uint64 UInt64,
    float64 Float64,
--    decimal64 Decimal64,
    string String,
    fixed_string FixedString(3),
    uuid UUID,
    date Date,
    date_time DateTime,
    enum Enum('hello' = 1, 'world' = 2)
)ENGINE = MergeTree()
ORDER BY int8;

INSERT INTO default.all_types VALUES
(-128, -9223372036854775808, 255, 18446744073709551615, 0.2222222, 'abc', 'cba', '61f0c404-5cb3-11e7-907b-a6006ad3dba0', '2020-11-02', '2020-11-07 10:34:45', 'hello');

INSERT INTO default.all_types VALUES
(-127, -9223372036854775807, 254, 18446744073709551614, 0.2222221, 'abc', 'cba', '61f0c404-5cb3-11e7-907b-a6006ad3dba1', '2020-11-03', '2020-11-05 10:34:49','world');
