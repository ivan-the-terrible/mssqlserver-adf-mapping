--https://database.guide/4-ways-to-list-all-views-in-a-sql-server-database/

-- Tables
SELECT SCHEMA_NAME(schema_id) + '.' + name FROM sys.tables ORDER BY schema_id, name

-- Views
SELECT SCHEMA_NAME(schema_id) + '.' + name, definition FROM sys.views v
INNER JOIN sys.sql_modules m
ON v.object_id = m.object_id
WHERE v.schema_id != 4 -- Exclude sys schema
ORDER BY schema_id, name

-- Stored Procedures
SELECT SCHEMA_NAME(schema_id) + '.' + name, definition FROM sys.procedures p
INNER JOIN sys.sql_modules m
ON p.object_id = m.object_id
WHERE p.schema_id != 4 -- Exclude sys schema
ORDER BY schema_id, name