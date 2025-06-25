Purpose: Monitors and analyzes MySQL query performance to identify and optimize slow queries.

Features: Automated slow query logging to a table, detailed data collection, intelligent processing via stored procedures, scheduled execution, and performance bottleneck identification.

Setup: Involves configuring MySQL variables, creating a query_log table, deploying a stored procedure, and setting up an event scheduler. Note: A database must be created, and all tables referenced must reside within the same database for the project to function correctly.

Usage: Provides SQL examples for viewing slow queries, finding inefficient SELECT statements, identifying frequent queries, and analyzing keyword usage patterns. Some pre-defined queries for testing purposes are provided in a separate test_file.