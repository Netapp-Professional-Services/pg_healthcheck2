"""
Query Optimization Opportunities Check

Provides actionable optimization recommendations by:
- Aggregating resource consumption by user/service for team attribution
- Correlating high-CPU queries with sequential scan data
- Displaying full query text in collapsible sections
- Suggesting specific indexes based on query patterns and table access

This check is designed for deeper analysis and should be run separately
from standard health checks or included as an optional appendix.

Weight: 8 (High value for optimization planning)
"""

import re
from plugins.common.check_helpers import CheckContentBuilder
from plugins.postgres.utils.qrylib.query_optimization_opportunities import (
    get_user_resource_aggregation_query,
    get_query_with_seqscan_correlation_query,
    get_tables_with_high_seqscans_query
)


def get_weight():
    """
    Returns the importance score for this module.

    Weight: 8 (Very high value optimization analysis)
    - Provides actionable recommendations with ROI estimates
    - Team/service attribution for organizational planning
    - Specific index candidates with CREATE statements
    """
    return 8


def _extract_table_names(query_text):
    """
    Extract table names from SQL query text.

    Simple regex-based extraction for common patterns:
    - FROM tablename
    - JOIN tablename
    - UPDATE tablename
    - DELETE FROM tablename

    Args:
        query_text: SQL query string

    Returns:
        list: Unique table names found in query
    """
    if not query_text:
        return []

    # Patterns to extract table names - handles quoted identifiers and ONLY keyword
    # Matches: FROM ONLY "schema"."table_name" alias
    #          FROM schema.table
    #          FROM table
    # Note: \w includes letters, digits, and underscore
    patterns = [
        # FROM [ONLY] ["schema".]"table" [alias]
        # Captures schema in group 1, table in group 2
        r'FROM\s+(?:ONLY\s+)?(?:"([^"]+)"\.)?(?:"([^"]+)"|(\w+))(?:\s+\w+)?',
        # JOIN ["schema".]"table" [alias]
        r'JOIN\s+(?:"([^"]+)"\.)?(?:"([^"]+)"|(\w+))(?:\s+\w+)?',
        # UPDATE ["schema".]"table"
        r'UPDATE\s+(?:"([^"]+)"\.)?(?:"([^"]+)"|(\w+))',
        # DELETE FROM [ONLY] ["schema".]"table"
        r'DELETE\s+FROM\s+(?:ONLY\s+)?(?:"([^"]+)"\.)?(?:"([^"]+)"|(\w+))',
    ]

    tables = set()

    for pattern in patterns:
        matches = re.findall(pattern, query_text, re.IGNORECASE)
        for match in matches:
            # match is tuple (schema, quoted_table, unquoted_table)
            # schema is match[0], table is either match[1] (quoted) or match[2] (unquoted)
            table_name = match[1] if match[1] else match[2]

            if table_name:
                # Clean up and lowercase
                table = table_name.strip().lower()

                # Filter out schema names and SQL keywords
                if table not in ['only', 'public', 'pg_catalog', 'information_schema']:
                    tables.add(table)

    return list(tables)


def _extract_where_columns(query_text):
    """
    Extract column names from WHERE clauses.

    Handles patterns like:
    - column = value
    - $1 = table.column (positional parameter on left)
    - table.column = $1 (positional parameter on right)
    - column OPERATOR value
    - column IN (...)

    Args:
        query_text: SQL query string

    Returns:
        list: Column names found in WHERE clauses
    """
    if not query_text or 'WHERE' not in query_text.upper():
        return []

    # Extract WHERE clause
    where_match = re.search(r'WHERE\s+(.+?)(?:ORDER BY|GROUP BY|LIMIT|$)', query_text, re.IGNORECASE | re.DOTALL)
    if not where_match:
        return []

    where_clause = where_match.group(1)

    columns = set()

    # Helper function to extract column name from various formats
    def extract_column_name(text):
        """Extract column name from text, handling quoted identifiers and table prefixes."""
        text = text.strip()

        # Remove double quotes
        text = text.replace('"', '')

        # Handle table.column format
        if '.' in text:
            parts = text.split('.')
            # Return just the column part (last element)
            return parts[-1].lower()

        return text.lower()

    # Pattern 1: column OPERATOR(...) syntax (PostgreSQL explicit operator)
    # Matches: "trip_id" OPERATOR(pg_catalog.=) $1
    operator_syntax = r'(?:"[\w]+"|[\w.]+)\s+OPERATOR\([^)]+\)'
    for match in re.finditer(operator_syntax, where_clause, re.IGNORECASE):
        full_match = match.group(0)
        # Extract the column name (before OPERATOR)
        col_match = re.match(r'("[\w]+"|([\w.]+))\s+OPERATOR', full_match, re.IGNORECASE)
        if col_match:
            col_text = col_match.group(1)
            col = extract_column_name(col_text)
            if col not in ['select', 'from', 'where', 'and', 'or', 'not', 'null', 'true', 'false']:
                columns.add(col)

    # Pattern 2: Handle standard comparison operators (=, !=, <, >, <=, >=)
    # Match both: "$1 = table.column" and "table.column = $1"
    # Also handles quoted identifiers: "$1 = "column_name""
    comparison_pattern = r'(?:\$\d+|"[\w]+"|\w+)\s*([=!<>]+)\s*(?:\$\d+|"[\w]+"|\w+)'
    for match in re.finditer(comparison_pattern, where_clause, re.IGNORECASE):
        # Get the full matched text
        full_match = match.group(0)

        # Split by the operator to get left and right sides
        operator = match.group(1)
        parts = full_match.split(operator)

        for part in parts:
            part = part.strip()
            # Skip positional parameters
            if part.startswith('$') and part[1:].isdigit():
                continue
            # Skip numeric literals and string literals
            if part.isdigit() or part.startswith("'"):
                continue

            # Extract column name
            col = extract_column_name(part)
            if col and col not in ['select', 'from', 'where', 'and', 'or', 'not', 'null', 'true', 'false']:
                columns.add(col)

    # Pattern 3: column IN (...)
    in_pattern = r'(?:"[\w]+"|(?:[\w.]+\.)?[\w]+)\s+IN\s*\('
    for match in re.finditer(in_pattern, where_clause, re.IGNORECASE):
        col_text = match.group(0).replace('IN', '').replace('(', '').strip()
        col = extract_column_name(col_text)
        if col not in ['select', 'from', 'where', 'and', 'or', 'not', 'null', 'true', 'false']:
            columns.add(col)

    return list(columns)


def _get_table_indexes(connector, table_names):
    """
    Get all indexes for a list of tables.

    Args:
        connector: PostgresConnector instance
        table_names: List of table names to check

    Returns:
        dict: {table_name: {column_name: [index_names]}}
    """
    if not table_names:
        return {}

    # Build query to get indexes for all tables
    table_list = "','".join(table_names)
    query = f"""
    SELECT
        t.relname AS table_name,
        i.relname AS index_name,
        a.attname AS column_name,
        ix.indisprimary AS is_primary,
        ix.indisunique AS is_unique
    FROM pg_class t
    JOIN pg_index ix ON t.oid = ix.indrelid
    JOIN pg_class i ON i.oid = ix.indexrelid
    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
    WHERE t.relname IN ('{table_list}')
      AND t.relkind = 'r'
    ORDER BY t.relname, i.relname, a.attnum
    """

    try:
        _, raw_results = connector.execute_query(query, return_raw=True)

        # Build nested dict: table -> column -> [indexes]
        table_indexes = {}
        for row in raw_results:
            table = row['table_name'].lower()
            column = row['column_name'].lower()
            index = row['index_name']

            if table not in table_indexes:
                table_indexes[table] = {}
            if column not in table_indexes[table]:
                table_indexes[table][column] = []

            table_indexes[table][column].append({
                'name': index,
                'is_primary': row['is_primary'],
                'is_unique': row['is_unique']
            })

        return table_indexes
    except Exception as e:
        # If query fails, return empty dict
        return {}


def check_query_optimization_opportunities(connector, settings):
    """
    Analyzes query patterns and provides optimization opportunities.

    Generates:
    1. User/service resource aggregation
    2. Detailed query analysis with full text
    3. Sequential scan correlation
    4. Specific index recommendations

    Args:
        connector: PostgresConnector instance
        settings: Configuration dictionary

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder()
    builder.h3("Query Optimization Opportunities")

    # Check if pg_stat_statements is enabled
    if not connector.has_pgstat:
        builder.note(
            "The `pg_stat_statements` extension is not enabled. "
            "This check cannot be performed.\n\n"
            "To enable: `CREATE EXTENSION pg_stat_statements;` and restart PostgreSQL."
        )
        findings = {
            'status': 'not_applicable',
            'reason': 'pg_stat_statements not enabled',
            'data': []
        }
        return builder.build(), findings

    builder.text(
        "This analysis identifies specific optimization opportunities by correlating "
        "high-CPU queries with sequential scan data and providing actionable "
        "recommendations including specific index candidates."
    )
    builder.blank()

    findings_data = {}

    try:
        # ===== SECTION 1: User/Service Resource Aggregation =====
        builder.h4("Top Resource-Consuming Users/Services")
        builder.text(
            "Aggregated resource consumption by database user/service. "
            "Use this to identify which teams or applications to work with for optimization."
        )
        builder.blank()

        user_agg_query = get_user_resource_aggregation_query(connector)
        params = {'limit': settings.get('row_limit', 10)}

        user_agg_formatted, user_agg_raw = connector.execute_query(
            user_agg_query,
            params=params,
            return_raw=True
        )

        if "[ERROR]" in user_agg_formatted:
            builder.error(f"User aggregation query failed:\n{user_agg_formatted}")
        elif not user_agg_raw:
            builder.note("No users found consuming >1% of cluster CPU.")
        else:
            builder.text(user_agg_formatted)
            findings_data['user_aggregation'] = user_agg_raw
            builder.blank()

            # Add insights
            top_users = user_agg_raw[:3] if len(user_agg_raw) >= 3 else user_agg_raw
            builder.text("*Key Observations:*")
            for idx, user in enumerate(top_users, 1):
                username = user.get('username', 'unknown')
                cpu_pct = user.get('percent_of_cluster_cpu', 0)
                query_count = user.get('query_count', 0)
                builder.text(f"{idx}. **{username}**: {cpu_pct}% cluster CPU across {query_count} queries")
            builder.blank()

        # ===== SECTION 2: Tables with High Sequential Scans =====
        builder.h4("Tables with High Sequential Scans")
        builder.text(
            "Tables being frequently scanned sequentially. These are prime candidates "
            "for index optimization."
        )
        builder.blank()

        seqscan_query = get_tables_with_high_seqscans_query()
        seqscan_formatted, seqscan_raw = connector.execute_query(
            seqscan_query,
            params=params,
            return_raw=True
        )

        seqscan_tables = {}
        if seqscan_raw:
            builder.text(seqscan_formatted)
            findings_data['high_seqscan_tables'] = seqscan_raw
            builder.blank()

            # Create lookup dict for correlation
            for row in seqscan_raw:
                table_name = row.get('tablename', '').lower()
                seqscan_tables[table_name] = row

        # ===== SECTION 3: Detailed Query Analysis =====
        builder.h4("Detailed Query Analysis with Optimization Recommendations")
        builder.text(
            "High-impact queries with full SQL text, sequential scan correlation, "
            "and specific index recommendations."
        )
        builder.blank()

        query_detail_query = get_query_with_seqscan_correlation_query(connector)
        detail_params = {
            'min_cpu_percent': settings.get('optimization_min_cpu_percent', 2.0),
            'limit': settings.get('row_limit', 10)
        }

        query_detail_formatted, query_detail_raw = connector.execute_query(
            query_detail_query,
            params=detail_params,
            return_raw=True
        )

        if "[ERROR]" in query_detail_formatted:
            builder.error(f"Query detail analysis failed:\n{query_detail_formatted}")
            findings_data['query_details'] = []
        elif not query_detail_raw:
            builder.note("No queries found consuming >2% of cluster CPU.")
            findings_data['query_details'] = []
        else:
            findings_data['query_details'] = query_detail_raw

            # Collect all unique table names from all queries for index lookup
            all_tables = set()
            for query in query_detail_raw:
                tables = _extract_table_names(query.get('full_query_text', ''))
                all_tables.update(tables)

            # Fetch indexes for all tables at once
            table_indexes = _get_table_indexes(connector, list(all_tables))

            # Process each query
            for idx, query in enumerate(query_detail_raw, 1):
                username = query.get('username', 'unknown')
                cpu_pct = float(query.get('percent_of_cluster_cpu') or 0)
                avg_time = float(query.get('avg_exec_time_ms') or 0)
                calls_per_hour = float(query.get('calls_per_hour') or 0)
                full_query = query.get('full_query_text', '')

                # Header for this query
                builder.text(f"==== Query #{idx}: {username} ({cpu_pct}% cluster CPU)")
                builder.blank()

                # Performance metrics
                builder.text("*Performance Metrics:*")
                total_executions = int(query.get('total_executions') or 0)
                cpu_time_hours = float(query.get('cpu_time_hours') or 0)
                cache_hit_rate = float(query.get('cache_hit_rate_percent') or 0)
                io_wait_pct = float(query.get('io_wait_percent') or 0)
                temp_mb = float(query.get('temp_written_mb') or 0)

                builder.text(f"- Executions: {total_executions:,} ({calls_per_hour:,.0f}/hour)")
                builder.text(f"- Avg execution time: {avg_time:,.2f} ms")
                builder.text(f"- Total CPU time: {cpu_time_hours:,.2f} hours")
                builder.text(f"- Cache hit rate: {cache_hit_rate:.1f}%")
                if io_wait_pct > 5:
                    builder.text(f"- I/O wait: {io_wait_pct:.1f}% of execution time")
                if temp_mb > 10:
                    builder.text(f"- Temp files: {temp_mb:,.2f} MB")
                builder.blank()

                # Full query text in collapsible section
                builder.text("[%collapsible]")
                builder.text("====")
                builder.text("*Full Query Text:*")
                builder.text("[source,sql]")
                builder.text("----")
                # Format query for better readability
                formatted_query = full_query.replace(',', ',\n      ')
                builder.text(formatted_query)
                builder.text("----")
                builder.text("====")
                builder.blank()

                # Analysis and recommendations
                builder.text("*🔍 Analysis:*")

                # Extract tables and columns
                tables = _extract_table_names(full_query)
                where_columns = _extract_where_columns(full_query)

                if tables:
                    builder.text(f"- Tables accessed: {', '.join(tables)}")

                    # Check for sequential scan correlation
                    matched_tables = []
                    for table in tables:
                        if table in seqscan_tables:
                            matched_tables.append(table)

                    if matched_tables:
                        builder.text(f"- ✅ Matched with high sequential scan tables: {', '.join(matched_tables)}")
                        for table in matched_tables:
                            seq_data = seqscan_tables[table]
                            builder.text(f"  • `{table}`: {seq_data.get('seq_scan', 0):,} seq scans, "
                                       f"{seq_data.get('n_live_tup', 0):,} rows")

                if where_columns:
                    builder.text(f"- WHERE clause columns: {', '.join(where_columns)}")

                # Check indexes for WHERE columns
                missing_indexes = []
                existing_indexes = []

                if tables and where_columns:
                    for table in tables:
                        if table in table_indexes:
                            for col in where_columns:
                                if col in table_indexes[table]:
                                    # Index exists
                                    idx_info = table_indexes[table][col]
                                    idx_names = [idx['name'] for idx in idx_info]
                                    existing_indexes.append((table, col, idx_names))
                                else:
                                    # No index on this column
                                    missing_indexes.append((table, col))
                        else:
                            # Table has no indexes or wasn't found
                            for col in where_columns:
                                missing_indexes.append((table, col))

                if existing_indexes:
                    builder.text("- ✅ Indexed columns:")
                    for table, col, idx_names in existing_indexes:
                        builder.text(f"  • `{table}.{col}`: {', '.join(idx_names)}")

                if missing_indexes:
                    builder.text("- ⚠️  Missing indexes:")
                    for table, col in missing_indexes:
                        builder.text(f"  • `{table}.{col}` - No index found")

                builder.blank()

                # Recommendations - provide contextual guidance based on the situation
                if missing_indexes:
                    builder.text("*💡 Recommended Optimization: Add Missing Indexes*")
                    builder.blank()

                    # Group missing indexes by table
                    table_missing_cols = {}
                    for table, col in missing_indexes:
                        if table not in table_missing_cols:
                            table_missing_cols[table] = []
                        table_missing_cols[table].append(col)

                    # Show all CREATE INDEX statements first
                    for table, cols in table_missing_cols.items():
                        # Suggest index on WHERE columns (limit to first 3 for composite)
                        index_cols = ', '.join(cols[:3])
                        index_name = f"idx_{table}_{'_'.join(cols[:3])}"

                        builder.text(f"**Create Index on `{table}`:**")
                        builder.text("[source,sql]")
                        builder.text("----")
                        builder.text(f"CREATE INDEX CONCURRENTLY {index_name}")
                        builder.text(f"  ON {table} ({index_cols});")
                        builder.text("----")
                        builder.blank()

                    # Show single Expected Impact section for all indexes
                    builder.text("*Expected Impact:*")
                    builder.text(f"- Reduce avg query time from {avg_time:,.0f}ms (current baseline)")
                    builder.text(f"- Free up {cpu_pct:.2f}% of cluster CPU")

                    # Check if any tables have high seq scans
                    tables_with_seqscans = []
                    for table in table_missing_cols.keys():
                        if table in seqscan_tables:
                            seq_count = seqscan_tables[table].get('seq_scan', 0)
                            tables_with_seqscans.append((table, seq_count))

                    if tables_with_seqscans:
                        builder.text("- Eliminate sequential scans:")
                        for table, seq_count in tables_with_seqscans:
                            builder.text(f"  • `{table}`: ~{seq_count:,} scans")

                    builder.blank()

                elif existing_indexes:
                    # Query has indexes - determine the right recommendation
                    is_high_frequency = calls_per_hour > 1000  # More than 1K calls/hour
                    is_slow = avg_time > 1000  # More than 1 second
                    is_fast = avg_time < 10  # Less than 10ms

                    if is_high_frequency and is_fast:
                        # Fast query but high frequency - recommend caching
                        builder.text("*💡 Recommended Optimization: Application-Level Caching*")
                        builder.blank()
                        builder.text("*Why This Matters:*")
                        builder.text(f"This query is already optimized (indexed, {avg_time:.2f}ms avg, {cache_hit_rate:.1f}% cache hit),")
                        builder.text(f"but executes {calls_per_hour:,.0f} times/hour, consuming {cpu_pct:.2f}% of cluster CPU.")
                        builder.text("Further database-level optimization won't help - the issue is query frequency.")
                        builder.blank()
                        builder.text("*Recommended Actions:*")
                        builder.text("1. **Implement application-level caching** (Redis, Memcached, or in-memory cache)")
                        builder.text("   - Cache results for this lookup query")
                        builder.text("   - Set appropriate TTL based on data update frequency")
                        builder.text(f"   - Could reduce database load by up to {cpu_pct:.2f}%")
                        builder.blank()
                        builder.text("2. **Consider data preloading** (if this is reference/lookup data)")
                        builder.text("   - Load data at application startup")
                        builder.text("   - Refresh periodically or on-demand")
                        builder.blank()
                        builder.text("3. **Batch operations** (if applicable)")
                        builder.text("   - Fetch multiple rows in a single query instead of N individual queries")
                        builder.text("   - Use IN clauses or JOIN operations where possible")
                        builder.blank()

                    elif is_slow:
                        # Slow despite indexes - investigate query plan
                        builder.text("*💡 Recommended Action: Investigate Query Execution*")
                        builder.blank()
                        builder.text("*Why This Matters:*")
                        builder.text(f"Query is slow ({avg_time:,.0f}ms avg) despite having indexes on WHERE columns.")
                        builder.text("The indexes may not be getting used, or the query plan may be inefficient.")
                        builder.blank()
                        builder.text("*Recommended Actions:*")
                        builder.text("1. **Analyze the execution plan:**")
                        builder.text("   ```sql")
                        builder.text("   EXPLAIN (ANALYZE, BUFFERS) <your_query>;")
                        builder.text("   ```")
                        builder.text("   - Verify indexes are being used (look for 'Index Scan' not 'Seq Scan')")
                        builder.text("   - Check for nested loops with high row counts")
                        builder.text("   - Look for buffer misses indicating cache issues")
                        builder.blank()
                        builder.text("2. **Update table statistics:**")
                        for table in tables:
                            builder.text(f"   ANALYZE {table};")
                        builder.text("   - Outdated statistics can cause poor query plans")
                        builder.blank()
                        builder.text("3. **Consider these possibilities:**")
                        builder.text("   - Index selectivity may be too low (too many matching rows)")
                        builder.text("   - Missing composite index for multi-column WHERE clause")
                        builder.text("   - Query may need rewriting to use indexes effectively")
                        builder.text("   - Large result set or expensive operations (sorting, aggregation)")
                        builder.blank()

                    else:
                        # Moderate frequency/speed - provide general optimization advice
                        builder.text("*💡 Optimization Status: Already Indexed*")
                        builder.blank()
                        builder.text(f"This query has indexes on WHERE columns and runs in {avg_time:.2f}ms average.")
                        if cpu_pct > 1.0:
                            builder.text(f"However, it still consumes {cpu_pct:.2f}% of cluster CPU due to frequency.")
                            builder.text("Consider application-level caching if this becomes a bottleneck.")
                        builder.blank()

                else:
                    # No WHERE clause or index info - check if it's a write operation
                    query_upper = full_query.upper()
                    is_insert = 'INSERT INTO' in query_upper
                    is_update = 'UPDATE' in query_upper and 'SET' in query_upper
                    is_delete = 'DELETE FROM' in query_upper

                    if is_insert or is_update or is_delete:
                        # Write operation - different optimization advice
                        operation_type = 'INSERT' if is_insert else ('UPDATE' if is_update else 'DELETE')
                        is_high_frequency = calls_per_hour > 1000  # More than 1K calls/hour

                        if is_high_frequency:
                            builder.text(f"*💡 Recommended Optimization: Bulk {operation_type} Operations*")
                            builder.blank()
                            builder.text("*Why This Matters:*")
                            builder.text(f"This {operation_type} operation executes {calls_per_hour:,.0f} times/hour ({total_executions:,} total),")
                            builder.text(f"consuming {cpu_pct:.2f}% of cluster CPU. Individual {operation_type}s are inefficient at this scale.")
                            builder.blank()
                            builder.text("*Recommended Actions:*")

                            if is_insert:
                                builder.text("1. **Batch inserts using multi-value syntax:**")
                                builder.text("   ```sql")
                                builder.text("   -- Instead of N individual INSERTs:")
                                builder.text("   INSERT INTO table (col1, col2) VALUES ($1, $2);")
                                builder.text("   -- Use single INSERT with multiple rows:")
                                builder.text("   INSERT INTO table (col1, col2) VALUES ($1, $2), ($3, $4), ..., ($N, $M);")
                                builder.text("   ```")
                                builder.blank()
                                builder.text("2. **Use COPY for bulk loading:**")
                                builder.text("   - For large batches, COPY FROM STDIN is 10-100x faster than INSERT")
                                builder.text("   - Most database drivers support COPY protocol")
                                builder.blank()
                                builder.text("3. **Consider asynchronous writes:**")
                                builder.text("   - Queue insert operations and batch them")
                                builder.text("   - Use background workers to process batches")
                                builder.text(f"   - Could reduce database load by up to {cpu_pct:.2f}%")
                                builder.blank()

                                # For very high frequency inserts, suggest architectural alternatives
                                if calls_per_hour > 100000:  # More than 100K inserts/hour
                                    builder.text("4. **Consider architectural alternatives for high-volume event data:**")
                                    builder.blank()
                                    builder.text("   At this scale (100K+ inserts/hour), consider specialized data stores:")
                                    builder.blank()
                                    builder.text("   - **Kafka:** Event streaming platform for real-time IoT/telemetry data")
                                    builder.text("     • Built for high-throughput writes (millions of events/sec)")
                                    builder.text("     • Natural fit for time-series event logs")
                                    builder.text("     • Can feed data to multiple downstream consumers")
                                    builder.blank()
                                    builder.text("   - **Cassandra:** Wide-column store optimized for write-heavy workloads")
                                    builder.text("     • Linear write scalability")
                                    builder.text("     • Excellent for time-series data with TTL support")
                                    builder.text("     • Multi-datacenter replication for IoT devices")
                                    builder.blank()
                                    builder.text("   - **TimescaleDB:** PostgreSQL extension for time-series data")
                                    builder.text("     • Keeps PostgreSQL compatibility while improving write performance")
                                    builder.text("     • Automatic partitioning and compression")
                                    builder.text("     • Better suited for event/metrics data than standard PostgreSQL")
                                    builder.blank()
                                    builder.text("   **💡 Decision Framework:**")
                                    builder.text("   - Use trend analysis database to track this metric over time")
                                    builder.text("   - If write volume continues growing, PostgreSQL may become a bottleneck")
                                    builder.text("   - Consider hybrid architecture: Kafka/Cassandra for ingestion, PostgreSQL for analytics")
                                    builder.blank()

                            elif is_update:
                                builder.text("1. **Batch UPDATE operations:**")
                                builder.text("   - Group multiple updates into fewer transactions")
                                builder.text("   - Use UPDATE with IN clause or temporary tables")
                                builder.blank()
                                builder.text("2. **Review UPDATE triggers and constraints:**")
                                builder.text("   - Each UPDATE may fire triggers and check constraints")
                                builder.text("   - Consider deferring constraint checks")
                                builder.blank()

                            else:  # DELETE
                                builder.text("1. **Batch DELETE operations:**")
                                builder.text("   - Use DELETE with IN clause for multiple rows")
                                builder.text("   - Consider TRUNCATE for clearing entire tables")
                                builder.blank()

                            # Check for excessive indexes on the table
                            if tables:
                                for table in tables:
                                    if table in table_indexes:
                                        index_count = len(set(
                                            idx['name']
                                            for col_indexes in table_indexes[table].values()
                                            for idx in col_indexes
                                        ))
                                        if index_count > 5:
                                            builder.text(f"**⚠️  Note:** Table `{table}` has {index_count} indexes")
                                            builder.text(f"- Each {operation_type} must update all indexes")
                                            builder.text("- Review if all indexes are necessary")
                                            builder.text("- Consider removing unused indexes to speed up writes")
                                            builder.blank()

                        else:
                            # Low frequency write operation
                            builder.text(f"*Query Type: {operation_type} Operation*")
                            builder.blank()
                            if tables:
                                builder.text(f"This {operation_type} operates on: {', '.join(tables)}")
                                if avg_time > 100:
                                    builder.text(f"Average execution time: {avg_time:.2f}ms")
                                    builder.text("Run EXPLAIN (ANALYZE, BUFFERS) if performance is a concern.")
                            builder.blank()

                    elif avg_time > 1000:
                        # General slow query advice for SELECT without WHERE
                        builder.text("*💡 Recommended Action: Investigate Query Performance*")
                        builder.blank()
                        builder.text(f"Query is slow ({avg_time:,.0f}ms avg). Run EXPLAIN (ANALYZE, BUFFERS) to identify bottleneck:")
                        builder.text("- Look for sequential scans that should be index scans")
                        builder.text("- Check for nested loops with high row counts")
                        builder.text("- Identify expensive operations (sorts, hash joins, aggregations)")
                        builder.blank()

                # Separator between queries
                builder.text("---")
                builder.blank()

        # Final recommendations
        recommendations = {
            "high": [
                "Optimization Workflow:",
                "  1. Review user/service aggregation to identify team ownership",
                "  2. For each recommended index:",
                "     a. Run EXPLAIN (ANALYZE, BUFFERS) to confirm sequential scans",
                "     b. Create index using CONCURRENTLY to avoid table locks",
                "     c. Monitor query performance after index creation",
                "  3. Re-run this check after optimizations to measure impact",
                "",
                "Index Creation Best Practices:",
                "  • Always use CONCURRENTLY for production databases",
                "  • Create indexes during low-traffic periods when possible",
                "  • Monitor disk space - indexes consume storage",
                "  • Consider partial indexes for frequently filtered queries",
                "  • Composite indexes: put high-selectivity columns first",
            ],
            "general": [
                "Understanding Sequential Scans:",
                "  • Sequential scans read entire table from disk",
                "  • Acceptable for small tables (<10,000 rows) or full-table queries",
                "  • Problematic for large tables with selective queries",
                "  • seq_scan_percent > 80% indicates missing index opportunities",
                "",
                "When NOT to Create an Index:",
                "  • Table has < 10,000 rows (sequential scan is faster)",
                "  • Query returns > 20% of table rows (seq scan is more efficient)",
                "  • Column has low cardinality (few distinct values)",
                "  • Write-heavy table (indexes slow down INSERTs/UPDATEs)",
                "",
                "Monitoring Index Effectiveness:",
                "  • Check pg_stat_user_indexes.idx_scan after creation",
                "  • If idx_scan stays at 0, index is not being used (drop it)",
                "  • Compare query performance before/after using pg_stat_statements",
                "  • Monitor disk usage with pg_total_relation_size()",
            ]
        }

        builder.recs(recommendations)

        # Build final findings
        findings = {
            'status': 'success',
            'message': f'Analyzed {len(query_detail_raw)} optimization opportunities',
            'data': findings_data,
            'metadata': {
                'check': 'query_optimization_opportunities',
                'requires_extension': 'pg_stat_statements',
                'postgres_version': connector.version_info.get('version', 'unknown')
            }
        }

        return builder.build(), findings

    except Exception as e:
        builder.error(f"Failed during optimization analysis: {str(e)}")
        findings = {
            'status': 'error',
            'error_message': str(e),
            'data': {}
        }
        return builder.build(), findings
