
"""
Table Counts Check

Queries system_schema.tables to coynt tables
    Called from table_statistics
    Checks the number of user tables in the cluster.
    Returns WARN when count between 301 and 999
    Return CAUTION status when count >= 1000
    :param total_tables:
    :return: status

CQL-only check - works on managed Instaclustr clusters.
"""


def check_table_counts(total_tables, builder):

    table_count_caution=200
    table_count_warn=300
    table_count_critical=1000

    status="OK"
    message=""

    builder.h4("Why the number of tables matters")
    builder.para("Every Table requires memory, both on-=heap and off-heap , and disk storage.  Too many tables can impact performance by reducing the amount of memory needed for your work load. ")
    builder.para("More tables means more memtables in memory. When the number of tables gets too high,  memtables are smaller and are flushed frequently. The large number of small SSTables requires more cycles to compact. ")
    builder.para("Repairs can take longer and generate more overhead on your nodes.")
    builder.text("The point where a high number of tables creates performance problems depends, in part, of the size of your Cassandra node.")
    builder.recs([f"Instaclustr recommends keeping the number of tables between  {table_count_caution} and  {table_count_warn} "] )

    builder.tip(["Do not create a separate table for each user or tenant",
                "Do not use tables to partition data",
                "Instead use partition keys to order data within a single table"] )

    if total_tables < table_count_warn:
        status="OK"
        builder.tip(f"Keep the number of tables below {table_count_warn} " )
    elif table_count_warn <= total_tables  < table_count_critical :
        status="CAUTION"
        builder.warning(f"Number of tables exceeds recommended limit of " + {table_count_warn} )
    elif total_tables > table_count_critical:
        status="WARN"
        builder.critical(f"The number of tables exceeds + {table_count_critical}. You may experience performance issues on the node.")

    if status != "OK":
        builder.recs(["If you are experiencing Java Out of Memory (OOM) errors, increase the size of the Java heap",
                      "If you are experiences rapid memtable flushes, increase the size of memtable_total_space",
                      " Drop unused tables",
                     "The only long-term fix is to redesign your data model."])

    return builder
