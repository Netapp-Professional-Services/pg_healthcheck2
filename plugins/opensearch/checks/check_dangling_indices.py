"""
Dangling Indices Check

Identifies indices that exist on disk but are not part of the cluster state.
This indicates potential data recovery opportunities or orphaned data cleanup.

Data source:
- Live clusters: GET /_dangling
- Imported diagnostics: dangling_indices.json

Sales Opportunities:
- Data recovery consulting for lost indices
- Migration assistance for orphaned data
- Disaster recovery planning services
"""

from plugins.common.check_helpers import CheckContentBuilder


def run(connector, settings):
    """
    Check for dangling indices.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("Dangling Indices Analysis")

    try:
        # Get dangling indices data
        dangling_data = connector.execute_query({"operation": "dangling_indices"})

        if isinstance(dangling_data, dict) and "error" in dangling_data:
            builder.note(
                "Dangling indices information is not available.\n\n"
                "This API may require additional permissions or may not be supported."
            )
            return builder.build(), {
                "status": "unavailable",
                "data": {"reason": dangling_data.get("error", "Unknown")}
            }

        dangling_indices = dangling_data.get("dangling_indices", [])
        dangling_count = len(dangling_indices)

        if dangling_count > 0:
            builder.warning(
                f"**{dangling_count} Dangling Indices Detected**\n\n"
                "Dangling indices exist on disk but are not part of the cluster state. "
                "These may contain recoverable data from a previous cluster configuration."
            )
            builder.blank()

            # Build table of dangling indices
            builder.h4("Dangling Index Details")
            index_table = []
            total_shards = 0

            for idx in dangling_indices:
                index_name = idx.get("index_name", "unknown")
                index_uuid = idx.get("index_uuid", "unknown")
                creation_date = idx.get("creation_date_millis", 0)
                node_ids = idx.get("node_ids", [])

                # Calculate creation date
                if creation_date:
                    from datetime import datetime
                    creation_str = datetime.fromtimestamp(creation_date / 1000).strftime("%Y-%m-%d %H:%M:%S")
                else:
                    creation_str = "Unknown"

                index_table.append({
                    "Index Name": index_name,
                    "UUID": index_uuid[:8] + "..." if len(index_uuid) > 8 else index_uuid,
                    "Created": creation_str,
                    "Nodes": str(len(node_ids))
                })

            builder.table(index_table)
            builder.blank()

            # Recovery options
            builder.h4("Recovery Options")
            builder.para(
                "Dangling indices can be imported back into the cluster using the "
                "dangling index API. Before importing, verify that the data is still needed "
                "and that there are no naming conflicts."
            )

            builder.text("*To import a dangling index:*")
            builder.code(
                "POST /_dangling/<index-uuid>?accept_data_loss=true",
                language="http"
            )
            builder.blank()

            builder.text("*To delete a dangling index permanently:*")
            builder.code(
                "DELETE /_dangling/<index-uuid>?accept_data_loss=true",
                language="http"
            )
            builder.blank()

            # Recommendations
            recs = {
                "high": [
                    "Review dangling indices to determine if data recovery is needed",
                    "Import indices containing critical data before they are lost",
                    "Investigate root cause of cluster state inconsistency"
                ],
                "general": [
                    "Implement regular snapshots to prevent data loss",
                    "Consider disaster recovery planning services",
                    "Document recovery procedures for future incidents"
                ]
            }
            builder.recs(recs)

        else:
            builder.success(
                "✅ No dangling indices detected.\n\n"
                "The cluster state is consistent with data on disk."
            )

        # Build structured data for rules engine
        return builder.build(), {
            "status": "success",
            "data": {
                "dangling_count": dangling_count,
                "has_dangling_indices": dangling_count > 0,
                # Sales signals
                "needs_data_recovery": dangling_count > 0,
                "recovery_consulting_opportunity": dangling_count > 2,
                "disaster_recovery_gap": dangling_count > 0,
            }
        }

    except Exception as e:
        builder.error(f"Failed to check dangling indices: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }
