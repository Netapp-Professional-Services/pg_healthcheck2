from plugins.common.check_helpers import CheckContentBuilder
from plugins.kafka.utils.qrylib.consumer_group_health_queries import get_describe_consumer_groups_query
from plugins.kafka.utils.qrylib.consumer_group_health_queries import get_all_consumer_lag_query

def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 8

def run_check_consumer_group_health(connector, settings):
    """
    Analyzes consumer group health including state, member count, and lag.
    
    Filters out test/console consumers and focuses on production groups.
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}
    
    try:
        builder.h3("Consumer Group Health")
        
        # Get thresholds from settings
        critical_lag = settings.get('kafka_lag_critical', 10000)  # Default 10k messages
        warning_lag = settings.get('kafka_lag_warning', 5000)  # Default 5k messages
        min_members = settings.get('kafka_min_consumer_members', 2)
        
        # === STEP 1: COLLECT FACTS - Consumer Group Descriptions ===
        desc_query = get_describe_consumer_groups_query(connector)
        desc_formatted, desc_raw = connector.execute_query(desc_query, return_raw=True)
        
        # Check for errors
        if "[ERROR]" in desc_formatted or (isinstance(desc_raw, dict) and 'error' in desc_raw):
            builder.add(desc_formatted)
            structured_data["consumer_groups"] = {"status": "error", "data": desc_raw}
            return builder.build(), structured_data
        
        if not isinstance(desc_raw, list):
            builder.error(f"Unexpected consumer group data format: {type(desc_raw).__name__}")
            structured_data["consumer_groups"] = {
                "status": "error", 
                "details": f"Expected list, got {type(desc_raw).__name__}"
            }
            return builder.build(), structured_data
        
        # === STEP 2: COLLECT FACTS - Consumer Lag ===
        lag_query = get_all_consumer_lag_query(connector)
        lag_formatted, lag_raw = connector.execute_query(lag_query, return_raw=True)
        
        # Check for errors
        if "[ERROR]" in lag_formatted or (isinstance(lag_raw, dict) and 'error' in lag_raw):
            builder.add(lag_formatted)
            structured_data["consumer_groups"] = {"status": "error", "data": lag_raw}
            return builder.build(), structured_data
        
        if not isinstance(lag_raw, dict):
            builder.error(f"Unexpected lag data format: {type(lag_raw).__name__}")
            structured_data["consumer_groups"] = {
                "status": "error",
                "details": f"Expected dict, got {type(lag_raw).__name__}"
            }
            return builder.build(), structured_data
        
        # === STEP 3: INTERPRET FACTS - Build Combined View ===
        
        # Extract lag data
        group_lags_details = lag_raw.get('group_lags', [])
        groups_without_offsets = lag_raw.get('groups_without_offsets', [])
        groups_with_errors = lag_raw.get('groups_with_errors', [])
        
        # Aggregate lag by group
        group_lag_map = {}
        for item in group_lags_details:
            group_id = item.get('group_id')
            if group_id not in group_lag_map:
                group_lag_map[group_id] = {'total_lag': 0, 'max_lag': 0, 'details': []}
            lag_value = item.get('lag', 0)
            group_lag_map[group_id]['total_lag'] += lag_value
            group_lag_map[group_id]['max_lag'] = max(group_lag_map[group_id]['max_lag'], lag_value)
            group_lag_map[group_id]['details'].append(item)
        
        # Helper: Identify test/console consumers
        def is_test_consumer(group_id):
            """Returns True if this looks like a test/console consumer."""
            if not group_id:
                return False
            test_patterns = ['console-consumer-', 'test-consumer-', 'temp-']
            return any(pattern in group_id.lower() for pattern in test_patterns)
        
        # Combine group descriptions with lag data
        all_groups = []
        production_groups = []
        test_groups = []
        
        unstable_groups = []
        low_member_groups = []
        critical_lag_groups = []
        warning_lag_groups = []
        
        for group in desc_raw:
            group_id = group.get('group_id')
            group_state = group.get('state')
            member_count = group.get('members', 0)
            
            # Determine lag status
            if group_id in groups_without_offsets:
                total_lag = 0
                max_lag = 0
                has_offsets = False
            elif group_id in group_lag_map:
                total_lag = group_lag_map[group_id]['total_lag']
                max_lag = group_lag_map[group_id]['max_lag']
                has_offsets = True
            else:
                total_lag = 0
                max_lag = 0
                has_offsets = False
            
            # Build combined record
            group_data = {
                'group_id': group_id,
                'state': group_state,
                'members': member_count,
                'total_lag': total_lag,
                'max_lag': max_lag,
                'has_offsets': has_offsets,
                'is_test_consumer': is_test_consumer(group_id)
            }
            
            all_groups.append(group_data)
            
            # Categorize
            if is_test_consumer(group_id):
                test_groups.append(group_data)
            else:
                production_groups.append(group_data)
                
                # === INTERPRET: Apply business logic (production groups only) ===
                if group_state != 'Stable':
                    unstable_groups.append(group_id)
                
                if member_count < min_members:
                    low_member_groups.append(group_id)
                
                if has_offsets and total_lag > critical_lag:
                    critical_lag_groups.append(group_id)
                elif has_offsets and total_lag > warning_lag:
                    warning_lag_groups.append(group_id)
        
        # === STEP 4: BUILD REPORT ===
        
        if not all_groups:
            builder.note("No consumer groups found.")
            structured_data["consumer_groups"] = {
                "status": "success",
                "groups_analyzed": 0,
                "production_groups": 0,
                "test_groups": 0,
                "data": []
            }
            return builder.build(), structured_data
        
        # Summary counts
        issues_found = bool(unstable_groups or low_member_groups or critical_lag_groups or warning_lag_groups)
        
        # Overall status
        if critical_lag_groups:
            builder.critical(
                f"**Critical Consumer Lag:** {len(critical_lag_groups)} production consumer group(s) "
                f"have lag exceeding {critical_lag:,} messages. This may lead to data loss if retention expires."
            )
        
        if warning_lag_groups:
            builder.warning(
                f"**High Consumer Lag:** {len(warning_lag_groups)} production consumer group(s) "
                f"have lag exceeding {warning_lag:,} messages."
            )
        
        if unstable_groups:
            builder.warning(
                f"**Unstable Consumer Groups:** {len(unstable_groups)} production consumer group(s) "
                f"are not in 'Stable' state. This may indicate rebalancing or failures."
            )
        
        if low_member_groups:
            builder.warning(
                f"**Low Member Count:** {len(low_member_groups)} production consumer group(s) "
                f"have fewer than {min_members} member(s). This reduces redundancy."
            )
        
        # Production groups table
        if production_groups:
            builder.h4("Production Consumer Groups")
            
            prod_rows = []
            for group in sorted(production_groups, key=lambda x: x['total_lag'], reverse=True):
                # Status indicator
                if group['total_lag'] > critical_lag:
                    indicator = "🔴"
                elif group['total_lag'] > warning_lag:
                    indicator = "⚠️"
                elif group['state'] != 'Stable':
                    indicator = "⚠️"
                elif group['members'] < min_members:
                    indicator = "⚠️"
                else:
                    indicator = "✅"
                
                prod_rows.append({
                    "Status": indicator,
                    "Group ID": group['group_id'],
                    "State": group['state'],
                    "Members": group['members'],
                    "Total Lag": f"{group['total_lag']:,}" if group['has_offsets'] else "No offsets",
                    "Max Partition Lag": f"{group['max_lag']:,}" if group['has_offsets'] and group['max_lag'] > 0 else "-"
                })
            
            builder.table(prod_rows)
        else:
            builder.note("No production consumer groups found (only test/console consumers detected).")
        
        # Test groups - show as info only
        if test_groups:
            builder.h4(f"Test/Console Consumers ({len(test_groups)})")
            builder.para("_The following appear to be temporary test or console consumers (informational only):_")
            
            test_rows = []
            for group in test_groups:
                test_rows.append({
                    "Group ID": group['group_id'],
                    "State": group['state'],
                    "Members": group['members']
                })
            
            builder.table(test_rows)
        
        # Groups without offsets
        if groups_without_offsets:
            non_test_without_offsets = [g for g in groups_without_offsets if not is_test_consumer(g)]
            if non_test_without_offsets:
                builder.h4("Groups Without Committed Offsets")
                builder.note(
                    f"{len(non_test_without_offsets)} production consumer group(s) have no committed offsets:\n\n" +
                    "\n".join([f"* {g}" for g in non_test_without_offsets[:10]]) +
                    (f"\n\n_... and {len(non_test_without_offsets) - 10} more_" if len(non_test_without_offsets) > 10 else "") +
                    "\n\n**This is normal for:**\n" +
                    "* Newly created consumer groups\n" +
                    "* Groups with `enable.auto.commit=false`\n" +
                    "* Groups that haven't completed their first poll cycle"
                )
        
        # Errors
        if groups_with_errors:
            builder.h4("Groups With Query Errors")
            builder.warning(
                f"Could not retrieve lag data for {len(groups_with_errors)} consumer group(s):\n\n" +
                "\n".join([f"* **{e.get('group_id')}**: {e.get('error')}" for e in groups_with_errors[:10]])
            )
        
        # Recommendations
        if issues_found:
            recommendations = {}
            
            if critical_lag_groups:
                recommendations["critical"] = [
                    f"**Immediately investigate consumer performance** for groups with >{critical_lag:,} lag",
                    "**Check for consumer failures** - Verify all consumer instances are running",
                    "**Review processing logic** - Look for slow operations or blocking calls",
                    "**Consider scaling** - Add more consumer instances if falling behind",
                    "**Check retention** - Ensure messages aren't expiring before consumption"
                ]
            
            if warning_lag_groups or unstable_groups or low_member_groups:
                recommendations["high"] = []
                
                if warning_lag_groups:
                    recommendations["high"].extend([
                        f"**Monitor lag trends** for groups with >{warning_lag:,} lag",
                        "**Optimize consumer processing** - Profile and improve message handling performance"
                    ])
                
                if unstable_groups:
                    recommendations["high"].extend([
                        "**Investigate unstable groups** - Check logs for rebalancing or failures",
                        "**Reduce rebalancing frequency** - Tune `session.timeout.ms` and `heartbeat.interval.ms`"
                    ])
                
                if low_member_groups:
                    recommendations["high"].extend([
                        f"**Scale up to {min_members}+ members** for redundancy and better throughput",
                        "**Document scaling procedures** for each consumer group"
                    ])
            
            recommendations["general"] = [
                "Set up alerts for consumer lag thresholds (warning at 5k, critical at 10k)",
                "Monitor consumer group state changes for early warning of issues",
                "Implement consumer health checks in your deployment pipelines",
                "Document consumer group ownership and scaling policies",
                "Regularly review consumer processing metrics and optimize bottlenecks"
            ]
            
            builder.recs(recommendations)
        else:
            # No issues
            builder.success(
                "All production consumer groups are healthy.\n\n" +
                f"Analyzed {len(production_groups)} production group(s), {len(test_groups)} test/console consumer(s)."
            )
        
        # === STRUCTURED DATA ===
        structured_data["consumer_groups"] = {
            "status": "success",
            "groups_analyzed": len(all_groups),
            "production_groups": len(production_groups),
            "test_groups": len(test_groups),
            "issues_detected": issues_found,
            "groups_with_issues": {
                "unstable": unstable_groups,
                "low_members": low_member_groups,
                "critical_lag": critical_lag_groups,
                "warning_lag": warning_lag_groups
            },
            "groups_without_offsets": groups_without_offsets,
            "groups_with_errors": groups_with_errors,
            "thresholds": {
                "critical_lag": critical_lag,
                "warning_lag": warning_lag,
                "min_members": min_members
            },
            "data": all_groups,  # All groups with categorization
            "production_data": production_groups,  # Just production groups
            "test_data": test_groups  # Just test groups
        }
        
    except Exception as e:
        import traceback
        from logging import getLogger
        logger = getLogger(__name__)
        logger.error(f"Consumer group health check failed: {e}\n{traceback.format_exc()}")
        
        builder.error(f"Check failed: {e}")
        structured_data["consumer_groups"] = {
            "status": "error",
            "details": str(e)
        }
    
    return builder.build(), structured_data
