"""
X-Pack/Commercial features status check.

Analyzes which commercial features are available, enabled, and actively used.
This check identifies license upsell opportunities and unused paid features.

Data source:
- Live clusters: GET /_xpack/usage
- Imported diagnostics: commercial/xpack.json

Sales Opportunities:
- Customers with Basic license but need Platinum features
- Customers paying for features they don't use
- Customers who could benefit from ML, CCR, or security features
"""

from plugins.common.check_helpers import CheckContentBuilder


def run(connector, settings):
    """
    Check X-Pack/commercial feature status.

    Returns:
        tuple: (adoc_content, structured_findings)
    """
    builder = CheckContentBuilder(connector.formatter)
    builder.h3("Commercial Features & License Analysis")

    try:
        # Get X-Pack usage data
        xpack_data = connector.execute_query({"operation": "xpack"})

        if isinstance(xpack_data, dict) and "error" in xpack_data:
            builder.note(
                "X-Pack feature information is not available.\n\n"
                "This may indicate an OpenSearch cluster (vs Elasticsearch) or missing diagnostic data."
            )
            return builder.build(), {
                "status": "unavailable",
                "data": {"reason": xpack_data.get("error", "Unknown")}
            }

        # Analyze features
        features_analysis = _analyze_features(xpack_data)

        # Summary
        builder.h4("Feature Summary")

        summary_table = [
            {"Category": "Licensed Features", "Count": str(features_analysis['licensed_count'])},
            {"Category": "Enabled Features", "Count": str(features_analysis['enabled_count'])},
            {"Category": "Actively Used", "Count": str(features_analysis['used_count'])},
            {"Category": "Available but Unused", "Count": str(features_analysis['unused_licensed_count'])},
            {"Category": "Disabled/Unavailable", "Count": str(features_analysis['unavailable_count'])},
        ]
        builder.table(summary_table)
        builder.blank()

        # Feature details
        builder.h4("Feature Status Details")

        feature_table = []
        for feature in features_analysis['features']:
            status_icon = "✅" if feature['available'] else "❌"
            if feature['available'] and not feature['in_use']:
                status_icon = "⚠️"  # Available but not used

            feature_table.append({
                "Feature": feature['name'],
                "Available": status_icon,
                "Enabled": "Yes" if feature['enabled'] else "No",
                "In Use": "Yes" if feature['in_use'] else "No",
                "Details": feature.get('details', '-')
            })
        builder.table(feature_table)
        builder.blank()

        # Opportunities section
        if features_analysis['opportunities']:
            builder.h4("Optimization Opportunities")

            for opp in features_analysis['opportunities']:
                if opp['type'] == 'upsell':
                    builder.warning(f"**{opp['title']}**\n\n{opp['description']}")
                elif opp['type'] == 'unused':
                    builder.note(f"**{opp['title']}**\n\n{opp['description']}")
                builder.blank()

        # Security features analysis
        security = xpack_data.get('security', {})
        if security.get('available'):
            builder.h4("Security Configuration")

            realms = security.get('realms', {})
            auth_methods = []

            for realm_type, realm_data in realms.items():
                if realm_data.get('enabled') and realm_data.get('available'):
                    auth_methods.append(realm_type)

            has_external_auth = any(r in auth_methods for r in ['ldap', 'saml', 'oidc', 'active_directory', 'kerberos', 'pki'])

            security_table = [
                {"Setting": "Security Enabled", "Value": "Yes" if security.get('enabled') else "No"},
                {"Setting": "SSL (HTTP)", "Value": "Yes" if security.get('ssl', {}).get('http', {}).get('enabled') else "No"},
                {"Setting": "SSL (Transport)", "Value": "Yes" if security.get('ssl', {}).get('transport', {}).get('enabled') else "No"},
                {"Setting": "Audit Logging", "Value": "Yes" if security.get('audit', {}).get('enabled') else "No"},
                {"Setting": "External Auth (SSO)", "Value": "Yes" if has_external_auth else "No"},
                {"Setting": "Auth Methods", "Value": ", ".join(auth_methods) if auth_methods else "file, native only"},
            ]
            builder.table(security_table)
            builder.blank()

            if not has_external_auth:
                builder.note(
                    "**SSO Integration Opportunity**\n\n"
                    "External authentication (LDAP, SAML, OIDC) is not configured. "
                    "Enterprise SSO integration can improve security and user management."
                )

        # ML analysis
        ml = xpack_data.get('ml', {})
        if ml.get('enabled'):
            builder.h4("Machine Learning Status")

            ml_jobs = ml.get('jobs', {}).get('_all', {}).get('count', 0)
            ml_datafeeds = ml.get('datafeeds', {}).get('_all', {}).get('count', 0)
            ml_dfa = ml.get('data_frame_analytics_jobs', {}).get('_all', {}).get('count', 0)
            ml_models = ml.get('inference', {}).get('trained_models', {}).get('_all', {}).get('count', 0)

            ml_table = [
                {"ML Feature": "Available", "Value": "Yes" if ml.get('available') else "No (license required)"},
                {"ML Feature": "Anomaly Detection Jobs", "Value": str(ml_jobs)},
                {"ML Feature": "Datafeeds", "Value": str(ml_datafeeds)},
                {"ML Feature": "Data Frame Analytics", "Value": str(ml_dfa)},
                {"ML Feature": "Trained Models", "Value": str(ml_models)},
            ]
            builder.table(ml_table)

        # Build structured data for rules engine
        return builder.build(), {
            "status": "success",
            "data": {
                "licensed_count": features_analysis['licensed_count'],
                "enabled_count": features_analysis['enabled_count'],
                "used_count": features_analysis['used_count'],
                "unused_licensed_count": features_analysis['unused_licensed_count'],
                "unavailable_count": features_analysis['unavailable_count'],
                "opportunity_count": len(features_analysis['opportunities']),
                # Security signals
                "security_enabled": security.get('enabled', False),
                "has_external_auth": has_external_auth if security.get('available') else None,
                "audit_enabled": security.get('audit', {}).get('enabled', False),
                # ML signals
                "ml_available": ml.get('available', False),
                "ml_enabled": ml.get('enabled', False),
                "ml_job_count": ml.get('jobs', {}).get('_all', {}).get('count', 0),
                "ml_unused": ml.get('available', False) and ml.get('jobs', {}).get('_all', {}).get('count', 0) == 0,
                # CCR signals
                "ccr_available": xpack_data.get('ccr', {}).get('available', False),
                "ccr_enabled": xpack_data.get('ccr', {}).get('enabled', False),
                "ccr_follower_count": xpack_data.get('ccr', {}).get('follower_indices_count', 0),
                # Watcher signals
                "watcher_available": xpack_data.get('watcher', {}).get('available', False),
                "watcher_count": xpack_data.get('watcher', {}).get('count', {}).get('total', 0),
                # Upsell signals
                "has_upsell_opportunities": len([o for o in features_analysis['opportunities'] if o['type'] == 'upsell']) > 0,
                "has_unused_features": features_analysis['unused_licensed_count'] > 0,
            }
        }

    except Exception as e:
        builder.error(f"Failed to check X-Pack features: {e}")
        return builder.build(), {
            "status": "error",
            "error": str(e)
        }


def _analyze_features(xpack_data):
    """Analyze X-Pack features and identify opportunities."""

    features = []
    opportunities = []

    # Define features to check
    feature_checks = [
        ('ml', 'Machine Learning', lambda d: d.get('jobs', {}).get('_all', {}).get('count', 0) > 0),
        ('ccr', 'Cross-Cluster Replication', lambda d: d.get('follower_indices_count', 0) > 0),
        ('watcher', 'Alerting (Watcher)', lambda d: d.get('count', {}).get('total', 0) > 0),
        ('security', 'Security', lambda d: d.get('enabled', False)),
        ('graph', 'Graph Analytics', lambda d: False),  # No usage stats available
        ('logstash', 'Logstash Management', lambda d: False),
        ('searchable_snapshots', 'Searchable Snapshots', lambda d: d.get('indices_count', 0) > 0),
        ('transform', 'Transforms', lambda d: True),  # Assume used if enabled
        ('rollup', 'Rollups', lambda d: True),
        ('ilm', 'Index Lifecycle Management', lambda d: d.get('policy_count', 0) > 0),
        ('slm', 'Snapshot Lifecycle', lambda d: True),
        ('sql', 'SQL', lambda d: d.get('queries', {}).get('_all', {}).get('total', 0) > 0),
        ('eql', 'Event Query Language', lambda d: d.get('queries', {}).get('_all', {}).get('total', 0) > 0),
        ('frozen_indices', 'Frozen Indices', lambda d: d.get('indices_count', 0) > 0),
        ('data_streams', 'Data Streams', lambda d: d.get('data_streams', 0) > 0),
        ('enrich', 'Enrich Policies', lambda d: True),
        ('vectors', 'Vector Search', lambda d: d.get('dense_vector_fields_count', 0) > 0),
    ]

    licensed_count = 0
    enabled_count = 0
    used_count = 0
    unavailable_count = 0

    for feature_key, feature_name, is_used_fn in feature_checks:
        feature_data = xpack_data.get(feature_key, {})

        available = feature_data.get('available', False)
        enabled = feature_data.get('enabled', False)

        try:
            in_use = is_used_fn(feature_data) if available else False
        except:
            in_use = False

        # Count
        if available:
            licensed_count += 1
            if enabled:
                enabled_count += 1
            if in_use:
                used_count += 1
        else:
            unavailable_count += 1

        # Build details string
        details = ""
        if feature_key == 'ml':
            jobs = feature_data.get('jobs', {}).get('_all', {}).get('count', 0)
            details = f"{jobs} jobs"
        elif feature_key == 'ccr':
            followers = feature_data.get('follower_indices_count', 0)
            details = f"{followers} followers"
        elif feature_key == 'watcher':
            watches = feature_data.get('count', {}).get('total', 0)
            details = f"{watches} watches"
        elif feature_key == 'ilm':
            policies = feature_data.get('policy_count', 0)
            details = f"{policies} policies"

        features.append({
            'name': feature_name,
            'key': feature_key,
            'available': available,
            'enabled': enabled,
            'in_use': in_use,
            'details': details
        })

    # Identify opportunities
    ml = xpack_data.get('ml', {})
    if not ml.get('available') and ml.get('enabled'):
        opportunities.append({
            'type': 'upsell',
            'title': 'Machine Learning License Upgrade',
            'description': 'ML is enabled but not available with current license. '
                          'Upgrade to Platinum/Enterprise for anomaly detection and predictive analytics.',
            'feature': 'ml'
        })

    if ml.get('available') and ml.get('jobs', {}).get('_all', {}).get('count', 0) == 0:
        opportunities.append({
            'type': 'unused',
            'title': 'Machine Learning Not Utilized',
            'description': 'ML is licensed but no jobs are configured. '
                          'Consider anomaly detection for logs, metrics, or security analytics.',
            'feature': 'ml'
        })

    ccr = xpack_data.get('ccr', {})
    if not ccr.get('available') and ccr.get('enabled'):
        opportunities.append({
            'type': 'upsell',
            'title': 'Cross-Cluster Replication License',
            'description': 'CCR is enabled but not available. Upgrade license for multi-datacenter replication.',
            'feature': 'ccr'
        })

    watcher = xpack_data.get('watcher', {})
    if watcher.get('available') and watcher.get('count', {}).get('total', 0) == 0:
        opportunities.append({
            'type': 'unused',
            'title': 'Alerting Not Configured',
            'description': 'Watcher is available but no alerts are configured. '
                          'Set up alerts for proactive monitoring.',
            'feature': 'watcher'
        })

    graph = xpack_data.get('graph', {})
    if not graph.get('available') and graph.get('enabled'):
        opportunities.append({
            'type': 'upsell',
            'title': 'Graph Analytics License',
            'description': 'Graph exploration requires Platinum license for relationship analysis.',
            'feature': 'graph'
        })

    security = xpack_data.get('security', {})
    if security.get('available') and not security.get('audit', {}).get('enabled'):
        opportunities.append({
            'type': 'unused',
            'title': 'Security Audit Logging Disabled',
            'description': 'Audit logging is available but not enabled. Enable for compliance and forensics.',
            'feature': 'security_audit'
        })

    unused_licensed_count = licensed_count - used_count

    return {
        'features': features,
        'opportunities': opportunities,
        'licensed_count': licensed_count,
        'enabled_count': enabled_count,
        'used_count': used_count,
        'unused_licensed_count': unused_licensed_count,
        'unavailable_count': unavailable_count,
    }
