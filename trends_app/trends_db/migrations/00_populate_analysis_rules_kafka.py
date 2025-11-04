import sys
import json
from pathlib import Path
import psycopg2
import yaml

# --- Setup Project Path ---
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# --- Dynamically load the plugin to get the rules ---
try:
    from plugins.kafka import KafkaPlugin
except ImportError as e:
    print("Error: Could not import the KafkaPlugin. Make sure this script is in the correct location.")
    print(f"Details: {e}")
    sys.exit(1)

def load_db_config():
    """Loads database configuration from the main trends.yaml file."""
    config_path = project_root / 'config' / 'trends.yaml'
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            return config.get('database')
    except FileNotFoundError:
        print(f"Error: Configuration file not found at {config_path}")
        return None
    except Exception as e:
        print(f"Error reading configuration: {e}")
        return None

def main():
    """
    Connects to the database and inserts the dynamically loaded analysis rules.
    """
    db_config = load_db_config()
    if not db_config:
        sys.exit(1)

    # --- Use the plugin to get the assembled rules ---
    print("Loading rules via the Kafka plugin...")
    technology_plugin = KafkaPlugin()
    metric_analysis_config = technology_plugin.get_rules_config()
    
    if not metric_analysis_config:
        print("Error: No rules were loaded by the plugin. Check the rules directory and files.")
        sys.exit(1)
        
    print(f"Successfully loaded {len(metric_analysis_config)} rule configurations.")

    # --- Define the Rule Set to be Inserted ---
    rule_set_name = "Default Kafka Rules"
    technology = "kafka"
    rules_json = json.dumps(metric_analysis_config) # Convert the Python dict to a JSON string

    conn = None
    try:
        print("Connecting to the health_trends database...")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Check if this rule set already exists
        cursor.execute("SELECT id FROM analysis_rules WHERE rule_set_name = %s;", (rule_set_name,))
        if cursor.fetchone():
            print(f"'{rule_set_name}' already exists. To update, please remove the existing entry from the table.")
            return

        # Insert the new rule set
        print(f"Inserting '{rule_set_name}' into the analysis_rules table...")
        insert_query = """
            INSERT INTO analysis_rules (rule_set_name, technology, rules_json)
            VALUES (%s, %s, %s);
        """
        cursor.execute(insert_query, (rule_set_name, technology, rules_json))
        
        conn.commit()
        print("✅ Successfully populated analysis_rules table.")

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
