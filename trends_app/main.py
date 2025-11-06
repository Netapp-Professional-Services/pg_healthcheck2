"""
Defines the main Flask blueprint for the web application.

This module contains the primary routes for the application, including the
dashboard, all API endpoints for the frontend to fetch data and perform
actions, and server-side report generation logic.
"""

import io
import json
import os
import subprocess
import tempfile
import psycopg2

from datetime import datetime

from deepdiff import DeepDiff
from flask import (Blueprint, render_template, request, jsonify, current_app,
                   redirect, url_for, abort, send_file, Response)
from flask_login import login_required, current_user


from .database import (get_unique_targets, load_user_preferences,
                       fetch_runs_by_ids, save_user_preference,
                       fetch_template_asset)

from .utils import load_trends_config, format_path
from .ai_connector import get_ai_recommendation
from .prompt_generator import generate_web_prompt, generate_slides_prompt

bp = Blueprint('main', __name__)

@bp.before_request
def before_request_callback():
    """Redirects authenticated users to change their password if required.

    This function runs before every request. If the currently logged-in user
    has the `password_change_required` flag set, it redirects them to the
    password change page, preventing access to other parts of the application.
    """

    if current_user.is_authenticated and current_user.password_change_required:
        if request.endpoint and 'static' not in request.endpoint and 'auth.' not in request.endpoint:
             return redirect(url_for('auth.change_password'))

@bp.app_template_filter('format_path')
def jinja_format_path(path):
    """Exposes the `format_path` utility function to Jinja2 templates."""
    return format_path(path)

@bp.route('/')
@login_required
def dashboard():
    """Renders the main dashboard page.

    This route fetches all necessary data for the dashboard, including a list
    of unique targets for filtering and user preferences. If `run1` and `run2`
    query parameters are provided, it fetches those two specific health check
    runs and performs a `DeepDiff` to identify and display changes between them.

    Args:
        run1 (int, optional): The ID of the first run to compare, passed as a
            URL query parameter.
        run2 (int, optional): The ID of the second run to compare, passed as a
            URL query parameter.

    Returns:
        A rendered HTML template of the dashboard.
    """

    config = load_trends_config()
    db_settings = config.get('database')
    
    run_id_1 = request.args.get('run1', type=int)
    run_id_2 = request.args.get('run2', type=int)
    
    runs = []
    accessible_company_ids = [c['id'] for c in current_user.accessible_companies]

    if run_id_1 and run_id_2:
        runs = fetch_runs_by_ids(db_settings, [run_id_1, run_id_2], accessible_company_ids)
    
    changes_by_check = {}
    if len(runs) >= 2:
        findings1 = runs[0].get('findings', {})
        findings2 = runs[1].get('findings', {})
        if isinstance(findings1, dict) and isinstance(findings2, dict):
            diff = DeepDiff(findings2, findings1, ignore_order=True, view='tree')
            if diff:
                for change_type, items in diff.items():
                    for item in items:
                        top_level_check = item.path(output_format='list')[0]
                        if top_level_check not in changes_by_check:
                            changes_by_check[top_level_check] = []
                        
                        path_str = format_path(item.path())
                        summary = ""
                        if change_type == 'values_changed':
                            summary = f"🔄 Value at `{path_str}` changed from **'{item.t1}'** to **'{item.t2}'**"
                        elif change_type == 'iterable_item_added':
                            summary = f"➕ Item added to `{path_str}`: `{json.dumps(item.t2)}`"
                        elif change_type == 'iterable_item_removed':
                            summary = f"➖ Item removed from `{path_str}`: `{json.dumps(item.t1)}`"
                        
                        if summary:
                            changes_by_check[top_level_check].append(summary)

    unique_targets = get_unique_targets(db_settings, accessible_company_ids)
    user_preferences = load_user_preferences(db_settings, current_user.username)

    return render_template('dashboard.html', 
                           runs=runs, 
                           changes_by_check=changes_by_check,
                           unique_targets=unique_targets,
                           user_preferences=json.dumps(user_preferences))

# --- API Routes ---

@bp.route('/api/runs')
@login_required
def get_all_runs():
    """API endpoint to fetch all accessible health check runs.

    This endpoint returns a list of all runs the current user has permission
    to view. It supports filtering by target system and by a date range. It
    also indicates whether each run has been marked as a "favorite" by the user.

    Args:
        target (str, optional): A filter string in the format
            'company:host:port:dbname'.
        start_time (str, optional): A start date string (e.g., 'YYYY-MM-DD').
        end_time (str, optional): An end date string (e.g., 'YYYY-MM-DD').

    Returns:
        JSON: A JSON array of run objects.
    """

    config = load_trends_config()
    db_settings = config.get('database')
    
    accessible_company_ids = [c['id'] for c in current_user.accessible_companies]
    if not accessible_company_ids:
        return jsonify([])

    target_filter = request.args.get('target')
    start_time_str = request.args.get('start_time')
    end_time_str = request.args.get('end_time')

    conn = None
    all_runs = []
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()

        # Parse target filter if provided
        company_name = None
        target_host = None
        target_port = None
        target_db_name = None
        if target_filter and target_filter != 'all':
            try:
                company_name, target_host, port_str, target_db_name = target_filter.split(':', 3)
                target_port = int(port_str)
            except (ValueError, IndexError) as e:
                current_app.logger.warning(f"Invalid target filter format: {target_filter}. Error: {e}")

        # Use stored procedure for analytics abstraction
        query = "SELECT get_health_check_runs(%s, %s, %s, %s, %s, %s, %s, %s);"
        cursor.execute(query, (
            accessible_company_ids,
            current_user.id,
            company_name,
            target_host,
            target_port,
            target_db_name,
            start_time_str,
            end_time_str
        ))
        result = cursor.fetchone()[0]

        # Parse JSONB result and format for API response
        if result:
            for run_obj in result:
                all_runs.append({
                    "id": run_obj['id'],
                    "timestamp": run_obj['run_timestamp'],
                    "target": f"{run_obj['target_host']}:{run_obj['target_port']} ({run_obj['target_db_name']})",
                    "is_favorite": run_obj['is_favorite']
                })
    except psycopg2.Error as e:
        current_app.logger.error(f"Database error fetching all runs with filters: {e}")
    finally:
        if conn: conn.close()
    return jsonify(all_runs)

@bp.route('/api/runs/toggle-favorite', methods=['POST'])
@login_required
def toggle_favorite():
    """API endpoint to add or remove a run from the user's favorites."""
    data = request.get_json()
    run_id = data.get('run_id')

    if not run_id:
        return jsonify({'status': 'error', 'message': 'run_id is required.'}), 400
    
    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()

        cursor.execute("SELECT 1 FROM user_favorite_runs WHERE user_id = %s AND run_id = %s;", (current_user.id, run_id))
        exists = cursor.fetchone()

        if exists:
            cursor.execute("DELETE FROM user_favorite_runs WHERE user_id = %s AND run_id = %s;", (current_user.id, run_id))
            new_status = False
        else:
            cursor.execute("INSERT INTO user_favorite_runs (user_id, run_id) VALUES (%s, %s);", (current_user.id, run_id))
            new_status = True
        
        conn.commit()
        return jsonify({'status': 'success', 'is_favorite': new_status})

    except psycopg2.Error as e:
        if conn: conn.rollback()
        current_app.logger.error(f"Database error toggling favorite: {e}")
        return jsonify({'status': 'error', 'message': 'Database error.'}), 500
    finally:
        if conn: conn.close()

@bp.route('/api/save-preference', methods=['POST'])
@login_required
def save_preference():
    """API endpoint to save a user-specific preference.

    Accepts a JSON object with 'name' and 'value' keys to store a key-value
    preference in the database for the current user.
    """

    data = request.get_json()
    pref_name = data.get('name')
    pref_value = data.get('value')

    if not pref_name or pref_value is None:
        return jsonify({'status': 'error', 'message': 'Missing preference name or value'}), 400

    config = load_trends_config()
    db_settings = config.get('database')
    save_user_preference(db_settings, current_user.username, pref_name, pref_value)
    
    return jsonify({'status': 'success', 'message': f'Preference {pref_name} saved.'})

@bp.route('/api/user-ai-profiles')
@login_required
def get_user_ai_profiles():
    """API endpoint to fetch all AI profiles for the current user."""
    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    profiles = []
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, profile_name FROM user_ai_profiles WHERE user_id = %s ORDER BY profile_name;",
            (current_user.id,)
        )
        for row in cursor.fetchall():
            profiles.append({'id': row[0], 'name': row[1]})
    except psycopg2.Error as e:
        current_app.logger.error(f"Database error fetching AI profiles: {e}")
        return jsonify({"error": "Could not fetch AI profiles."}), 500
    finally:
        if conn: conn.close()
    return jsonify(profiles)

@bp.route('/api/analysis-rules')
@login_required
def get_analysis_rules():
    """API endpoint to fetch all available analysis rule sets."""
    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    rules = []
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        cursor.execute("SELECT id, rule_set_name FROM analysis_rules ORDER BY rule_set_name;")
        for row in cursor.fetchall():
            rules.append({'id': row[0], 'name': row[1]})
    except psycopg2.Error as e:
        current_app.logger.error(f"Database error fetching analysis rules: {e}")
        return jsonify({"error": "Could not fetch analysis rules."}), 500
    finally:
        if conn: conn.close()
    return jsonify(rules)

@bp.route('/api/prompt-templates')
@login_required
def get_prompt_templates():
    """API endpoint to fetch prompt templates available to the user.

    This includes global templates and custom templates created by the user.
    """
    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    templates = []
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, template_name FROM prompt_templates WHERE user_id IS NULL OR user_id = %s ORDER BY template_name;",
            (current_user.id,)
        )
        for row in cursor.fetchall():
            templates.append({'id': row[0], 'name': row[1]})
    except psycopg2.Error as e:
        current_app.logger.error(f"Database error fetching prompt templates: {e}")
        return jsonify({"error": "Could not fetch prompt templates."}), 500
    finally:
        if conn: conn.close()
    return jsonify(templates)

@bp.route('/api/generate-ai-report', methods=['POST'])
@login_required
def generate_ai_report():
    """API endpoint to generate an on-demand AI report.

    This is a privileged action. It takes the IDs for a run, AI profile,
    template, and rule set, generates a prompt, gets the AI recommendation,
    saves the encrypted report to the database, and returns the report
    as a downloadable file.
    """

    if not current_user.has_privilege('GenerateReports'):
        abort(403)

    data = request.get_json()
    run_id, profile_id, template_id, rule_set_id = data.get('run_id'), data.get('profile_id'), data.get('template_id'), data.get('rule_set_id')
    report_name, report_description = data.get('report_name'), data.get('report_description')

    if not all([run_id, profile_id, template_id, rule_set_id]):
        return jsonify({"error": "Missing required parameters."}), 400

    config = load_trends_config()
    db_settings = config.get('database')
    accessible_company_ids = [c['id'] for c in current_user.accessible_companies]
    runs_data = fetch_runs_by_ids(db_settings, [run_id], accessible_company_ids)
    if not runs_data:
        return jsonify({"error": "Run not found or permission denied."}), 404
    findings_json = runs_data[0].get('findings')
    if not isinstance(findings_json, dict):
        return jsonify({"error": "Invalid findings data."}), 500

    prompt = generate_web_prompt(findings_json, rule_set_id, template_id)
    if prompt.startswith("Error:"): return jsonify({"error": prompt}), 500
    ai_response = get_ai_recommendation(prompt, profile_id)
    if ai_response.startswith("Error:"): return jsonify({"error": ai_response}), 500

    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO generated_ai_reports (run_id, rule_set_id, ai_profile_id, generated_by_user_id, report_name, report_description, template_id, report_content) VALUES (%s, %s, %s, %s, %s, %s, %s, pgp_sym_encrypt(%s, get_encryption_key()));",
            (run_id, rule_set_id, profile_id, current_user.id, report_name, report_description, template_id, ai_response)
        )
        conn.commit()
    except psycopg2.Error as e:
        if conn: conn.rollback()
        current_app.logger.error(f"DB error saving report: {e}")
        return jsonify({"error": "DB error saving report."}), 500
    finally:
        if conn: conn.close()

    return send_file(io.BytesIO(ai_response.encode('utf-8')), mimetype='text/plain', as_attachment=True, download_name=f"ai_report_run_{run_id}.adoc")

# --- FINAL CORRECTED ROUTE ---
@bp.route('/generate-slides')
@login_required
def generate_slides():

    """Generates an HTML slide presentation from a health check run.

    This is a privileged action that orchestrates a complex workflow:
    1. Fetches health check findings from the database.
    2. Generates a markdown-based prompt for an AI to create slide content.
    3. Fetches template assets (backgrounds, logos) from the database.
    4. Post-processes the AI's markdown response to inject Marp directives
       for theming and layout.
    5. Calls the external `marp-cli` tool to convert the final markdown into
       a standalone HTML slide presentation.
    6. Renders the resulting HTML in a viewer template.
    """

    if not current_user.has_privilege('GenerateReports'):
        abort(403)

    run_id = request.args.get('run_id', type=int)
    profile_id = request.args.get('profile_id', type=int)
    rule_set_id = request.args.get('rule_set_id', type=int)
    template_id = request.args.get('template_id', type=int)

    if not all([run_id, profile_id, rule_set_id, template_id]):
        return render_template('error.html', error_message="Missing required parameters."), 400

    config = load_trends_config()
    db_settings = config.get('database')
    
    accessible_company_ids = [c['id'] for c in current_user.accessible_companies]
    runs_data = fetch_runs_by_ids(db_settings, [run_id], accessible_company_ids)
    
    if not runs_data:
        return render_template('error.html', error_message="Run not found or permission denied."), 404
    
    findings_json = runs_data[0].get('findings')
    if not isinstance(findings_json, dict):
        return render_template('error.html', error_message="Invalid findings data."), 500

    temp_files = {}
    md_file_path = None
    html_output_path = None
    try:
        # Step 1: Get AI content
        prompt = generate_slides_prompt(findings_json, rule_set_id, template_id, assets={})
        if prompt.startswith("Error:"):
            return render_template('error.html', error_message=prompt), 500

        ai_content_md = get_ai_recommendation(prompt, profile_id)
        if ai_content_md.startswith("Error:"):
            return render_template('error.html', error_message=ai_content_md), 500

        # Step 2: Prepare temporary image files
        bg_path = None
        logo_path = None

        background_data = fetch_template_asset(db_settings, 'slide_background')
        if background_data:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.png') as temp_bg:
                temp_bg.write(background_data)
                bg_path = temp_bg.name
                temp_files['slide_background'] = bg_path
        
        logo_data = fetch_template_asset(db_settings, 'company_logo')
        if logo_data:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.png') as temp_logo:
                temp_logo.write(logo_data)
                logo_path = temp_logo.name
                temp_files['company_logo'] = logo_path

        # Step 3: Post-process the AI's markdown to inject directives
        slides = [s for s in ai_content_md.split('---') if s.strip()]
        processed_slides = []

        for i, slide_content in enumerate(slides):
            content = slide_content.strip()
            
            if i == 0:
                title_slide = "---"
                if bg_path:
                    # Correctly add the Marp directive for the background image
                    title_slide += f"\n"
                title_slide += "\nclass: center, middle"
                title_slide += f"\n{content}"
                if logo_path:
                    # Add the standard Markdown for the logo
                    title_slide += f'\n![Logo]({logo_path})'
                processed_slides.append(title_slide)
            else:
                other_slide = "---"
                if bg_path:
                    # Correctly add the Marp directive for all other slides
                    other_slide += f"\n"
                other_slide += f"\n{content}"
                processed_slides.append(other_slide)

        final_markdown = '\n'.join(processed_slides)

        # Step 4: Convert the final markdown to HTML
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.md') as md_file:
            md_file.write(final_markdown)
            md_file_path = md_file.name
        
        html_output_path = md_file_path.replace('.md', '.html')
        
        subprocess.run(['marp', md_file_path, '-o', html_output_path], check=True)

        with open(html_output_path, 'r') as html_file:
            slides_html = html_file.read()
        
        return render_template('profile/view_slides.html', slides_html=slides_html)

    except FileNotFoundError:
        return render_template('error.html', error_message="`marp-cli` not found. Please ensure it is installed and in your system's PATH."), 500
    except subprocess.CalledProcessError as e:
        return render_template('error.html', error_message=f"Error running marp-cli: {e.stderr}"), 500
    except Exception as e:
        current_app.logger.error(f"An unexpected error occurred during slide generation: {e}")
        return render_template('error.html', error_message="An unexpected error occurred during slide generation."), 500
    finally:
        # Step 5: Re-enabled cleanup logic
        for f in temp_files.values():
            if os.path.exists(f):
                os.remove(f)
        if md_file_path and os.path.exists(md_file_path):
            os.remove(md_file_path)
        if html_output_path and os.path.exists(html_output_path):
            os.remove(html_output_path)


@bp.route('/api/all-reports')
@login_required
def get_all_reports():
    """API endpoint to fetch the user's complete report history.

    This includes both AI-generated reports and manually uploaded reports,
    joining across multiple tables to provide comprehensive metadata for
    each report in the user's history.
    """

    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()

        # Call the stored function get_all_reports(user_id)
        # Returns JSONB array of report objects
        cursor.execute("SELECT get_all_reports(%s)", (current_user.id,))
        result = cursor.fetchone()

        # The stored function returns JSONB, parse it
        reports_json = result[0] if result else []

        # Convert timestamps from ISO format strings if they're not already
        # The JSONB from the function should already have properly formatted data
        return jsonify(reports_json)
    except psycopg2.Error as e:
        current_app.logger.error(f"DB error fetching all reports: {e}")
        return jsonify({"error": "Could not fetch report history."}), 500
    finally:
        if conn: conn.close()

@bp.route('/api/download-report/<string:report_type>/<int:report_id>')
@login_required
def download_report(report_type, report_id):
    """API endpoint to download a report.

    Fetches the report content from the appropriate table based on type,
    decrypts it if necessary, and returns it as a downloadable AsciiDoc file.
    """

    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()

        if report_type in ['generated', 'trend_analysis']:
            # Fetch from generated_ai_reports
            cursor.execute(
                "SELECT pgp_sym_decrypt(report_content::bytea, get_encryption_key()), run_id, report_name FROM generated_ai_reports WHERE id = %s AND generated_by_user_id = %s;",
                (report_id, current_user.id)
            )
            report_data = cursor.fetchone()
            if not report_data:
                abort(404, "Report not found or permission denied.")
            report_content, run_id, report_name = report_data
            filename = f"{report_name or f'ai_report_run_{run_id}'}.adoc"
        elif report_type == 'health_check':
            # Fetch from health_check_runs - verify user access via company
            cursor.execute(
                """
                SELECT hcr.report_adoc, hcr.prompt_template_name, hcr.target_host
                FROM health_check_runs hcr
                JOIN companies c ON hcr.company_id = c.id
                JOIN user_company_access uca ON c.id = uca.company_id
                WHERE hcr.id = %s
                  AND uca.user_id = %s
                  AND hcr.report_adoc IS NOT NULL;
                """,
                (report_id, current_user.id)
            )
            report_data = cursor.fetchone()
            if not report_data:
                abort(404, "Report not found or permission denied.")
            report_content, template_name, target_host = report_data
            filename = f"health_check_{target_host}_{template_name}_{report_id}.adoc"
        else:
            abort(404, "Invalid report type for download.")

        return send_file(io.BytesIO(report_content.encode('utf-8')), mimetype='text/plain', as_attachment=True, download_name=filename)
    except psycopg2.Error as e:
        current_app.logger.error(f"DB error downloading report: {e}")
        abort(500, "DB error occurred.")
    finally:
        if conn: conn.close()

@bp.route('/api/generated-reports/<int:report_id>', methods=['PUT'])
@login_required
def update_report_metadata(report_id):
    """API endpoint to update the metadata of a generated report.

    Allows the user to change the name, description, and annotations
    of a report they have previously generated.
    """


    data = request.get_json()
    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE generated_ai_reports SET report_name = %s, report_description = %s, annotations = %s WHERE id = %s AND generated_by_user_id = %s;",
            (data.get('report_name'), data.get('report_description'), data.get('annotations'), report_id, current_user.id)
        )
        conn.commit()
        if cursor.rowcount == 0:
            return jsonify({"error": "Report not found or permission denied."}), 404
        return jsonify({"status": "success"})
    except psycopg2.Error as e:
        if conn: conn.rollback()
        current_app.logger.error(f"DB error updating report metadata: {e}")
        return jsonify({"error": "DB error occurred."}), 500
    finally:
        if conn: conn.close()


# ============================================================================
# TREND ANALYSIS ROUTES
# ============================================================================

@bp.route('/trend-analysis')
@login_required
def trend_analysis_page():
    """Trend analysis page."""
    if not current_user.has_privilege('ViewTrendAnalysis'):
        abort(403)
    
    config = load_trends_config()
    db_settings = config.get('database')
    accessible_company_ids = [c['id'] for c in current_user.accessible_companies]
    
    from . import trends_analysis
    recent_analyses = trends_analysis.get_trend_analyses_history(db_settings, accessible_company_ids)
    
    return render_template(
        'trend_analysis.html',
        recent_analyses=recent_analyses
    )


@bp.route('/api/trend-analysis/companies')
@login_required
def get_companies_for_trend_analysis():
    """API endpoint to get accessible companies."""
    if not current_user.has_privilege('ViewTrendAnalysis'):
        abort(403)
    
    config = load_trends_config()
    db_settings = config.get('database')
    accessible_company_ids = [c['id'] for c in current_user.accessible_companies]
    
    from . import trends_analysis
    companies = trends_analysis.get_accessible_companies_list(db_settings, accessible_company_ids)
    return jsonify(companies)


@bp.route('/api/trend-analysis/templates')
@login_required
def get_trend_analysis_templates():
    """API endpoint to get trend analysis templates only."""
    if not current_user.has_privilege('ViewTrendAnalysis'):
        abort(403)
    
    config = load_trends_config()
    db_settings = config.get('database')
    
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        
        query = """
            SELECT id, template_name as name
            FROM prompt_templates
            WHERE technology = 'trend_analysis'
            ORDER BY template_name;
        """
        cursor.execute(query)
        templates = [{"id": row[0], "name": row[1]} for row in cursor.fetchall()]
        return jsonify(templates)
        
    except psycopg2.Error as e:
        current_app.logger.error(f"Error fetching trend templates: {e}")
        return jsonify([]), 500
    finally:
        if conn:
            conn.close()

@bp.route('/api/trend-analysis/preview', methods=['POST'])
@login_required
def get_trend_preview():
    """API endpoint to preview trend data before AI generation."""
    if not current_user.has_privilege('ViewTrendAnalysis'):
        abort(403)
    
    data = request.get_json()
    company_id = data.get('company_id')
    days = data.get('days')
    
    if not company_id or not days:
        return jsonify({'error': 'Missing required parameters'}), 400
    
    config = load_trends_config()
    db_settings = config.get('database')
    accessible_company_ids = [c['id'] for c in current_user.accessible_companies]
    
    from . import trends_analysis
    trend_data = trends_analysis.get_trend_data(
        db_config=db_settings,
        company_id=company_id,
        days=days,
        accessible_company_ids=accessible_company_ids
    )
    
    if not trend_data:
        return jsonify({'error': 'No trend data available or access denied'}), 403
    
    return jsonify(trend_data)


@bp.route('/api/generate-trend-analysis', methods=['POST'])
@login_required
def generate_trend_analysis_api():
    """Generate trend analysis."""
    if not current_user.has_privilege('ViewTrendAnalysis'):
        abort(403)
    
    data = request.get_json()
    company_id = data.get('company_id')
    days = data.get('days', 90)
    profile_id = data.get('profile_id')
    template_id = data.get('template_id')
    
    if not all([company_id, profile_id, template_id]):
        return jsonify({"error": "Missing required parameters"}), 400
    
    config = load_trends_config()
    db_settings = config.get('database')
    accessible_company_ids = [c['id'] for c in current_user.accessible_companies]
    
    from . import trends_analysis
    success, result = trends_analysis.generate_trend_analysis(
        db_config=db_settings,
        company_id=company_id,
        days=days,
        profile_id=profile_id,
        template_id=template_id,
        accessible_company_ids=accessible_company_ids,
        user_id=current_user.id
    )
    
    if success:
        return jsonify({
            "status": "success",
            "analysis_id": result['analysis_id'],
            "view_url": url_for('profile.view_report', report_type='trend_analysis', report_id=result['analysis_id'])
        })
    else:
        return jsonify({"error": result}), 500
