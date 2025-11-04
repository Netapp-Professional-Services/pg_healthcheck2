import os
import json
import psycopg2
from flask import Blueprint, render_template, request, jsonify, current_app, redirect, url_for, flash, abort
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename
from .utils import load_trends_config

bp = Blueprint('profile', __name__, url_prefix='/profile')

# --- REPORT UPLOAD LOGIC ---

ALLOWED_EXTENSIONS = {'adoc', 'asciidoc'}

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@bp.route('/upload-report', methods=['GET', 'POST'])
@login_required
def upload_report_form():
    """Renders the page for uploading a new AsciiDoc report and handles the upload."""
    if not current_user.has_privilege('UploadReports'):
        abort(403)

    if request.method == 'POST':
        # Check if the post request has the file part
        if 'report_file' not in request.files:
            flash('No file part in the request.', 'danger')
            return redirect(request.url)

        file = request.files['report_file']
        report_name = request.form.get('report_name')
        report_description = request.form.get('report_description')

        # If the user does not select a file, the browser submits an empty file without a filename.
        if file.filename == '':
            flash('No selected file.', 'danger')
            return redirect(request.url)

        if not report_name:
            flash('Report Name is a required field.', 'danger')
            return redirect(request.url)

        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            content = file.read().decode('utf-8')

            config = load_trends_config()
            db_settings = config.get('database')
            conn = None
            try:
                conn = psycopg2.connect(**db_settings)
                cursor = conn.cursor()

                # Encrypt the content and insert into the new table
                cursor.execute(
                    """
                    INSERT INTO uploaded_reports (
                        uploaded_by_user_id, report_name, report_description,
                        encrypted_report_content, original_filename
                    ) VALUES (
                        %s, %s, %s,
                        pgp_sym_encrypt(%s, get_encryption_key()),
                        %s
                    );
                    """,
                    (current_user.id, report_name, report_description, content, filename)
                )
                conn.commit()
                flash(f"Report '{report_name}' uploaded successfully!", "success")
                return redirect(url_for('profile.report_history'))

            except psycopg2.Error as e:
                if conn:
                    conn.rollback()
                flash(f"Database error: {e}", "danger")
                current_app.logger.error(f"DB error uploading report: {e}")
            finally:
                if conn:
                    conn.close()
        else:
            flash('Invalid file type. Please upload a .adoc or .asciidoc file.', 'danger')

    # For GET requests, just render the form
    return render_template('profile/upload_report.html')


# --- AI PROFILE AND OTHER ROUTES ---

@bp.route('/ai-settings', methods=['GET'])
@login_required
def ai_settings():
    """Displays the user's AI profiles and the form to create new ones."""
    if not current_user.has_privilege('ManageAIProfiles'):
        abort(403)

    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    profiles = []
    providers = []
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()

        # Fetch user's existing profiles with model name
        cursor.execute(
            """
            SELECT up.id, up.profile_name, p.provider_name, 
                   COALESCE(up.model_name, p.api_model) as model,
                   up.temperature, up.max_output_tokens
            FROM user_ai_profiles up
            JOIN ai_providers p ON up.provider_id = p.id
            WHERE up.user_id = %s ORDER BY up.profile_name;
            """,
            (current_user.id,)
        )
        for row in cursor.fetchall():
            profiles.append({
                "id": row[0], "name": row[1], "provider": row[2],
                "model": row[3], "temp": row[4], "tokens": row[5]
            })

        # Fetch available AI providers for the dropdown
        cursor.execute("SELECT id, provider_name FROM ai_providers WHERE is_active = true ORDER BY provider_name;")
        for row in cursor.fetchall():
            providers.append({"id": row[0], "name": row[1]})

    except psycopg2.Error as e:
        flash("Database error while loading AI settings.", "danger")
        current_app.logger.error(f"DB error on AI settings page: {e}")
    finally:
        if conn:
            conn.close()

    return render_template('profile/ai_settings.html', profiles=profiles, providers=providers)

@bp.route('/ai-settings/create', methods=['POST'])
@login_required
def create_ai_profile():
    """Handles the creation of a new AI profile with model selection."""
    if not current_user.has_privilege('ManageAIProfiles'):
        abort(403)

    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()

        profile_name = request.form.get('profile_name')
        provider_id = request.form.get('provider_id', type=int)
        model_name = request.form.get('model_name')
        temperature = request.form.get('temperature', 0.7, type=float)
        max_tokens = request.form.get('max_tokens', 2048, type=int)
        user_api_key = request.form.get('user_api_key', '')
        proxy_username = request.form.get('proxy_username', '')

        if not profile_name or not provider_id or not model_name:
            flash("Profile Name, Provider, and Model are required.", "danger")
            return redirect(url_for('profile.ai_settings'))

        encrypted_key = None
        if user_api_key:
            cursor.execute("SELECT pgp_sym_encrypt(%s, get_encryption_key());", (user_api_key,))
            encrypted_key = cursor.fetchone()[0]

        # Store model_name in the profile
        cursor.execute("""
            INSERT INTO user_ai_profiles (
                user_id, profile_name, provider_id, model_name, temperature, max_output_tokens,
                encrypted_user_api_key, proxy_username
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, (current_user.id, profile_name, provider_id, model_name, temperature, max_tokens, encrypted_key, proxy_username))

        conn.commit()
        flash(f"AI Profile '{profile_name}' created successfully.", "success")

    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash(f"Database error: {e}", "danger")
        current_app.logger.error(f"DB error creating AI profile: {e}")
    finally:
        if conn: conn.close()

    return redirect(url_for('profile.ai_settings'))

@bp.route('/ai-settings/get/<int:profile_id>', methods=['GET'])
@login_required
def get_ai_profile(profile_id):
    """Fetches data for a single AI profile to populate the edit modal."""
    if not current_user.has_privilege('ManageAIProfiles'):
        abort(403)

    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT up.id, up.profile_name, up.provider_id, up.temperature, 
                   up.max_output_tokens, up.proxy_username, up.model_name
            FROM user_ai_profiles up
            WHERE up.id = %s AND up.user_id = %s;
            """,
            (profile_id, current_user.id)
        )
        profile = cursor.fetchone()
        if profile:
            return jsonify({
                "id": profile[0], 
                "profile_name": profile[1], 
                "provider_id": profile[2],
                "temperature": float(profile[3]), 
                "max_output_tokens": profile[4],
                "proxy_username": profile[5] or "",
                "model_name": profile[6] or ""
            })
        return jsonify({"error": "Profile not found"}), 404
    except psycopg2.Error as e:
        return jsonify({"error": "Database error"}), 500
    finally:
        if conn: conn.close()

@bp.route('/ai-settings/edit/<int:profile_id>', methods=['POST'])
@login_required
def edit_ai_profile(profile_id):
    """Handles updates to an existing AI profile."""
    if not current_user.has_privilege('ManageAIProfiles'):
        abort(403)

    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()

        profile_name = request.form.get('profile_name')
        provider_id = request.form.get('provider_id', type=int)
        model_name = request.form.get('model_name')
        temperature = request.form.get('temperature', type=float)
        max_tokens = request.form.get('max_tokens', type=int)
        user_api_key = request.form.get('user_api_key', '')
        proxy_username = request.form.get('proxy_username', '')

        if not profile_name or not provider_id or not model_name:
            flash("Profile Name, Provider, and Model are required.", "danger")
            return redirect(url_for('profile.ai_settings'))

        # Build the update query dynamically
        query_parts = ["UPDATE user_ai_profiles SET profile_name = %s, provider_id = %s, model_name = %s, temperature = %s, max_output_tokens = %s, proxy_username = %s"]
        params = [profile_name, provider_id, model_name, temperature, max_tokens, proxy_username]

        if user_api_key:
            cursor.execute("SELECT pgp_sym_encrypt(%s, get_encryption_key());", (user_api_key,))
            encrypted_key = cursor.fetchone()[0]
            query_parts.append(", encrypted_user_api_key = %s")
            params.append(encrypted_key)

        query_parts.append("WHERE id = %s AND user_id = %s;")
        params.extend([profile_id, current_user.id])

        cursor.execute(" ".join(query_parts), tuple(params))
        conn.commit()
        flash(f"AI Profile '{profile_name}' updated successfully.", "success")

    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash(f"Database error: {e}", "danger")
    finally:
        if conn: conn.close()

    return redirect(url_for('profile.ai_settings'))

@bp.route('/ai-settings/delete/<int:profile_id>', methods=['POST'])
@login_required
def delete_ai_profile(profile_id):
    """Deletes an AI profile owned by the current user."""
    if not current_user.has_privilege('ManageAIProfiles'):
        abort(403)

    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM user_ai_profiles WHERE id = %s AND user_id = %s;",
            (profile_id, current_user.id)
        )
        conn.commit()
        if cursor.rowcount > 0:
            flash("AI Profile deleted successfully.", "success")
        else:
            flash("Profile not found or you do not have permission to delete it.", "danger")
    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash("Database error while deleting profile.", "danger")
    finally:
        if conn: conn.close()

    return redirect(url_for('profile.ai_settings'))

@bp.route('/report-history')
@login_required
def report_history():
    """Renders the page that displays the user's generated report history."""
    return render_template('profile/report_history.html')

# --- MODIFIED: Unified route to view any type of report ---
@bp.route('/view-report/<string:report_type>/<int:report_id>')
@login_required
def view_report(report_type, report_id):
    """Displays a single report (generated or uploaded) in an online viewer."""
    config = load_trends_config()
    db_settings = config.get('database')
    conn = None

    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()

        if report_type in ['generated', 'trend_analysis']:
#        if report_type == 'generated':
            # Fetch from generated_ai_reports
            cursor.execute(
                """
                SELECT report_name, pgp_sym_decrypt(report_content::bytea, get_encryption_key())
                FROM generated_ai_reports
                WHERE id = %s AND generated_by_user_id = %s;
                """,
                (report_id, current_user.id)
            )

        elif report_type == 'trend_analysis':
            # Trend analysis reports
            cursor.execute(
                """
                SELECT report_name, pgp_sym_decrypt(report_content::bytea, get_encryption_key())
                FROM generated_ai_reports
                WHERE id = %s AND generated_by_user_id = %s AND report_type = 'trend_analysis';
                """,
                (report_id, current_user.id)
            )

        elif report_type == 'uploaded':
            # Fetch from uploaded_reports
            cursor.execute(
                """
                SELECT report_name, pgp_sym_decrypt(encrypted_report_content::bytea, get_encryption_key())
                FROM uploaded_reports
                WHERE id = %s AND uploaded_by_user_id = %s;
                """,
                (report_id, current_user.id)
            )
        else:
            abort(404, "Invalid report type specified.")

        report_data = cursor.fetchone()
        if not report_data:
            abort(404, "Report not found or you do not have permission to access it.")

        report_name, report_content = report_data
        return render_template('profile/view_report.html',
                               report_id=report_id,
                               report_name=report_name,
                               report_content=report_content,
                               report_type=report_type)
    except psycopg2.Error as e:
        current_app.logger.error(f"Database error viewing report: {e}")
        abort(500, "Database error occurred.")
    finally:
        if conn:
            conn.close()


@bp.route('/save-report/<string:report_type>/<int:report_id>', methods=['POST'])
@login_required
def save_report(report_type, report_id):
    """Saves an edited report back to the database with versioning."""
    if not current_user.has_privilege('EditReports'):
        return jsonify({"status": "error", "message": "Permission denied."}), 403

    data = request.get_json()
    new_content = data.get('content')
    change_summary = data.get('description', '')

    if new_content is None:
        return jsonify({"status": "error", "message": "No content provided."}), 400

    if report_type not in ['generated', 'uploaded', 'trend_analysis']:
        return jsonify({"status": "error", "message": "Invalid report type."}), 400

    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()

        # Determine the correct table and columns
        if report_type in ['generated', 'trend_analysis']:
            table_name = 'generated_ai_reports'
            content_column = 'report_content'
            user_column = 'generated_by_user_id'
            version_fk_column = 'generated_report_id'
        else:  # uploaded
            table_name = 'uploaded_reports'
            content_column = 'encrypted_report_content'
            user_column = 'uploaded_by_user_id'
            version_fk_column = 'uploaded_report_id'

        # Get current content
        cursor.execute(f"""
            SELECT {content_column}
            FROM {table_name}
            WHERE id = %s AND {user_column} = %s;
        """, (report_id, current_user.id))

        result = cursor.fetchone()
        if not result:
            return jsonify({"status": "error", "message": "Report not found or permission denied."}), 404

        current_content_encrypted = result[0]

        # Save current content as a version (version_number will be auto-set by trigger)
        if report_type in ['generated', 'trend_analysis']:  # CHANGE: handle both
            cursor.execute("""
                INSERT INTO report_versions 
                (generated_report_id, edited_by_user_id, encrypted_report_content, change_summary, auto_cleanup)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING version_number;
            """, (report_id, current_user.id, current_content_encrypted,
                  change_summary or 'Auto-save before edit', True))
        else:
            cursor.execute("""
                INSERT INTO report_versions 
                (uploaded_report_id, edited_by_user_id, encrypted_report_content, change_summary, auto_cleanup)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING version_number;
            """, (report_id, current_user.id, current_content_encrypted,
                  change_summary or 'Auto-save before edit', True))

        version_number = cursor.fetchone()[0]

        # Update the main table with new content
        cursor.execute(f"""
            UPDATE {table_name}
            SET {content_column} = pgp_sym_encrypt(%s, get_encryption_key())
            WHERE id = %s AND {user_column} = %s;
        """, (new_content, report_id, current_user.id))

        conn.commit()
        return jsonify({
            "status": "success", 
            "message": f"Report saved successfully. Version {version_number} created.",
            "version": version_number
        })

    except psycopg2.Error as e:
        if conn: conn.rollback()
        current_app.logger.error(f"Database error saving report: {e}")
        return jsonify({"status": "error", "message": "A database error occurred."}), 500
    finally:
        if conn: conn.close()


@bp.route('/report-versions/<string:report_type>/<int:report_id>')
@login_required
def get_report_versions(report_type, report_id):
    """Get version history for a report."""
    if report_type not in ['generated', 'uploaded', 'trend_analysis']:
        return jsonify({"error": "Invalid report type"}), 400

    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()

        # Verify user owns the report
        if report_type in ['generated', 'trend_analysis']:
            cursor.execute(
                "SELECT 1 FROM generated_ai_reports WHERE id = %s AND generated_by_user_id = %s;",
                (report_id, current_user.id)
            )
            fk_column = 'generated_report_id'
        else:
            cursor.execute(
                "SELECT 1 FROM uploaded_reports WHERE id = %s AND uploaded_by_user_id = %s;",
                (report_id, current_user.id)
            )
            fk_column = 'uploaded_report_id'

        if not cursor.fetchone():
            return jsonify({"error": "Report not found or permission denied"}), 404

        # Get version history
        cursor.execute(f"""
            SELECT rv.version_number, rv.version_timestamp, rv.change_summary, rv.is_pinned,
                   u.username
            FROM report_versions rv
            LEFT JOIN users u ON rv.edited_by_user_id = u.id
            WHERE rv.{fk_column} = %s
            ORDER BY rv.version_number DESC;
        """, (report_id,))

        versions = []
        for row in cursor.fetchall():
            versions.append({
                "version": row[0],
                "created_at": row[1].strftime('%Y-%m-%d %H:%M:%S'),
                "description": row[2] or "No description",
                "is_pinned": row[3],
                "username": row[4] or "Unknown"
            })

        return jsonify({"versions": versions})

    except psycopg2.Error as e:
        current_app.logger.error(f"Database error getting versions: {e}")
        return jsonify({"error": "Database error"}), 500
    finally:
        if conn: conn.close()

@bp.route('/restore-version/<string:report_type>/<int:report_id>/<int:version_number>', methods=['POST'])
@login_required
def restore_version(report_type, report_id, version_number):
    """Restore a report to a previous version."""
    if not current_user.has_privilege('EditReports'):
        return jsonify({"status": "error", "message": "Permission denied."}), 403

    if report_type not in ['generated', 'uploaded', 'trend_analysis']:
        return jsonify({"status": "error", "message": "Invalid report type."}), 400

    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()

        # Determine FK column
        if report_type in ['generated', 'trend_analysis']:  # CHANGE: handle both
            fk_column = 'generated_report_id'
            table_name = 'generated_ai_reports'
            content_column = 'report_content'
            user_column = 'generated_by_user_id'
        else:
            fk_column = 'uploaded_report_id'
            table_name = 'uploaded_reports'
            content_column = 'encrypted_report_content'
            user_column = 'uploaded_by_user_id'

        # Get the version content
        cursor.execute(f"""
            SELECT encrypted_report_content 
            FROM report_versions
            WHERE {fk_column} = %s AND version_number = %s;
        """, (report_id, version_number))

        version_data = cursor.fetchone()
        if not version_data:
            return jsonify({"status": "error", "message": "Version not found"}), 404

        version_content_encrypted = version_data[0]

        # Decrypt the version content
        cursor.execute(
            "SELECT pgp_sym_decrypt(%s::bytea, get_encryption_key());",
            (version_content_encrypted,)
        )
        decrypted_content = cursor.fetchone()[0]

        # Get current content to save as a version
        cursor.execute(f"""
            SELECT {content_column}
            FROM {table_name}
            WHERE id = %s AND {user_column} = %s;
        """, (report_id, current_user.id))

        result = cursor.fetchone()
        if not result:
            return jsonify({"status": "error", "message": "Report not found or permission denied"}), 404

        current_content_encrypted = result[0]

        # Save current content as a version before restoring
        if report_type in ['generated', 'trend_analysis']:  # CHANGE: handle both
            cursor.execute("""
                INSERT INTO report_versions 
                (generated_report_id, edited_by_user_id, encrypted_report_content, change_summary, auto_cleanup)
                VALUES (%s, %s, %s, %s, %s);
            """, (report_id, current_user.id, current_content_encrypted,
                  f"Auto-save before restoring to version {version_number}", True))
        else:
            cursor.execute("""
                INSERT INTO report_versions 
                (uploaded_report_id, edited_by_user_id, encrypted_report_content, change_summary, auto_cleanup)
                VALUES (%s, %s, %s, %s, %s);
            """, (report_id, current_user.id, current_content_encrypted,
                  f"Auto-save before restoring to version {version_number}", True))

        # Restore the version
        cursor.execute(f"""
            UPDATE {table_name}
            SET {content_column} = pgp_sym_encrypt(%s, get_encryption_key())
            WHERE id = %s AND {user_column} = %s;
        """, (decrypted_content, report_id, current_user.id))

        conn.commit()
        return jsonify({
            "status": "success",
            "message": f"Restored to version {version_number}",
            "content": decrypted_content
        })

    except psycopg2.Error as e:
        if conn: conn.rollback()
        current_app.logger.error(f"Database error restoring version: {e}")
        return jsonify({"status": "error", "message": "Database error"}), 500
    finally:
        if conn: conn.close()

@bp.route('/pin-version/<string:report_type>/<int:report_id>/<int:version_number>', methods=['POST'])
@login_required
def pin_version(report_type, report_id, version_number):
    """Pin a version to prevent it from being auto-deleted."""
    if report_type not in ['generated', 'uploaded', 'trend_analysis']:
        return jsonify({"status": "error", "message": "Invalid report type."}), 400

    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()

        # Determine FK column and verify ownership
        if report_type in ['generated', 'trend_analysis']:  # CHANGE: handle both
            fk_column = 'generated_report_id'
            cursor.execute(
                "SELECT 1 FROM generated_ai_reports WHERE id = %s AND generated_by_user_id = %s;",
                (report_id, current_user.id)
            )
        else:
            fk_column = 'uploaded_report_id'
            cursor.execute(
                "SELECT 1 FROM uploaded_reports WHERE id = %s AND uploaded_by_user_id = %s;",
                (report_id, current_user.id)
            )

        if not cursor.fetchone():
            return jsonify({"status": "error", "message": "Permission denied"}), 403

        # Toggle pin status
        cursor.execute(f"""
            UPDATE report_versions
            SET is_pinned = NOT is_pinned
            WHERE {fk_column} = %s AND version_number = %s
            RETURNING is_pinned;
        """, (report_id, version_number))

        result = cursor.fetchone()
        if not result:
            return jsonify({"status": "error", "message": "Version not found"}), 404

        is_pinned = result[0]
        conn.commit()

        return jsonify({
            "status": "success",
            "is_pinned": is_pinned,
            "message": f"Version {'pinned' if is_pinned else 'unpinned'} successfully"
        })

    except psycopg2.Error as e:
        if conn: conn.rollback()
        current_app.logger.error(f"Database error pinning version: {e}")
        return jsonify({"status": "error", "message": "Database error"}), 500
    finally:
        if conn: conn.close()

@bp.route('/prompt-templates')
@login_required
def prompt_templates():
    """Renders the page for a user to manage their personal prompt templates."""
    if not current_user.has_privilege('ManageUserTemplates'):
        abort(403)

    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    user_templates = []
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, template_name, technology FROM prompt_templates WHERE user_id = %s ORDER BY template_name;",
            (current_user.id,)
        )
        for row in cursor.fetchall():
            user_templates.append({"id": row[0], "name": row[1], "technology": row[2]})
    except psycopg2.Error as e:
        flash("Database error while loading your templates.", "danger")
    finally:
        if conn:
            conn.close()

    return render_template('profile/prompt_templates.html', templates=user_templates)

@bp.route('/prompt-templates/create', methods=['POST'])
@login_required
def create_prompt_template():
    """Handles the creation of a new user-owned prompt template."""
    if not current_user.has_privilege('ManageUserTemplates'):
        abort(403)

    template_name = request.form.get('template_name')
    technology = request.form.get('technology')
    template_content = request.form.get('template_content')

    if not all([template_name, technology, template_content]):
        flash("All fields are required.", "danger")
        return redirect(url_for('profile.prompt_templates'))

    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO prompt_templates (template_name, technology, template_content, user_id) VALUES (%s, %s, %s, %s);",
            (template_name, technology, template_content, current_user.id)
        )
        conn.commit()
        flash(f"Template '{template_name}' created successfully.", "success")
    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash(f"Database error: {e}", "danger")
    finally:
        if conn: conn.close()

    return redirect(url_for('profile.prompt_templates'))

@bp.route('/prompt-templates/get/<int:template_id>')
@login_required
def get_prompt_template_content(template_id):
    """API endpoint to fetch the content of a user's template for editing."""
    if not current_user.has_privilege('ManageUserTemplates'):
        return jsonify({"error": "Permission denied"}), 403

    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT template_name, technology, template_content FROM prompt_templates WHERE id = %s AND user_id = %s;",
            (template_id, current_user.id)
        )
        template = cursor.fetchone()
        if template:
            return jsonify({
                "name": template[0],
                "technology": template[1],
                "content": template[2]
            })
        return jsonify({"error": "Template not found"}), 404
    except psycopg2.Error as e:
        return jsonify({"error": "Database error"}), 500
    finally:
        if conn: conn.close()

@bp.route('/prompt-templates/edit/<int:template_id>', methods=['POST'])
@login_required
def edit_prompt_template(template_id):
    """Handles updates to a user's prompt template."""
    if not current_user.has_privilege('ManageUserTemplates'):
        abort(403)

    template_name = request.form.get('template_name')
    technology = request.form.get('technology')
    template_content = request.form.get('template_content')

    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE prompt_templates SET template_name = %s, technology = %s, template_content = %s WHERE id = %s AND user_id = %s;",
            (template_name, technology, template_content, template_id, current_user.id)
        )
        conn.commit()
        flash("Template updated successfully.", "success")
    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash(f"Database error: {e}", "danger")
    finally:
        if conn: conn.close()

    return redirect(url_for('profile.prompt_templates'))

@bp.route('/prompt-templates/delete/<int:template_id>', methods=['POST'])
@login_required
def delete_prompt_template(template_id):
    """Deletes a user's prompt template."""
    if not current_user.has_privilege('ManageUserTemplates'):
        abort(403)

    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM prompt_templates WHERE id = %s AND user_id = %s;", (template_id, current_user.id))
        conn.commit()
        flash("Template deleted successfully.", "success")
    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash(f"Database error: {e}", "danger")
    finally:
        if conn: conn.close()

    return redirect(url_for('profile.prompt_templates'))

@bp.route('/ai-settings/get-models/<int:provider_id>')
@login_required
def get_provider_models(provider_id):
    """Get available models for a provider, with auto-discovery if needed."""
    config = load_trends_config()
    db_settings = config.get('database')
    conn = None

    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()

        # Get provider details
        cursor.execute("""
            SELECT models_last_refreshed, supports_discovery, provider_type,
                   api_endpoint,
                   pgp_sym_decrypt(encrypted_api_key::bytea, get_encryption_key()),
                   api_model
            FROM ai_providers 
            WHERE id = %s;
        """, (provider_id,))

        provider_data = cursor.fetchone()
        if not provider_data:
            return jsonify({'error': 'Provider not found'}), 404

        last_refreshed, supports_discovery, provider_type, api_endpoint, api_key, default_model = provider_data

        # Check if we need to refresh (older than 24 hours)
        needs_refresh = False
        if last_refreshed is None:
            needs_refresh = True
        elif supports_discovery:
            from datetime import datetime, timedelta
            age = datetime.now(last_refreshed.tzinfo if last_refreshed.tzinfo else None) - last_refreshed
            if age > timedelta(hours=24):
                needs_refresh = True

        # If needs refresh and supports discovery, fetch fresh models
        if needs_refresh and supports_discovery and provider_type:
            try:
                discovered_models = discover_models_for_provider(
                    provider_type, api_endpoint, api_key, config
                )

                if discovered_models:
                    # Clear old cached models and insert new ones
                    cursor.execute("DELETE FROM ai_provider_models WHERE provider_id = %s;", (provider_id,))

                    for idx, model in enumerate(discovered_models):
                        cursor.execute("""
                            INSERT INTO ai_provider_models 
                            (provider_id, model_name, display_name, description, capabilities, sort_order)
                            VALUES (%s, %s, %s, %s, %s, %s);
                        """, (
                            provider_id,
                            model['name'],
                            model.get('display_name', model['name']),
                            model.get('description', ''),
                            json.dumps({
                                'input_token_limit': model.get('input_token_limit'),
                                'output_token_limit': model.get('output_token_limit'),
                            }),
                            idx
                        ))

                    # Update last refreshed timestamp
                    cursor.execute("""
                        UPDATE ai_providers 
                        SET models_last_refreshed = NOW()
                        WHERE id = %s;
                    """, (provider_id,))

                    conn.commit()

            except Exception as e:
                current_app.logger.error(f"Error discovering models: {e}")
                # Continue to return cached models if available

        # Return cached models
        cursor.execute("""
            SELECT model_name, display_name, description, capabilities, is_available
            FROM ai_provider_models
            WHERE provider_id = %s AND is_available = TRUE
            ORDER BY sort_order, display_name;
        """, (provider_id,))

        models = []
        for row in cursor.fetchall():
            model_name, display_name, description, capabilities, is_available = row
            models.append({
                'name': model_name,
                'display_name': display_name,
                'description': description,
                'capabilities': capabilities if capabilities else {}
            })

        # If no cached models, return the default model
        if not models and default_model:
            models = [{
                'name': default_model,
                'display_name': default_model,
                'description': 'Default model (discovery not supported or failed)',
                'capabilities': {}
            }]

        return jsonify({
            'models': models,
            'last_refreshed': last_refreshed.isoformat() if last_refreshed else None,
            'supports_discovery': supports_discovery
        })

    except Exception as e:
        current_app.logger.error(f"Error getting provider models: {e}")
        return jsonify({'error': 'Failed to get models'}), 500
    finally:
        if conn:
            conn.close()

def discover_models_for_provider(provider_type, api_endpoint, api_key, config):
    """
    Simple inline model discovery function.
    Returns list of model dictionaries or empty list on error.
    """
    import requests

    # Build headers and URL based on provider type
    headers = {'Content-Type': 'application/json'}
    timeout = config.get('ai_timeout', 30)
    verify_ssl = config.get('ai_ssl_verify', True)
    ssl_cert_path = config.get('ssl_cert_path')
    if verify_ssl and ssl_cert_path:
        verify_ssl = ssl_cert_path

    try:
        if provider_type == 'google_gemini':
            # Google uses API key in URL
            base_url = api_endpoint.rstrip('/')
            if not base_url.endswith('/models'):
                base_url = base_url.rsplit('/', 1)[0]  # Remove model name if present
            url = f"{base_url}?key={api_key}"

            response = requests.get(url, headers=headers, timeout=timeout, verify=verify_ssl)
            response.raise_for_status()
            data = response.json()

            models = []
            for model in data.get('models', []):
                if 'generateContent' in model.get('supportedGenerationMethods', []):
                    model_name = model.get('name', '').replace('models/', '')
                    if model_name:
                        models.append({
                            'name': model_name,
                            'display_name': model.get('displayName', model_name),
                            'description': model.get('description', ''),
                            'input_token_limit': model.get('inputTokenLimit'),
                            'output_token_limit': model.get('outputTokenLimit'),
                        })
            return models

        elif provider_type in ['openai', 'xai', 'together', 'deepseek', 'openrouter']:
            # OpenAI-compatible APIs
            base_url = api_endpoint.rsplit('/', 1)[0] if '/chat/completions' in api_endpoint else api_endpoint
            url = f"{base_url}/models"
            headers['Authorization'] = f'Bearer {api_key}'

            response = requests.get(url, headers=headers, timeout=timeout, verify=verify_ssl)
            response.raise_for_status()
            data = response.json()

            models = []
            for model in data.get('data', []):
                model_id = model.get('id', '')
                # Filter for chat models
                if any(kw in model_id.lower() for kw in ['gpt', 'chat', 'turbo', 'grok', 'llama', 'mistral', 'deepseek']):
                    models.append({
                        'name': model_id,
                        'display_name': model_id,
                        'description': f"Owner: {model.get('owned_by', 'Unknown')}",
                    })
            return models

        elif provider_type == 'ollama':
            # Ollama local
            url = f"{api_endpoint.rsplit('/api/', 1)[0]}/api/tags"

            response = requests.get(url, timeout=timeout, verify=False)  # Local doesn't need SSL
            response.raise_for_status()
            data = response.json()

            models = []
            for model in data.get('models', []):
                models.append({
                    'name': model.get('name'),
                    'display_name': model.get('name'),
                    'description': f"Size: {model.get('size', 'Unknown')}",
                })
            return models

        else:
            # Unknown provider type
            return []

    except Exception as e:
        current_app.logger.error(f"Error discovering models for {provider_type}: {e}")
        return []

@bp.route('/ai-settings/refresh-models/<int:provider_id>', methods=['POST'])
@login_required
def refresh_provider_models(provider_id):
    """Force refresh models for a provider."""
    if not current_user.has_privilege('ManageAIProfiles'):
        return jsonify({'error': 'Permission denied'}), 403

    config = load_trends_config()
    db_settings = config.get('database')
    conn = None

    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()

        # Reset last_refreshed to force re-discovery
        cursor.execute("""
            UPDATE ai_providers 
            SET models_last_refreshed = NULL
            WHERE id = %s;
        """, (provider_id,))
        conn.commit()

        # Now call get_provider_models which will trigger discovery
        return get_provider_models(provider_id)

    except Exception as e:
        current_app.logger.error(f"Error refreshing models: {e}")
        return jsonify({'error': 'Failed to refresh models'}), 500
    finally:
        if conn:
            conn.close()
