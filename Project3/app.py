import os
import uuid
from functools import wraps

import pymysql
from dotenv import load_dotenv
from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, session, send_from_directory
)
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename

load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key")

UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "uploads")
ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp"}
MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16 MB

app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH

os.makedirs(UPLOAD_FOLDER, exist_ok=True)


# ---------------------------------------------------------------------------
# Database helper
# ---------------------------------------------------------------------------

def get_db():
    return pymysql.connect(
        host=os.environ.get("DB_HOST", "127.0.0.1"),
        user=os.environ.get("DB_USER", "root"),
        password=os.environ.get("DB_PASS", ""),
        database=os.environ.get("DB_NAME", "photo_gallery"),
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def init_db():
    """Create tables if they don't exist."""
    db = get_db()
    with db.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                email VARCHAR(100) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS photos (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                filename VARCHAR(255) NOT NULL,
                original_name VARCHAR(255) NOT NULL,
                description VARCHAR(500),
                file_size INT,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )
        """)
    db.close()


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            flash("Please log in first.", "warning")
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


# ---------------------------------------------------------------------------
# Auth routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    if "user_id" in session:
        return redirect(url_for("gallery"))
    return redirect(url_for("login"))


@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        email = request.form.get("email", "").strip()
        password = request.form.get("password", "")

        if not username or not email or not password:
            flash("All fields are required.", "danger")
            return redirect(url_for("register"))

        pw_hash = generate_password_hash(password)
        db = get_db()
        try:
            with db.cursor() as cur:
                cur.execute(
                    "INSERT INTO users (username, email, password_hash) VALUES (%s, %s, %s)",
                    (username, email, pw_hash),
                )
            flash("Account created! Please log in.", "success")
            return redirect(url_for("login"))
        except pymysql.err.IntegrityError:
            flash("Username or email already exists.", "danger")
        finally:
            db.close()

    return render_template("register.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        db = get_db()
        with db.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE username = %s", (username,))
            user = cur.fetchone()
        db.close()

        if user and check_password_hash(user["password_hash"], password):
            session["user_id"] = user["id"]
            session["username"] = user["username"]
            flash(f"Welcome back, {user['username']}!", "success")
            return redirect(url_for("gallery"))

        flash("Invalid username or password.", "danger")

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    flash("Logged out.", "info")
    return redirect(url_for("login"))


# ---------------------------------------------------------------------------
# Photo routes
# ---------------------------------------------------------------------------

@app.route("/gallery")
@login_required
def gallery():
    db = get_db()
    with db.cursor() as cur:
        cur.execute(
            "SELECT * FROM photos WHERE user_id = %s ORDER BY uploaded_at DESC",
            (session["user_id"],),
        )
        photos = cur.fetchall()
    db.close()
    return render_template("gallery.html", photos=photos)


@app.route("/upload", methods=["GET", "POST"])
@login_required
def upload():
    if request.method == "POST":
        files = request.files.getlist("photos")
        description = request.form.get("description", "").strip()

        if not files or files[0].filename == "":
            flash("No files selected.", "danger")
            return redirect(url_for("upload"))

        db = get_db()
        count = 0
        for f in files:
            if f and allowed_file(f.filename):
                original_name = secure_filename(f.filename)
                ext = original_name.rsplit(".", 1)[1].lower()
                unique_name = f"{uuid.uuid4().hex}.{ext}"
                save_path = os.path.join(app.config["UPLOAD_FOLDER"], unique_name)
                f.save(save_path)
                file_size = os.path.getsize(save_path)

                with db.cursor() as cur:
                    cur.execute(
                        """INSERT INTO photos
                           (user_id, filename, original_name, description, file_size)
                           VALUES (%s, %s, %s, %s, %s)""",
                        (session["user_id"], unique_name, original_name, description, file_size),
                    )
                count += 1
        db.close()

        flash(f"Successfully uploaded {count} photo(s).", "success")
        return redirect(url_for("gallery"))

    return render_template("upload.html")


@app.route("/search")
@login_required
def search():
    query = request.args.get("q", "").strip()
    photos = []
    if query:
        db = get_db()
        with db.cursor() as cur:
            cur.execute(
                """SELECT * FROM photos
                   WHERE user_id = %s
                     AND (original_name LIKE %s OR description LIKE %s)
                   ORDER BY uploaded_at DESC""",
                (session["user_id"], f"%{query}%", f"%{query}%"),
            )
            photos = cur.fetchall()
        db.close()
    return render_template("search.html", photos=photos, query=query)


@app.route("/download/<int:photo_id>")
@login_required
def download(photo_id):
    db = get_db()
    with db.cursor() as cur:
        cur.execute(
            "SELECT * FROM photos WHERE id = %s AND user_id = %s",
            (photo_id, session["user_id"]),
        )
        photo = cur.fetchone()
    db.close()

    if not photo:
        flash("Photo not found.", "danger")
        return redirect(url_for("gallery"))

    return send_from_directory(
        app.config["UPLOAD_FOLDER"],
        photo["filename"],
        as_attachment=True,
        download_name=photo["original_name"],
    )


@app.route("/delete/<int:photo_id>", methods=["POST"])
@login_required
def delete(photo_id):
    db = get_db()
    with db.cursor() as cur:
        cur.execute(
            "SELECT * FROM photos WHERE id = %s AND user_id = %s",
            (photo_id, session["user_id"]),
        )
        photo = cur.fetchone()
        if photo:
            filepath = os.path.join(app.config["UPLOAD_FOLDER"], photo["filename"])
            if os.path.exists(filepath):
                os.remove(filepath)
            cur.execute("DELETE FROM photos WHERE id = %s", (photo_id,))
            flash("Photo deleted.", "info")
        else:
            flash("Photo not found.", "danger")
    db.close()
    return redirect(url_for("gallery"))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5000, debug=True)
