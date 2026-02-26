"""
Part A  –  Photo Gallery Application
Backend : Amazon DynamoDB  +  Amazon S3
Web     : Python / Flask
"""

import os, uuid, hashlib, datetime
from io import BytesIO

import boto3
from boto3.dynamodb.conditions import Attr
from flask import (
    Flask, render_template, request, redirect,
    url_for, session, flash, send_file,
)
from dotenv import load_dotenv
from werkzeug.utils import secure_filename

# ── Load .env ────────────────────────────────────────────────────────────────
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# ── Flask ────────────────────────────────────────────────────────────────────
app = Flask(
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), "..", "templates"),
    static_folder=os.path.join(os.path.dirname(__file__), "..", "static"),
)
app.secret_key = os.getenv("SECRET_KEY", "dev-secret-key")

ALLOWED_EXT = {"png", "jpg", "jpeg", "gif", "bmp", "webp"}

# ── AWS clients ──────────────────────────────────────────────────────────────
_aws = dict(
    region_name=os.getenv("AWS_REGION", "us-east-1"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)
dynamodb = boto3.resource("dynamodb", **_aws)
s3_client = boto3.client("s3", **_aws)

S3_BUCKET   = os.getenv("S3_BUCKET_NAME", "se422-photo-gallery-bucket")
TBL_USERS   = os.getenv("DYNAMO_USERS_TABLE", "PhotoGalleryUsers")
TBL_PHOTOS  = os.getenv("DYNAMO_PHOTOS_TABLE", "PhotoGalleryPhotos")


# ── Helpers ──────────────────────────────────────────────────────────────────
def _pw(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def _ok_file(name: str) -> bool:
    return "." in name and name.rsplit(".", 1)[1].lower() in ALLOWED_EXT


def _init_tables():
    existing = {t.name for t in dynamodb.tables.all()}
    if TBL_USERS not in existing:
        dynamodb.create_table(
            TableName=TBL_USERS,
            KeySchema=[{"AttributeName": "username", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "username", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        dynamodb.Table(TBL_USERS).wait_until_exists()
    if TBL_PHOTOS not in existing:
        dynamodb.create_table(
            TableName=TBL_PHOTOS,
            KeySchema=[{"AttributeName": "photo_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "photo_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        dynamodb.Table(TBL_PHOTOS).wait_until_exists()


def _init_bucket():
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET)
    except Exception:
        rgn = os.getenv("AWS_REGION", "us-east-1")
        if rgn == "us-east-1":
            s3_client.create_bucket(Bucket=S3_BUCKET)
        else:
            s3_client.create_bucket(
                Bucket=S3_BUCKET,
                CreateBucketConfiguration={"LocationConstraint": rgn},
            )


# ── Bootstrap ────────────────────────────────────────────────────────────────
_init_tables()
_init_bucket()

users_tbl  = dynamodb.Table(TBL_USERS)
photos_tbl = dynamodb.Table(TBL_PHOTOS)


# ══════════════════════════════════════════════════════════════════════════════
#  ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return redirect(url_for("gallery") if "username" in session else url_for("login"))


# ── Auth ─────────────────────────────────────────────────────────────────────
@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        username = request.form["username"].strip()
        password = request.form["password"].strip()
        email    = request.form.get("email", "").strip()
        if not username or not password:
            flash("Username and password are required.", "danger")
            return redirect(url_for("register"))
        if "Item" in users_tbl.get_item(Key={"username": username}):
            flash("Username already taken.", "danger")
            return redirect(url_for("register"))
        users_tbl.put_item(Item={
            "username": username,
            "password": _pw(password),
            "email": email,
            "created_at": datetime.datetime.utcnow().isoformat(),
        })
        flash("Account created — please log in.", "success")
        return redirect(url_for("login"))
    return render_template("register.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"].strip()
        password = request.form["password"].strip()
        resp = users_tbl.get_item(Key={"username": username})
        user = resp.get("Item")
        if user and user["password"] == _pw(password):
            session["username"] = username
            flash(f"Welcome back, {username}!", "success")
            return redirect(url_for("gallery"))
        flash("Invalid credentials.", "danger")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.pop("username", None)
    flash("Logged out.", "info")
    return redirect(url_for("login"))


# ── Gallery ──────────────────────────────────────────────────────────────────
@app.route("/gallery")
def gallery():
    if "username" not in session:
        return redirect(url_for("login"))
    username = session["username"]
    items = photos_tbl.scan(FilterExpression=Attr("username").eq(username)).get("Items", [])
    items.sort(key=lambda p: p.get("uploaded_at", ""), reverse=True)
    return render_template("gallery.html", photos=items, username=username)


# ── Upload ───────────────────────────────────────────────────────────────────
@app.route("/upload", methods=["GET", "POST"])
def upload():
    if "username" not in session:
        return redirect(url_for("login"))
    if request.method == "POST":
        files = request.files.getlist("photos")
        if not files or files[0].filename == "":
            flash("No files selected.", "danger")
            return redirect(url_for("upload"))
        tags = request.form.get("tags", "").strip()
        desc = request.form.get("description", "").strip()
        count = 0
        for f in files:
            if f and _ok_file(f.filename):
                safe = secure_filename(f.filename)
                pid  = str(uuid.uuid4())
                key  = f"photos/{session['username']}/{pid}_{safe}"
                s3_client.upload_fileobj(
                    f.stream, S3_BUCKET, key,
                    ExtraArgs={"ContentType": f.content_type},
                )
                photos_tbl.put_item(Item={
                    "photo_id": pid,
                    "username": session["username"],
                    "filename": safe,
                    "s3_key": key,
                    "tags": tags,
                    "description": desc,
                    "uploaded_at": datetime.datetime.utcnow().isoformat(),
                })
                count += 1
        flash(f"Uploaded {count} photo(s).", "success")
        return redirect(url_for("gallery"))
    return render_template("upload.html")


# ── Search ───────────────────────────────────────────────────────────────────
@app.route("/search")
def search():
    if "username" not in session:
        return redirect(url_for("login"))
    q = request.args.get("q", "").strip().lower()
    username = session["username"]
    if not q:
        return render_template("search.html", photos=[], query="")
    items = photos_tbl.scan(FilterExpression=Attr("username").eq(username)).get("Items", [])
    results = [
        p for p in items
        if q in p.get("filename", "").lower()
        or q in p.get("tags", "").lower()
        or q in p.get("description", "").lower()
    ]
    return render_template("search.html", photos=results, query=q)


# ── Download ─────────────────────────────────────────────────────────────────
@app.route("/download/<photo_id>")
def download(photo_id):
    if "username" not in session:
        return redirect(url_for("login"))
    item = photos_tbl.get_item(Key={"photo_id": photo_id}).get("Item")
    if not item or item["username"] != session["username"]:
        flash("Photo not found.", "danger")
        return redirect(url_for("gallery"))
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=item["s3_key"])
    return send_file(BytesIO(obj["Body"].read()), download_name=item["filename"], as_attachment=True)


# ── Delete ───────────────────────────────────────────────────────────────────
@app.route("/delete/<photo_id>", methods=["POST"])
def delete(photo_id):
    if "username" not in session:
        return redirect(url_for("login"))
    item = photos_tbl.get_item(Key={"photo_id": photo_id}).get("Item")
    if not item or item["username"] != session["username"]:
        flash("Photo not found.", "danger")
        return redirect(url_for("gallery"))
    s3_client.delete_object(Bucket=S3_BUCKET, Key=item["s3_key"])
    photos_tbl.delete_item(Key={"photo_id": photo_id})
    flash("Photo deleted.", "success")
    return redirect(url_for("gallery"))


# ── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
