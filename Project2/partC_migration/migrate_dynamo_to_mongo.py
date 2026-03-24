"""
Part C  –  Data Migration from DynamoDB to MongoDB

Reads every item from the DynamoDB Users and Photos tables
and upserts them into the MongoDB collections used by Part B.
S3 photo files are NOT touched — both parts already share the
same S3 bucket, so only metadata needs to migrate.

Usage:
    python migrate_dynamo_to_mongo.py
"""

import os
import boto3
from pymongo import MongoClient, ReplaceOne
from dotenv import load_dotenv

# ── Config ───────────────────────────────────────────────────────────────────
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

_aws = dict(
    region_name=os.getenv("AWS_REGION", "us-east-1"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)
dynamodb = boto3.resource("dynamodb", **_aws)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB_NAME", "photo_gallery")
mongo     = MongoClient(MONGO_URI)
db        = mongo[MONGO_DB]

TBL_USERS  = os.getenv("DYNAMO_USERS_TABLE", "PhotoGalleryUsers")
TBL_PHOTOS = os.getenv("DYNAMO_PHOTOS_TABLE", "PhotoGalleryPhotos")


def scan_all(table_name: str) -> list:
    """Full-table scan with pagination support."""
    table = dynamodb.Table(table_name)
    items, last_key = [], None
    while True:
        kwargs = {}
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
    return items


def migrate_users():
    print(f"[1/2] Scanning DynamoDB table '{TBL_USERS}' …")
    users = scan_all(TBL_USERS)
    print(f"      Found {len(users)} user(s).")
    if not users:
        return

    ops = [
        ReplaceOne({"username": u["username"]}, u, upsert=True)
        for u in users
    ]
    result = db["users"].bulk_write(ops)
    print(f"      Upserted {result.upserted_count}, "
          f"modified {result.modified_count} user document(s).")


def migrate_photos():
    print(f"[2/2] Scanning DynamoDB table '{TBL_PHOTOS}' …")
    photos = scan_all(TBL_PHOTOS)
    print(f"      Found {len(photos)} photo(s).")
    if not photos:
        return

    ops = [
        ReplaceOne({"photo_id": p["photo_id"]}, p, upsert=True)
        for p in photos
    ]
    result = db["photos"].bulk_write(ops)
    print(f"      Upserted {result.upserted_count}, "
          f"modified {result.modified_count} photo document(s).")


def main():
    print("=" * 60)
    print("  DynamoDB  ➜  MongoDB  Migration")
    print("=" * 60)
    migrate_users()
    migrate_photos()
    print("=" * 60)
    print("  Migration complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
