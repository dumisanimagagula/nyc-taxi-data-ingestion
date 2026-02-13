#!/usr/bin/env python
"""Simple script to verify Bronze layer ingestion succeeded"""

import os

import boto3

# Set MinIO credentials
os.environ["AWS_ACCESS_KEY_ID"] = "minio"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minio123"
os.environ["AWS_ENDPOINT_URL"] = "http://minio:9000"

# Connect to MinIO
s3 = boto3.client("s3")

print("=" * 80)
print("BRONZE LAYER VERIFICATION")
print("=" * 80)

# List Bronze bucket contents
print("\n Bronze Bucket Contents (first 50 files):")
resp = s3.list_objects_v2(Bucket="bronze", Prefix="nyc-taxi/", MaxKeys=50)

if "Contents" not in resp:
    print("  ❌ NO FILES FOUND in bronze/nyc-taxi/")
else:
    total_size = 0
    data_files = []
    metadata_files = []

    for obj in resp["Contents"]:
        key = obj["Key"]
        size = obj["Size"]
        print(f"  - {key} ({size:,} bytes)")

        if key.endswith(".parquet"):
            data_files.append(key)
            total_size += size
        elif "/metadata/" in key:
            metadata_files.append(key)

    print(f"\n✓ Found {len(data_files)} data files ({total_size / 1024 / 1024:.2f} MB)")
    print(f"✓ Found {len(metadata_files)} metadata files")

print("=" * 80)
