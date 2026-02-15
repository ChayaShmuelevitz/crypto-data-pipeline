terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# חיבור ל-AWS
provider "aws" {
  region = "eu-west-1"
}

# Bronze Bucket - נתונים גולמיים
resource "aws_s3_bucket" "bronze" {
  bucket = "data-pipeline-bronze-${random_id.suffix.hex}"
}

# Silver Bucket - נתונים נקיים
resource "aws_s3_bucket" "silver" {
  bucket = "data-pipeline-silver-${random_id.suffix.hex}"
}

# Gold Bucket - נתונים מוכנים לאנליזה
resource "aws_s3_bucket" "gold" {
  bucket = "data-pipeline-gold-${random_id.suffix.hex}"
}

# suffix אקראי כדי שהשם יהיה ייחודי
resource "random_id" "suffix" {
  byte_length = 4
}

# Versioning ל-Bronze
resource "aws_s3_bucket_versioning" "bronze_versioning" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration {
    status = "Enabled"
  }
}

# חסימת גישה ציבורית לכל ה-Buckets
resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket                  = aws_s3_bucket.bronze.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "silver" {
  bucket                  = aws_s3_bucket.silver.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "gold" {
  bucket                  = aws_s3_bucket.gold.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}