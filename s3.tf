#provision aws S3 bucket

resource "aws_s3_bucket" "wofais-aws-bucket" {
  bucket = "wofais-aws-bucket"

  tags = {
    Name        = "wofai_bucket"
    Environment = "Dev"
  }
}