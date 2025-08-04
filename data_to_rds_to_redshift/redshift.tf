
# IAM Role for Redshift to access S3 and interact with other resources on AWS (e.g for data loading/unloading)
resource "aws_iam_role" "redshift_role" {
  name = "wofai_redshift-s3-iam-access-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action    = "sts:AssumeRole",
      Effect    = "Allow",
      Principal = {
        Service = "redshift.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "redshift_s3_policy" {
  role       = aws_iam_role.redshift_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}



# Create a new VPC for Redshift cluster
resource "aws_vpc" "redshift_vpc" {
  cidr_block           = "10.0.0.0/16"

  tags = {
    Name = "wofai-redshift-vpc"
  }
}

# Create public subnet for access to the redshift cluster
resource "aws_subnet" "redshift_subnet" {
  vpc_id            = aws_vpc.redshift_vpc.id
  cidr_block        = "10.0.1.0/24"
  availability_zone = "us-east-1a"

  tags = {
    Name = "redshift-subnet"
  }
}

# Internet Gateway to enable internet access (if needed)
resource "aws_internet_gateway" "redshift_igw" {
  vpc_id = aws_vpc.redshift_vpc.id

  tags = {
    Name = "redshift-igw"
  }
}

# Route Table
resource "aws_route_table" "redshift_rt" {
  vpc_id = aws_vpc.redshift_vpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.redshift_igw.id
  }

  tags = {
    Name = "redshift-rt"
  }
}

# Associate Route Table with subnet
resource "aws_route_table_association" "redshift_rta" {
  subnet_id      = aws_subnet.redshift_subnet.id
  route_table_id = aws_route_table.redshift_rt.id
}


# Security Group for Redshift allowing inbound traffic on Redshift port 5439
resource "aws_security_group" "redshift_sg" {
  name        = "wofai_redshift-sg"
  description = "Allow Redshift access"
  vpc_id      = aws_vpc.redshift_vpc.id

  ingress {
    description      = "Allow Redshift"
    from_port        = 5439
    to_port          = 5439
    protocol         = "tcp"
    cidr_blocks      = ["10.0.0.0/24"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["10.0.0.0/24"]
  }

  tags = {
    Name = "redshift-sg"
  }
}


# Redshift Cluster
resource "aws_redshift_cluster" "redshift_cluster" {
  cluster_identifier = "wofai-redshift-cluster"
  database_name      = "wofai_database"
  master_username    = "admin"
  manage_master_password = true
  node_type          = "dc2.large"
  cluster_type       = "multi-node"
  number_of_nodes    = 3 
  iam_roles          = [aws_iam_role.redshift_role.arn]
  publicly_accessible = true
  vpc_security_group_ids = [aws_security_group.redshift_sg.id]
  cluster_subnet_group_name      = aws_redshift_subnet_group.redshift_subnet_group.name

  tags = {
    Name = "Wofai-Redshift-Cluster"
  }
}

# Redshift Subnet Group for Redshift clusters in VPC
resource "aws_redshift_subnet_group" "redshift_subnet_group" {
  name       = "redshift-subnet-group"
  subnet_ids = [aws_subnet.redshift_subnet.id]

  tags = {
    Name = "redshift-subnet-group"
  }
}
