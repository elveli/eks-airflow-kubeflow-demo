# -----------------------------------------------------------------------------
# VPC — cost-optimized for a throwaway demo.
#
# Default mode (enable_nat = false):
#   2 PUBLIC subnets (EKS control plane needs 2 AZs), nodes get public IPs,
#   egress goes straight out the internet gateway → NO NAT gateway at all.
#   Inbound is still closed: the EKS cluster security group only allows
#   intra-cluster traffic and nothing in this repo opens node ports.
#
# enable_nat = true:
#   Adds 2 private subnets and ONE NAT gateway (deliberately not per-AZ —
#   an AZ outage takes out egress, which is fine for a demo and halves cost).
# -----------------------------------------------------------------------------

resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true # required by EKS

  tags = { Name = "${var.name}-vpc" }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id

  tags = { Name = "${var.name}-igw" }
}

# --- Public subnets (always created: control plane + optional ALBs live here)
resource "aws_subnet" "public" {
  count = length(var.azs)

  vpc_id            = aws_vpc.this.id
  availability_zone = var.azs[count.index]
  cidr_block        = cidrsubnet(var.vpc_cidr, 4, count.index) # /20 each

  # Managed node groups do not assign public IPs themselves; in no-NAT mode
  # the subnet must do it or nodes can't reach ECR / the internet.
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.name}-public-${var.azs[count.index]}"
    # Lets the AWS Load Balancer Controller discover subnets for
    # internet-facing load balancers (only used if you opt into an ALB).
    "kubernetes.io/role/elb"                    = "1"
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  tags = { Name = "${var.name}-public-rt" }
}

resource "aws_route" "public_internet" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.this.id
}

resource "aws_route_table_association" "public" {
  count = length(aws_subnet.public)

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# --- Private subnets + single NAT (only when enable_nat = true) --------------
resource "aws_subnet" "private" {
  count = var.enable_nat ? length(var.azs) : 0

  vpc_id            = aws_vpc.this.id
  availability_zone = var.azs[count.index]
  cidr_block        = cidrsubnet(var.vpc_cidr, 4, count.index + 8)

  tags = {
    Name                                        = "${var.name}-private-${var.azs[count.index]}"
    "kubernetes.io/role/internal-elb"           = "1"
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
  }
}

resource "aws_eip" "nat" {
  count = var.enable_nat ? 1 : 0

  domain = "vpc"

  tags = { Name = "${var.name}-nat-eip" }
}

# SINGLE NAT gateway by design (~$0.045/h + $0.045/GB processed). Both AZs'
# private subnets route through it.
resource "aws_nat_gateway" "this" {
  count = var.enable_nat ? 1 : 0

  allocation_id = aws_eip.nat[0].id
  subnet_id     = aws_subnet.public[0].id

  tags = { Name = "${var.name}-nat" }

  depends_on = [aws_internet_gateway.this]
}

resource "aws_route_table" "private" {
  count = var.enable_nat ? 1 : 0

  vpc_id = aws_vpc.this.id

  tags = { Name = "${var.name}-private-rt" }
}

resource "aws_route" "private_nat" {
  count = var.enable_nat ? 1 : 0

  route_table_id         = aws_route_table.private[0].id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.this[0].id
}

resource "aws_route_table_association" "private" {
  count = var.enable_nat ? length(var.azs) : 0

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[0].id
}
