resource "aws_ecr_repository" "fraud_processor" {
  name                 = "${var.cluster_name}/fraud-processor"
  image_tag_mutability = "IMMUTABLE"
  force_delete         = true # demo: allow `terraform destroy` with images present

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "fraud_processor" {
  repository = aws_ecr_repository.fraud_processor.name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep the 20 most recent images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 20
      }
      action = { type = "expire" }
    }]
  })
}
