# The Data Engineering Project

## Contributors - Team Ness
[Liam Dearlove](https://github.com/ldearlove) |
[Paul Sandford](https://github.com/Sandpaul) |
[Inna Teterina](https://github.com/innateterina) |
[Muhammad Raza](https://github.com/muhammad7877) |
[Muhammed Irfan](https://github.com/Irfan6672) |
[Rahul Aneesh](https://github.com/KiraHeichou)

## Overview
This project creates a data platform that extracts data from an OLTP (Online Transaction Processing) database and stores the extracted
data in a data lake. This extracted data is transformed, stored as parquet files, and then loaded into a remodelled OLAP (Online Analytical Processing) data warehouse. This data platform is hosted on Amazon Web Services (AWS) via Terraform as IaC (Infrastructure as Code).
* Using AWS Eventbridge, the platform operates automatically on a schedule (every 5 minutes) to extract any recently added data from the OLTP database via an AWS Lambda function.
* This data is then stored as JSON file in an ingestion s3 bucket on AWS, and a Cloudwatch log of the process is created.
* The addition of data to the ingestion s3 bucket triggers the transformation Lambda function, in which the data is remodelled and stored as a parquet file in a processed s3 bucket.
* The addition of data to the processed s3 bucket triggers the load Lambda function, loading the transformed data into the OLAP data warehouse.
* The data warehouse separates the data into dimension and fact tables in order to improve querying and readability.
* The transformation and load Lambdas are also monitored using Cloudwatch.
* All Python code used in the Lambda functions is unit tested (following TDD principles), PEP8 compliant, checked for coverage levels and checked for security vulnerabilities using safety and bandit.
* All of the above checks are run automatically using GitHub Actions.

## Contents of Repo
Included in this rep:
1. A `src` directory containing the various Python modules needed for each Lambda function, grouped by function.
2. A `test` directory containing the unit tests for each Python function, along with test data for the tests that require it.
3. A `terraform` directory containing all the terraform files needed to deploy and manage the AWS infrastructure.
4. A `.github/workflows` directory containing a YAML file used for CI/CD with GitHub Actions.
5. A `Makefile` to help set up a virtual environment, install required dependencies, and run unit tests and safety checks.
6. A `.gitignore` file.
7. A `requirements.txt` file listing all the library dependencies and their version numbers.
8. This `README` file to detail the project and the contents of this repo.

## Instructions
To deploy this project locally:

1. Create and activate a virtual environment.
```bash
python -m venv venv
source venv/bin/activate
```
2. Install the required library dependencies.
```bash
pip install -r requirements.txt
```
3. Set up the `PYTHONPATH`.
```bash
export PYTHONPATH=$(pwd)
```
4. Check that the unit tests are passing, that coverage is at an acceptable level and that there are no security vulnerabilities.
```bash
make unit-test
make check-coverage
make security-test
```
5. Check that the code is PEP8 compliant.
```bash
make run-flake
```
6. Run the Terraform to set up the AWS infrastructure.
```bash
terraform init
terraform plan
terraform apply -auto-approve
```