# AWS_SERVERLESS
Lambdas using AWS services - SAM, DynamoDB, S3, SSM, Lambda

## Deploy serverless application to SAM CLI command
We have made some improvements to make build, package and deploy consistent. But avoid package if needed.
(NO need to pass template file to any command)
$ sam build

No `sam package` needed. Deploy will autopackage. No template also needed
$ sam deploy --s3-bucket mybucket --stack-name mystack --capabilities CAPABILITY_IAM

also different ways to deploy to the AWS https://docs.google.com/document/d/1pdFjkEGcE8PO9ZYO6E8e9V4oOVsSSD4cDXBqXTdJFhU/edit?usp=sharing

