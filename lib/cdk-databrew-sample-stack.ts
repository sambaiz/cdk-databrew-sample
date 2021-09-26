import * as cdk from '@aws-cdk/core';
import * as s3 from '@aws-cdk/aws-s3';
import * as s3deploy from '@aws-cdk/aws-s3-deployment';
import * as databrew from '@aws-cdk/aws-databrew';
import * as iam from '@aws-cdk/aws-iam';

export class CdkDatabrewSampleStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const {bucket, deployData} = this.createDataBucket()
    const dataset = this.createDataset(bucket)
    const recipe = this.createRecipe()
    const project = this.createProject(bucket, deployData, dataset.name, recipe.name)
    const jobRole = this.createJobRole(bucket)
    this.createProfileJob(dataset.name, bucket, jobRole)
    this.createRecipeJob(project.name, bucket, jobRole)
  }

  createDataBucket() {
    const bucket = new s3.Bucket(this, 'DataBucket', {
      bucketName: `databrew-sample-${this.account}-${this.region}`,
      removalPolicy: cdk.RemovalPolicy.DESTROY
    })
    const deployData = new s3deploy.BucketDeployment(this, 'DeploySource', {
      sources: [s3deploy.Source.asset('./data')],
      destinationBucket: bucket,
      destinationKeyPrefix: "src/"
    })
    return {bucket, deployData}
  }

  createDataset(bucket: s3.IBucket) {
    return new databrew.CfnDataset(this, 'Dataset', {
      name: "databrew-sample-train-dataset",
      input: {
        s3InputDefinition: {
          bucket: bucket.bucketName,
          key: "src/<[^/]+>.csv"
        }
      },
      format: "CSV",
    })
  }

  createRecipe() {
    return new databrew.CfnRecipe(this, 'Recipe', {
      name: "databrew-sample-train-recipe",
      steps: [{
        "action": {
          "operation": "CATEGORICAL_MAPPING",
          "parameters": {
            "categoryMap": "{\"RL\":\"1\",\"RM\":\"2\",\"FV\":\"3\",\"C (all)\":\"4\",\"RH\":\"5\"}",
            "deleteOtherRows": "false",
            "mapType": "NUMERIC",
            "mappingOption": "TOP_X_VALUES",
            "other": "6",
            "sourceColumn": "MSZoning",
            "targetColumn": "MSZoning_map"
          }
        }
      }]
    })
  }

  createProject(bucket: s3.IBucket, deployData: s3deploy.BucketDeployment, datasetName: string, recipeName: string) {
    const role = new iam.Role(this, 'ProjectRole', {
      assumedBy: new iam.ServicePrincipal("databrew.amazonaws.com"),
      inlinePolicies: {
        "project": iam.PolicyDocument.fromJson({
          "Version": "2012-10-17",
          "Statement": [
            {
              "Effect": "Allow",
              "Action": [
                  "s3:ListBucket",
                  "s3:GetObject"
              ],
              "Resource": [
                bucket.bucketArn,
                `${bucket.bucketArn}/*`
              ]
            },
            {
              "Effect": "Allow",
              "Action": [
                /*
                "glue:GetDatabases", 
                "glue:GetPartitions", 
                "glue:GetTable", 
                "glue:GetTables", 
                "glue:GetConnection",
                "lakeformation:GetDataAccess",
                */
                "ec2:DescribeVpcEndpoints",
                "ec2:DescribeRouteTables",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DescribeSecurityGroups",
                "ec2:DescribeSubnets",
                "ec2:DescribeVpcAttribute",
                "ec2:CreateNetworkInterface"
              ],
              "Resource": [
                "*"
              ]
            },
            {
                "Effect": "Allow",
                "Action": "ec2:DeleteNetworkInterface",
                "Condition": {
                    "StringLike": {
                        "aws:ResourceTag/aws-glue-service-resource": "*"
                    }
                },
                "Resource": [
                    "*"
                ]
            },
            {
              "Effect": "Allow",
              "Action": [
                "ec2:CreateTags",
                "ec2:DeleteTags"
              ],
              "Condition": {
                "ForAllValues:StringEquals": {
                  "aws:TagKeys": [
                    "aws-glue-service-resource"
                  ]
                }
              },
              "Resource": [
                "arn:aws:ec2:*:*:network-interface/*",
                "arn:aws:ec2:*:*:security-group/*"
              ]
            },
            {
              "Effect": "Allow",
              "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
              ],
              "Resource": [
                "arn:aws:logs:*:*:log-group:/aws-glue-databrew/*"
              ]
            },
            {
              "Effect": "Allow",
              "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
              ],
              "Resource": [
                "arn:aws:logs:*:*:log-group:/aws-glue-databrew/*"
              ]
            }
          ]
        })
      }
    })
    
    const project = new databrew.CfnProject(this, 'Project', {
      name: "databrew-sample-train-project",
      datasetName: datasetName,
      sample: {
        type: "FIRST_N",
        size: 500
      },
      recipeName: recipeName,
      roleArn: role.roleArn
    })
    project.node.addDependency(deployData)
    return project
  }

  createJobRole(bucket: s3.IBucket) {
    return new iam.Role(this, 'JobRole', {
      assumedBy: new iam.ServicePrincipal("databrew.amazonaws.com"),
      inlinePolicies: {
        "project": iam.PolicyDocument.fromJson({
          "Version": "2012-10-17",
          "Statement": [
            {
              "Effect": "Allow",
              "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket",
                "s3:DeleteObject"
              ],
              "Resource": [
                bucket.bucketArn,
                `${bucket.bucketArn}/*`
              ]
            },
            {
              "Effect": "Allow",
              "Action": [
                "s3:PutObjectAcl"
              ],
              "Resource": [
                `${bucket.bucketArn}/*`
              ],
              "Condition": {
                "StringEquals": {
                  "s3:x-amz-acl": "bucket-owner-full-control"
                }
              }
            }
          ]
        })
      }
    })
  }

  createProfileJob(datasetName: string, bucket: s3.IBucket, role: iam.IRole) {
    return new databrew.CfnJob(this, 'ProfileJob', {
      name: "databrew-sample-train-profile-job",
      type: "PROFILE",
      jobSample: {
        mode: "FULL_DATASET"
      },
      datasetName: datasetName,
      outputLocation: {
        bucket: bucket.bucketName,
        key: "profile/"
      },
      roleArn: role.roleArn
    })
  }

  createRecipeJob(projectName: string, bucket: s3.IBucket, role: iam.IRole) {
    return new databrew.CfnJob(this, 'RecipeJob', {
      name: "databrew-sample-train-recipe-job",
      type: "RECIPE",
      projectName: projectName,
      outputs: [{
        compressionFormat: "GZIP",
        format: "CSV",
        location: {
          bucket: bucket.bucketName,
          key: "dest/"
        }
      }],
      roleArn: role.roleArn
    })
  }
}
