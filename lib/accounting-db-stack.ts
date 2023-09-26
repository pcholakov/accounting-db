import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as lambda_node from "aws-cdk-lib/aws-lambda-nodejs";
import * as path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export class AccountingDbStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const queue = new sqs.Queue(this, "TxWrites", {});

    const table = new dynamodb.Table(this, "AccountsTable", {
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      partitionKey: {
        name: "pk",
        type: dynamodb.AttributeType.STRING,
      },
      sortKey: {
        name: "sk",
        type: dynamodb.AttributeType.STRING,
      },
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const writerFunction = new lambda_node.NodejsFunction(this, "Writer", {
      memorySize: 1024,
      timeout: cdk.Duration.seconds(5),
      runtime: lambda.Runtime.NODEJS_18_X,
      handler: "handler",
      entry: path.join(__dirname, "lambda/writer.ts"),
      environment: {
        QUEUE_NAME: queue.queueName,
        TABLE_NAME: table.tableName,
      },
    });
    table.grantWriteData(writerFunction);
    queue.grantConsumeMessages(writerFunction);

    // Crate event source mapping for queue
    const queueEventSourceMapping = new lambda.EventSourceMapping(this, "QueueEventSourceMapping", {
      eventSourceArn: queue.queueArn,
      batchSize: 1,
      enabled: true,
      target: writerFunction,
    });

    const benchmarkSetupAccounts = new lambda_node.NodejsFunction(this, "CreateAccounts", {
      memorySize: 2048,
      timeout: cdk.Duration.seconds(120),
      runtime: lambda.Runtime.NODEJS_18_X,
      handler: "handler",
      entry: path.join(__dirname, "lambda/create-accounts.ts"),
      environment: {
        TABLE_NAME: table.tableName,
      },
    });
    table.grantWriteData(benchmarkSetupAccounts);

    const benchmarkTransfers = new lambda_node.NodejsFunction(this, "BenchmarkTransfers", {
      memorySize: 8192,
      timeout: cdk.Duration.seconds(600),
      runtime: lambda.Runtime.NODEJS_18_X,
      handler: "handler",
      entry: path.join(__dirname, "lambda/benchmark.ts"),
      environment: {
        TABLE_NAME: table.tableName,
      },
    });
    table.grantWriteData(benchmarkTransfers);
  }
}
