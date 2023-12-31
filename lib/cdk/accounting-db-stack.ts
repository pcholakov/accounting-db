import * as cdk from "aws-cdk-lib";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as lambda_node from "aws-cdk-lib/aws-lambda-nodejs";
import { Construct } from "constructs";
import * as path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export class AccountingDbStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    /// On-demand gives us adaptive capacity that scales down to zero when not
    /// in use; you should get a baseline of 10,000 WCU which allows up to ~3333
    /// transfers/second. Sustained usage will cause DynamoDB to raise the limit
    /// at increments of 30 minutes.
    const onDemandBilling = {
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
    };

    /// Provisioned throughput allows us to specify the required capacity
    /// upfront. You will pay for this capacity it until the table is deleted,
    /// in full hour increments. If you re-create the table at a different
    /// capacity, you should only be billed for the maximum _active_ provisioned
    /// capacity within a given hour.
    // const provisionedThroughputBilling = {
    //   billingMode: dynamodb.BillingMode.PROVISIONED,
    //   writeCapacity: 20_000,
    //   readCapacity: 5,
    // };

    const table = new dynamodb.Table(this, "AccountsTable", {
      ...onDemandBilling,

      partitionKey: {
        name: "pk",
        type: dynamodb.AttributeType.STRING,
      },
      sortKey: {
        name: "sk",
        type: dynamodb.AttributeType.STRING,
      },

      contributorInsightsEnabled: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY, // Don't do this with real data you care about!
    });

    const benchmarkTransfers = new lambda_node.NodejsFunction(this, "BenchmarkTransfers", {
      memorySize: 4096,
      timeout: cdk.Duration.seconds(600),
      runtime: lambda.Runtime.NODEJS_LATEST,
      handler: "handler",
      entry: path.join(__dirname, "../lambda/benchmark.ts"),
      environment: {
        TABLE_NAME: table.tableName,
      },
    });
    table.grantReadWriteData(benchmarkTransfers);

    new cdk.CfnOutput(this, "BenchmarkFunctionName", {
      value: benchmarkTransfers.functionName,
    });
  }
}
