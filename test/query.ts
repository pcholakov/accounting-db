import * as dynamodb from "@aws-sdk/client-dynamodb";
import * as ddc from "@aws-sdk/lib-dynamodb";

const dynamodbClient = new dynamodb.DynamoDBClient({
  region: "localhost",
  endpoint: "http://localhost:8000",
  credentials: {
    accessKeyId: "a",
    secretAccessKey: "k",
  },
});
const documentClient = ddc.DynamoDBDocumentClient.from(dynamodbClient);

async function main() {
  const results = await documentClient.send(new ddc.ScanCommand({ TableName: "transactions" }));
  console.log({ items: results.Items });
}

main();
