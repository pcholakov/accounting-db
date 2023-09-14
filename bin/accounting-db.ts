#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "aws-cdk-lib";
import { AccountingDbStack } from "../lib/accounting-db-stack";

const app = new cdk.App();
new AccountingDbStack(app, "AccountingDb", {});
