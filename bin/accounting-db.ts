#!/usr/bin/env node
import "source-map-support/register.js";
import * as cdk from "aws-cdk-lib";
import { AccountingDbStack } from "../lib/accounting-db-stack.js";

const app = new cdk.App();

new AccountingDbStack(app, "AccountingDb", {});
