# airflow-utilities
Variety of custom Hooks, Operators in Airflow.

### Note 📝  
> This is only tested on Airflow version 2.2.2 because that's the version of AWS MWAA.

## Key features
1. `aws.athena.operators.AthenaOperatorExtended`
    - An extension of AthenaSQLOperator that adds the capability to run multiple queries in one statement separated by semicolon.
2. `aws.athena.operators.AthenaSQLCheckOperator`
    - An implementation of SQLCheckOperator for Athena tables.
3. `aws.redshift.operators.RedshiftOperatorExtended`
    - An extension of RedshiftSQLOperator that adds the capability to run multiple queries in one statement separated by semicolon.
4. `aws.transfers.operators.S3toDynamoDBTransferOperator`
    - A custom operator for inserting s3 data to DynamoDB.

