-- Create a Snowpark Image Repository in the current Database and Schema
CREATE IMAGE REPOSITORY IF NOT EXISTS images;

-- Get a list of existing imgae repositories in the current database and schema 
SHOW IMAGE REPOSITORIES IN SCHEMA;

-- Stage for specification file(s)
CREATE STAGE IF NOT EXISTS specs
ENCRYPTION = (TYPE='SNOWFLAKE_SSE');

-- Stage for storing our r-model
CREATE STAGE IF NOT EXISTS r_models
ENCRYPTION = (TYPE='SNOWFLAKE_SSE');

-- Create a compute pool, compute pools are account object so the name needs to unique for the account
CREATE COMPUTE POOL UDF_STANDARD2_POOL
with
instance_family=STANDARD_2
min_nodes=1
max_nodes=1
AUTO_SUSPEND_SECS=1200;

-- Create a Snowpark Container Service in the current database and schema
create service r_churn_api
in compute pool UDF_STANDARD2_POOL
from @specs
spec='r-churn-api.yaml';

-- Get information about the compute pool ie to chek if it is running for exampel
DESCRIBE COMPUTE POOL UDF_STANDARD2_POOL;

-- Get information about the service
DESCRIBE SERVICE r_churn_api;

-- Get the logs from the container used by the service
CALL SYSTEM$GET_SERVICE_LOGS('r_churn_api', '0', 'r-churn-api', 10);


-- Create a UDF that uses the service
CREATE OR REPLACE FUNCTION r_churn_udf(CreditScore float, Geography varchar
                                    , Gender varchar, Age INTEGER
                                    , Tenure INTEGER, Balance float
                                    , NumOfProducts INTEGER, HasCrCard INTEGER
                                    , IsActiveMember INTEGER
                                    , EstimatedSalary float)
RETURNS OBJECT
SERVICE=r_churn_api
ENDPOINT=rchurnendpoint
AS '/predict';

-- Test the UDF
SELECT r_churn_udf(CreditScore, Geography, Gender, Age, Tenure, Balance
                  , NumOfProducts, HasCrCard, IsActiveMember, EstimatedSalary) 
          AS CHURN_PREDICTIONS
FROM (VALUES (608, 'Spain', 'Female', 41, 1, 83807.86, 1, 0, 1, 112542.5),
              (556, 'France', 'Male', 61, 2, 117419.35, 1, 1, 1, 94153.83)
      AS v1 (CreditScore, Geography, Gender, Age, Tenure, Balance
            , NumOfProducts, HasCrCard, IsActiveMember, EstimatedSalary));
            

-- Suspend the service
ALTER SERVCIE r_churn_api SUSPEND;

-- Suspend the COMPUTE POOL
ALTER COMPUTE POOL UDF_STANDARD2_POOL SUSPEND;
