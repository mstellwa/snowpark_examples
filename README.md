# snowpark_examples
Various examples and demos primary using Snowpark for Python.

Most of those will use a `creds.json` file, stored in this directory, with the credentials needed to connect to your Snowflake account with the following structure
```json
{
    "account":"MY SNOWFLAKE ACCOUNT",
    "user": "MY USER",
    "password":"MY PASSWORD",
    "role":"MY ROLE",
    "warehouse":"MY WH",
    "database":"MY DB",
    "schema":"MY SCHEMA"
}
```

Source data that is used by the notebooks can be lodaed by running [00_Load_demo_data](https://github.com/mstellwa/snowpark_examples/blob/main/notebooks/00_Load_demo_data.ipynb)
