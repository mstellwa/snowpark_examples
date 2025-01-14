import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import FloatType

def generate_demo_data(session, num_customers=1000, ltv_multiplier=1, session_length_multiplier=1, month=5, start_year=2024, end_year=2024):
    random_id = F.uniform(0,5, F.random()).as_('RAND_ID')
    email = F.concat(F.call_builtin('RANDSTR', 10, F.random()), F.lit('@'), F.call_builtin('RANDSTR', 5, F.random()), F.lit('.com')).as_('EMAIL')
    gender = F.when(F.uniform(1,10,F.random())<=7, F.lit('MALE')).otherwise('FEMALE').as_('GENDER')
    LIFE_TIME_VALUE = (F.round(F.uniform(100,75000,F.random()) / 100, 2) * ltv_multiplier).as_('LIFE_TIME_VALUE')
    month_col = (F.lit(str(end_year) + str(month))).as_('YEAR_MONTH')
    membership_status = F.when(col('LIFE_TIME_VALUE') < 150, F.lit('BASIC'))\
        .when(col('LIFE_TIME_VALUE') < 250, F.lit('BRONZE'))\
            .when(col('LIFE_TIME_VALUE') < 350, F.lit('SILVER'))\
                .when(col('LIFE_TIME_VALUE') < 550, F.lit('GOLD'))\
                    .when(col('LIFE_TIME_VALUE') < 650, F.lit('PLATIN'))\
                        .when(col('LIFE_TIME_VALUE') >= 650, F.lit('DIAMOND')).as_('MEMBERSHIP_STATUS')
    membership_year = F.uniform(start_year, end_year,F.random(3)).as_("YEAR")
    #membership_length = F.date_from_parts(F.uniform(start_year, end_year,F.random(3))
    #                                      ,F.uniform(start_month,end_month,F.random(2))
    #                                      ,F.uniform(1,28,F.random(1))).as_('MEMBER_JOIN_DATE')
    avg_session_length = (col('LIFE_TIME_VALUE') / 100 + F.uniform(0,5, F.random()) * session_length_multiplier).cast(FloatType()).as_('AVG_SESSION_LENGTH_MIN')
    avg_time_on_app = (col('LIFE_TIME_VALUE') / 100 + F.uniform(1,7, F.random())).cast(FloatType()).as_('AVG_TIME_ON_APP_MIN')
    avg_time_on_website = (col('LIFE_TIME_VALUE') / 100 + F.uniform(3,7, F.random())).cast(FloatType()).as_('AVG_TIME_ON_WEBSITE_MIN')

    df = session.generator(random_id, email, month_col, LIFE_TIME_VALUE, gender,membership_status, membership_year, avg_session_length, avg_time_on_app, avg_time_on_website, rowcount=num_customers)

    # Add a month column, that is the month when the 
    df = df.with_column('MEMBER_JOIN_DATE', F.date_from_parts(col('YEAR'), F.iff(col("YEAR") <  end_year
                                                                                  , F.uniform(1, 12 ,F.random(2))
                                                                                  , F.uniform(1, month, F.random(2)))
                                                                        , F.uniform(1,28,F.random(1))))
    # Add some missing data
    df = df.with_column('AVG_SESSION_LENGTH_MIN', F.when(col('RAND_ID') == 2, None).otherwise(col('AVG_SESSION_LENGTH_MIN')))
    df = df.with_column('AVG_TIME_ON_APP_MIN', F.when(col('RAND_ID') == 3, None).otherwise(col('AVG_TIME_ON_APP_MIN')))
    df = df.with_column('AVG_TIME_ON_WEBSITE_MIN', F.when(col('RAND_ID') == 4, None).otherwise(col('AVG_TIME_ON_WEBSITE_MIN')))
    df = df.drop(['RAND_ID', 'YEAR']).cache_result()

    # Save Tables
    ltv_table_name = f'CUSTOMER_LIFE_TIME_VALUE'
    generaldata_table_name = f'CUSTOMER_GENERAL_DATA'
    behaviordata_table_name = f'CUSTOMER_BEHAVIOR_DATA'
    df[['EMAIL','LIFE_TIME_VALUE','YEAR_MONTH']].write.save_as_table(ltv_table_name, mode='append')
    print(f'Added {num_customers} customers to table: {ltv_table_name} for YEAR_MONTH: {str(end_year) + str(month)}')
    df[['EMAIL','GENDER','MEMBERSHIP_STATUS','MEMBER_JOIN_DATE']].write.save_as_table(generaldata_table_name, mode='append')
    print(f'Added {num_customers} customers to table: {generaldata_table_name}')
    df[['EMAIL','AVG_SESSION_LENGTH_MIN','AVG_TIME_ON_APP_MIN','AVG_TIME_ON_WEBSITE_MIN']].write.save_as_table(behaviordata_table_name, mode='append')
    print(f'Added {num_customers} customers to table: {behaviordata_table_name}')