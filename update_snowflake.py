from data_collector import *
import snowflake.connector
import boto3
from botocore.exceptions import ClientError

def update(awsAccessKeyId, awsSecretAccessKey, snowflake_user, snowflake_password,
           snowflake_account, snowflake_warehouse, snowflake_database, snowflake_schema, storage_aws_role_arn,
           s3_bucket_name,
           stock_price_start_date = datetime.datetime.now().strftime("%m/%d/%Y"), 
           stock_price_end_date   = datetime.datetime.now().strftime("%m/%d/%Y"), 
           stock_price_interval   = '1mo',
           start_over = False):
    stat_file_name            = "stats {} .csv".format(datetime.datetime.now().strftime("%Y-%m-%d  %H %M %S"))
    stock_prices_file_name    = "stock_prices {}.csv".format(datetime.datetime.now().strftime("%Y-%m-%d  %H %M %S"))
    tickers_summary_file_name = "tickers_summary {}.csv".format(datetime.datetime.now().strftime("%Y-%m-%d  %H %M %S"))


    sp500_data = sp500_data_collector()
#     sp500_data.tickers_list = ['AAPL','FB']
    sp500_data.get_ticker_stats()
    sp500_data.get_stock_prices(start_date = stock_price_start_date, end_date = stock_price_end_date, interval = stock_price_interval)

    sp500_data.ticker_stats.to_csv(stat_file_name, index_label='process_date')
    sp500_data.stock_prices.to_csv(stock_prices_file_name, index_label='process_date')
    sp500_data.tickers_summary.to_csv(tickers_summary_file_name, index_label='process_date')

    session = boto3.Session(
        aws_access_key_id     = awsAccessKeyId,
        aws_secret_access_key = awsSecretAccessKey,
    )
    
    s3 = session.resource('s3')
    
    conn = snowflake.connector.connect(
        user     = snowflake_user,
        password = snowflake_password,
        account  = snowflake_account,
        warehouse= snowflake_warehouse,
        database = snowflake_database,
        schema   = snowflake_schema)

    if start_over:
        print("emptying S3 bucket")
        s3.Bucket(s3_bucket_name).objects.all().delete()
        
        print("Use accountadmin role in snowflake")
        conn.cursor().execute("use role accountadmin")
        
        print("create integration")
        conn.cursor().execute(
        """
         create storage integration IF NOT EXISTS s3_int
           type = external_stage
          storage_provider = s3
          enabled = true
          storage_aws_role_arn = '{}'
          storage_allowed_locations = ('s3://{}')
        """.format(storage_aws_role_arn, s3_bucket_name)
        )
        
        print("create stages")
        conn.cursor().execute(
        """
        create or replace stage stat_stage
          url = 's3://{}/stat'
          storage_integration = s3_int;
        """.format(s3_bucket_name)
        )

        conn.cursor().execute(
        """
        create or replace stage stock_prices_stage
          url = 's3://{}/stock_prices'
          storage_integration = s3_int;
        """.format(s3_bucket_name)
        )

        conn.cursor().execute(
        """
        create or replace stage tickers_summary_stage
          url = 's3://{}/tickers_summary'
          storage_integration = s3_int;
        """.format(s3_bucket_name)
        )
        
        print("create tables")
        conn.cursor().execute(
        """
        CREATE OR REPLACE TABLE stats (
          process_date                     string,
          Market_Cap_intraday_5            string,
          Enterprise_Value_3               string,
          Trailing_P_over_E                string,
          Forward_P_over_E_1               string,
          PEG_Ratio_5_yr_expected_1        string,
          Price_over_Sales_ttm             string,
          Price_over_Book_mrq              string,
          Enterprise_Value_over_Revenue_3  string,
          Enterprise_Value_over_EBITDA_6   string,
          ticker                           string,
          create_date                      string
        );
        """
        )

        conn.cursor().execute(
        """
        CREATE OR REPLACE TABLE stock_prices (
          process_date   string,
          open           string,
          high           string,
          low            string,
          close          string,
          adjclose       string,
          volume         string,
          ticker         string,
          create_date    string
        );
        """
        )

        conn.cursor().execute(
        """    
        CREATE OR REPLACE TABLE ticker_summary (
          process_date           string, 
          Symbol                 string, 
          Security               string, 
          SEC_filings            string, 
          GICS_Sector            string, 
          GICS_Sub_Industry      string, 
          Headquarters_Location  string, 
          Date_first_added       string, 
          CIK                    string, 
          Founded                string
        );
        """
        )

        print("create pipes")
        conn.cursor().execute(
        """
        create or replace pipe {}.{}.stats_pipe auto_ingest=true as
          copy into {}.{}.stats
          from @{}.{}.stat_stage
          file_format = (type = 'CSV' skip_header = 1);
        """.format(snowflake_database,snowflake_schema,snowflake_database,snowflake_schema,snowflake_database,snowflake_schema)
        )

        conn.cursor().execute(
        """    
        create or replace pipe {}.{}.stock_prices_pipe auto_ingest=true as
          copy into {}.{}.stock_prices
          from @{}.{}.stock_prices_stage
          file_format = (type = 'CSV' skip_header = 1);
        """.format(snowflake_database,snowflake_schema,snowflake_database,snowflake_schema,snowflake_database,snowflake_schema)
        )

        conn.cursor().execute(
        """
        create or replace pipe {}.{}.ticker_summary_pipe auto_ingest=true as
          copy into {}.{}.ticker_summary
          from @{}.{}.tickers_summary_stage
          file_format = (type = 'CSV' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY='"');
        """.format(snowflake_database,snowflake_schema,snowflake_database,snowflake_schema,snowflake_database,snowflake_schema)
        )
        
        print("create views")
        conn.cursor().execute(
        """
        create or replace view stats_view as 
        select 
        TICKER,
        SECURITY,
        SEC_FILINGS,
        GICS_SECTOR,
        GICS_SUB_INDUSTRY,
        HEADQUARTERS_LOCATION,
        DATE_FIRST_ADDED,
        CIK,
        FOUNDED,
        try_to_decimal(TRAILING_P_OVER_E, 38, 8) as TRAILING_P_OVER_E,
        try_to_decimal(FORWARD_P_OVER_E_1, 38, 8) as FORWARD_P_OVER_E_1,
        try_to_decimal(PEG_RATIO_5_YR_EXPECTED_1, 38, 8) as PEG_RATIO_5_YR_EXPECTED_1,
        try_to_decimal(PRICE_OVER_SALES_TTM, 38, 8) as PRICE_OVER_SALES_TTM,
        try_to_decimal(PRICE_OVER_BOOK_MRQ, 38, 8) as PRICE_OVER_BOOK_MRQ,
        try_to_decimal(ENTERPRISE_VALUE_OVER_REVENUE_3, 38, 8) as ENTERPRISE_VALUE_OVER_REVENUE_3,
        try_to_decimal(ENTERPRISE_VALUE_OVER_EBITDA_6, 38, 8) as ENTERPRISE_VALUE_OVER_EBITDA_6,
        MARKET_CAP_INTRADAY_5,
        ENTERPRISE_VALUE_3,
        case when MARKET_CAP_INTRADAY_5 like '%B%' then try_to_decimal(replace(MARKET_CAP_INTRADAY_5,'B',''), 38, 8)*1000000000 
             when MARKET_CAP_INTRADAY_5 like '%T%' then try_to_decimal(replace(MARKET_CAP_INTRADAY_5,'T',''), 38, 8)*1000000000000 end as MARKET_CAP_INTRADAY_5_EXPANDED,
        case when ENTERPRISE_VALUE_3 like '%B%' then try_to_decimal(replace(ENTERPRISE_VALUE_3,'B',''), 38, 8)*1000000000 
             when ENTERPRISE_VALUE_3 like '%T%' then try_to_decimal(replace(ENTERPRISE_VALUE_3,'T',''), 38, 8)*1000000000000 end as ENTERPRISE_VALUE_3_EXPANDED
        from (select * from (select *,row_number() over (partition by TICKER order by CREATE_DATE desc) as rn from stats) where rn = 1) a left join ticker_summary b 
        on a.ticker = b.symbol;
        """
        )


        conn.cursor().execute(
        """
        create or replace view stock_prices_view as 
        select 
        a.PROCESS_DATE,
        try_to_decimal(OPEN, 38, 8) as OPEN,
        try_to_decimal(HIGH, 38, 8) as HIGH,
        try_to_decimal(LOW, 38, 8) as LOW,
        try_to_decimal(CLOSE, 38, 8) as CLOSE,
        try_to_decimal(ADJCLOSE, 38, 8) as ADJCLOSE,
        try_to_decimal(ADJCLOSE, 38, 8)/(lag(try_to_decimal(ADJCLOSE, 38, 8)) over (partition by TICKER order by a.PROCESS_DATE)) as ADJCLOSE_growth,
        try_to_decimal(VOLUME, 38, 8) as VOLUME,
        TICKER,
        SECURITY,
        SEC_FILINGS,
        GICS_SECTOR,
        GICS_SUB_INDUSTRY,
        HEADQUARTERS_LOCATION,
        DATE_FIRST_ADDED,
        CIK,
        FOUNDED
        from 
        (select distinct PROCESS_DATE,OPEN,HIGH,LOW,CLOSE,ADJCLOSE,VOLUME,TICKER from stock_prices)
        a left join ticker_summary b 
        on a.ticker = b.symbol;
        """
        )
        
        conn.cursor().execute(
        """           
        create or replace view {}.{}.UNIQUE_DATE_VIEW as 
        select 
        distinct PROCESS_DATE as PROCESS_DATE
        from stock_prices;       
        """.format(snowflake_database,snowflake_schema)
        )      
    else:
        print('emptying tickers_summary folder in the s3 bucket')
        s3.Bucket(s3_bucket_name).objects.filter(Prefix="tickers_summary/").delete()
        conn.cursor().execute(
        """    
        CREATE OR REPLACE TABLE ticker_summary (
          process_date           string, 
          Symbol                 string, 
          Security               string, 
          SEC_filings            string, 
          GICS_Sector            string, 
          GICS_Sub_Industry      string, 
          Headquarters_Location  string, 
          Date_first_added       string, 
          CIK                    string, 
          Founded                string
        );
        """
        )
    s3.meta.client.upload_file(stat_file_name, s3_bucket_name, 'stat/{}'.format(stat_file_name))
    s3.meta.client.upload_file(stock_prices_file_name, s3_bucket_name, 'stock_prices/{}'.format(stock_prices_file_name))
    s3.meta.client.upload_file(tickers_summary_file_name, s3_bucket_name, 'tickers_summary/{}'.format(tickers_summary_file_name))
