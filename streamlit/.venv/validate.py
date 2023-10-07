#!/usr/bin/env python
import streamlit as st
import pandas as pd
import numpy as np

import os
os.environ["SNOWFLAKE_DISABLE_ARROW"] = "1"

from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

engine = create_engine(URL(
    account = 'dgmgrmk-ug64073',
    user = 'Zevil',
    password = '17Ch10012',
    database = 'SNOWFLAKE_SAMPLE_DATA',
    schema = 'TPCDS_SF10TCL',
    warehouse = 'COMPUTE_WH',
    role='SYSADMIN',
))

connection = engine.connect()

# Streamlit app
st.title('Assignment1: Snowflake Queries')
#st.set_page_config(page_title='Assignment1: Snowflake Queries')
st.header('Ten Queries:')

tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10 = st.tabs(["Query 61", "Query 62", "Query 63", "Query 64", "Query 65", "Query 66", "Query 67", "Query 68", "Query 69", "Query 70"])

# Query to get unique years
y_query = "SELECT DISTINCT (d_year) FROM date_dim"
results = connection.execute(y_query)
# Extract years 
distinct_years = [row[0] for row in results]

m_query= "SELECT DISTINCT (d_moy) FROM date_dim"
results = connection.execute(m_query)
# Extract years 
distinct_months = [row[0] for row in results]


c_query= "SELECT DISTINCT (i_category) FROM item"
results = connection.execute(c_query)
# Extract years 
distinct_category = [row[0] for row in results]

city_q="SELECT DISTINCT (s_city) FROM store"
results = connection.execute(city_q)
# Extract years 
distinct_city = [row[0] for row in results]

with tab1:
   # Query 61
   def run_q61(x1,y1,z1):
    sql_query = f"select promotions,total,cast(promotions as decimal(17,4))/cast(total as decimal(17,4))*100 from (select sum(ss_ext_sales_price) promotions from store_sales, store, promotion, date_dim, customer, customer_address, item where ss_sold_date_sk = d_date_sk and ss_store_sk = s_store_sk and ss_promo_sk = p_promo_sk and ss_customer_sk= c_customer_sk and ca_address_sk = c_current_addr_sk and ss_item_sk = i_item_sk and ca_gmt_offset = -6 and i_category = '{x1}' and (p_channel_dmail = 'Y' or p_channel_email = 'Y' or p_channel_tv = 'Y') and s_gmt_offset = -6 and d_year = {y1} and d_moy = {z1}) promotional_sales, (select sum(ss_ext_sales_price) total from store_sales, store, date_dim, customer, customer_address, item where ss_sold_date_sk = d_date_sk and ss_store_sk = s_store_sk and ss_customer_sk= c_customer_sk and ca_address_sk = c_current_addr_sk and ss_item_sk = i_item_sk and ca_gmt_offset = -6 and i_category = '{x1}' and s_gmt_offset = -6 and d_year = {y1} and d_moy = {z1}) all_sales order by promotions, total limit 100;"
    return sql_query


   # Populate selectbox  
   x1 = st.selectbox("Category", distinct_category)
   y1 = st.selectbox("Year", distinct_years)
   z1 = st.selectbox("Month", distinct_months)
   if st.button("Run Query"):
    # Generate query
    query = run_q61(x1, y1, z1) 
    # Execute query
    results = connection.execute(query)
    # Convert to DataFrame
    df = pd.DataFrame(results.fetchall())
    st.subheader('Query 61')
    st.write('Find the ratio of items sold with and without promotions in a given month and year. Only items in certain categories sold to customers living in a specific time zone are considered.')
    st.divider()

    # Display results
    st.dataframe(df)


with tab2:
   # Query 62
   query= f"select substr(w_warehouse_name,1,20),sm_type ,web_name ,sum(case when (ws_ship_date_sk - ws_sold_date_sk <= 30 ) then 1 else 0 end)  as days_30 ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 30) and (ws_ship_date_sk - ws_sold_date_sk <= 60) then 1 else 0 end )  as days_31_60 ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 60) and (ws_ship_date_sk - ws_sold_date_sk <= 90) then 1 else 0 end)  as days_61_90 ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 90) and (ws_ship_date_sk - ws_sold_date_sk <= 120) then 1 else 0 end)  as days_91_120 ,sum(case when (ws_ship_date_sk - ws_sold_date_sk  > 120) then 1 else 0 end)  as over_120_days from web_sales ,warehouse ,ship_mode ,web_site ,date_dim where d_month_seq between 1214 and 1214 + 11 and ws_ship_date_sk   = d_date_sk and ws_warehouse_sk   = w_warehouse_sk and ws_ship_mode_sk   = sm_ship_mode_sk and ws_web_site_sk    = web_site_sk group by substr(w_warehouse_name,1,20) ,sm_type ,web_name order by substr(w_warehouse_name,1,20) ,sm_type ,web_name limit 100;"

   results = connection.execute(query)  
   df = pd.DataFrame(results.fetchall())
   st.subheader('Query 62')
   st.write('For web sales, create a report showing the counts of orders shipped within 30 days, from 31 to 60 days, from 61 to 90 days, from 91 to 120 days and over 120 days within a given year, grouped by warehouse, shipping mode and web site')
   st.divider()
   # Display as table
   st.dataframe(df)


with tab3:
   # Query 63
   query= f"select  * from (select i_manager_id ,sum(ss_sales_price) sum_sales ,avg(sum(ss_sales_price)) over (partition by i_manager_id) avg_monthly_sales from item ,store_sales ,date_dim ,store where ss_item_sk = i_item_sk and ss_sold_date_sk = d_date_sk and ss_store_sk = s_store_sk and d_month_seq in (1188,1188+1,1188+2,1188+3,1188+4,1188+5,1188+6,1188+7,1188+8,1188+9,1188+10,1188+11) and ((    i_category in ('Books','Children','Electronics') and i_class in ('personal','portable','reference','self-help') and i_brand in ('scholaramalgamalg #14','scholaramalgamalg #7', 'exportiunivamalg #9','scholaramalgamalg #9')) or(    i_category in ('Women','Music','Men') and i_class in ('accessories','classical','fragrances','pants') and i_brand in ('amalgimporto #1','edu packscholar #1','exportiimporto #1', 'importoamalg #1'))) group by i_manager_id, d_moy) tmp1 where case when avg_monthly_sales > 0 then abs (sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1 order by i_manager_id ,avg_monthly_sales ,sum_sales limit 100;"
   results = connection.execute(query)  
   df = pd.DataFrame(results.fetchall())
   st.subheader('Query 63')
   st.write('For a given year calculate the monthly sales of items of specific categories, classes and brands that were sold in stores and group the results by store manager. Additionally, for every month and manager print the yearly average sales of those items.')
   st.divider()
   # Display as table
   st.dataframe(df)

with tab4:
   # Query 64
   def run_q64(x4):
    query=f"with cs_ui as (select cs_item_sk ,sum(cs_ext_list_price) as sale,sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit) as refund from catalog_sales ,catalog_returns where cs_item_sk = cr_item_sk and cs_order_number = cr_order_number group by cs_item_sk having sum(cs_ext_list_price)>2*sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit)), cross_sales as (select i_product_name product_name ,i_item_sk item_sk ,s_store_name store_name ,s_zip store_zip ,ad1.ca_street_number b_street_number ,ad1.ca_street_name b_street_name ,ad1.ca_city b_city ,ad1.ca_zip b_zip ,ad2.ca_street_number c_street_number ,ad2.ca_street_name c_street_name ,ad2.ca_city c_city ,ad2.ca_zip c_zip ,d1.d_year as syear ,d2.d_year as fsyear ,d3.d_year s2year ,count(*) cnt ,sum(ss_wholesale_cost) s1 ,sum(ss_list_price) s2 ,sum(ss_coupon_amt) s3 FROM   store_sales ,store_returns ,cs_ui ,date_dim d1 ,date_dim d2 ,date_dim d3 ,store ,customer ,customer_demographics cd1 ,customer_demographics cd2 ,promotion ,household_demographics hd1 ,household_demographics hd2 ,customer_address ad1 ,customer_address ad2 ,income_band ib1 ,income_band ib2 ,item WHERE  ss_store_sk = s_store_sk AND ss_sold_date_sk = d1.d_date_sk AND ss_customer_sk = c_customer_sk AND ss_cdemo_sk= cd1.cd_demo_sk AND ss_hdemo_sk = hd1.hd_demo_sk AND ss_addr_sk = ad1.ca_address_sk and ss_item_sk = i_item_sk and ss_item_sk = sr_item_sk and ss_ticket_number = sr_ticket_number and ss_item_sk = cs_ui.cs_item_sk and c_current_cdemo_sk = cd2.cd_demo_sk AND c_current_hdemo_sk = hd2.hd_demo_sk AND c_current_addr_sk = ad2.ca_address_sk and c_first_sales_date_sk = d2.d_date_sk and c_first_shipto_date_sk = d3.d_date_sk and ss_promo_sk = p_promo_sk and hd1.hd_income_band_sk = ib1.ib_income_band_sk and hd2.hd_income_band_sk = ib2.ib_income_band_sk and cd1.cd_marital_status <> cd2.cd_marital_status and i_color in ('burlywood','coral','dark','puff','smoke','peach') and i_current_price between 22 and 22 + 10 and i_current_price between 22 + 1 and 22 + 15 group by i_product_name ,i_item_sk ,s_store_name ,s_zip ,ad1.ca_street_number ,ad1.ca_street_name ,ad1.ca_city ,ad1.ca_zip ,ad2.ca_street_number ,ad2.ca_street_name ,ad2.ca_city ,ad2.ca_zip ,d1.d_year ,d2.d_year ,d3.d_year ) select cs1.product_name ,cs1.store_name ,cs1.store_zip ,cs1.b_street_number ,cs1.b_street_name ,cs1.b_city ,cs1.b_zip ,cs1.c_street_number ,cs1.c_street_name ,cs1.c_city ,cs1.c_zip ,cs1.syear ,cs1.cnt ,cs1.s1 as s11 ,cs1.s2 as s21 ,cs1.s3 as s31 ,cs2.s1 as s12 ,cs2.s2 as s22 ,cs2.s3 as s32 ,cs2.syear as syear_2 ,cs2.cnt as cnt_2 from cross_sales cs1,cross_sales cs2 where cs1.item_sk=cs2.item_sk and cs1.syear = {x4} and syear_2 = {x4} + 1 and cnt_2 <= cs1.cnt and cs1.store_name = cs2.store_name and cs1.store_zip = cs2.store_zip order by cs1.product_name ,cs1.store_name ,cs2.cnt ,cs1.s1 ,cs2.s1 LIMIT 100;"
    return query

   # Populate selectbox  
  
   x4 = st.selectbox("YEAR", distinct_years)
  
   if st.button("RUN"):
    # Generate query
    query = run_q64(x4) 
    # Execute query
    results = connection.execute(query)
    # Convert to DataFrame
    df = pd.DataFrame(results.fetchall())
    st.subheader('Query 64')
    st.write('Find those stores that sold more cross-sales items from one year to another. Cross-sale items are items that are sold over the Internet, by catalog and in store')
    st.divider()

    # Display results
    st.dataframe(df)

with tab5:
   # Query 65
   query = (
    "SELECT "
    "    s_store_name, "
    "    i_item_desc, "
    "    sc.revenue, "
    "    i_current_price, "
    "    i_wholesale_cost, "
    "    i_brand "
    "FROM "
    "    store, item, "
    "    (SELECT ss_store_sk, avg(revenue) as ave "
    "     FROM "
    "         (SELECT  ss_store_sk, ss_item_sk, "
    "                  sum(ss_sales_price) as revenue "
    "          FROM store_sales, date_dim "
    "          WHERE ss_sold_date_sk = d_date_sk and d_month_seq between 1192 and 1192+11 "
    "          GROUP BY ss_store_sk, ss_item_sk) sa "
    "     GROUP BY ss_store_sk) sb, "
    "    (SELECT ss_store_sk, ss_item_sk, sum(ss_sales_price) as revenue "
    "     FROM store_sales, date_dim "
    "     WHERE ss_sold_date_sk = d_date_sk and d_month_seq between 1192 and 1192+11 "
    "     GROUP BY ss_store_sk, ss_item_sk) sc "
    "WHERE sb.ss_store_sk = sc.ss_store_sk and "
    "      sc.revenue <= 0.1 * sb.ave and "
    "      s_store_sk = sc.ss_store_sk and "
    "      i_item_sk = sc.ss_item_sk "
    "ORDER BY s_store_name, i_item_desc "
    "LIMIT 100;"
   )

   results = connection.execute(query)  
   df = pd.DataFrame(results.fetchall())
   st.subheader('Query 65')
   st.write('In a given period, for each store, report the list of items with revenue less than 10% the average revenue for all the items in that store.')
   st.divider()
   # Display as table
   st.dataframe(df)

with tab6:
    #query66
    def run_q66(x6):
        query=f"select w_warehouse_name ,w_warehouse_sq_ft ,w_city ,w_county ,w_state ,w_country ,ship_carriers ,year ,sum(jan_sales) as jan_sales ,sum(feb_sales) as feb_sales ,sum(mar_sales) as mar_sales ,sum(apr_sales) as apr_sales ,sum(may_sales) as may_sales ,sum(jun_sales) as jun_sales ,sum(jul_sales) as jul_sales ,sum(aug_sales) as aug_sales ,sum(sep_sales) as sep_sales ,sum(oct_sales) as oct_sales ,sum(nov_sales) as nov_sales ,sum(dec_sales) as dec_sales ,sum(jan_sales/w_warehouse_sq_ft) as jan_sales_per_sq_foot ,sum(feb_sales/w_warehouse_sq_ft) as feb_sales_per_sq_foot ,sum(mar_sales/w_warehouse_sq_ft) as mar_sales_per_sq_foot ,sum(apr_sales/w_warehouse_sq_ft) as apr_sales_per_sq_foot ,sum(may_sales/w_warehouse_sq_ft) as may_sales_per_sq_foot ,sum(jun_sales/w_warehouse_sq_ft) as jun_sales_per_sq_foot ,sum(jul_sales/w_warehouse_sq_ft) as jul_sales_per_sq_foot ,sum(aug_sales/w_warehouse_sq_ft) as aug_sales_per_sq_foot ,sum(sep_sales/w_warehouse_sq_ft) as sep_sales_per_sq_foot ,sum(oct_sales/w_warehouse_sq_ft) as oct_sales_per_sq_foot ,sum(nov_sales/w_warehouse_sq_ft) as nov_sales_per_sq_foot ,sum(dec_sales/w_warehouse_sq_ft) as dec_sales_per_sq_foot ,sum(jan_net) as jan_net ,sum(feb_net) as feb_net ,sum(mar_net) as mar_net ,sum(apr_net) as apr_net ,sum(may_net) as may_net ,sum(jun_net) as jun_net ,sum(jul_net) as jul_net ,sum(aug_net) as aug_net ,sum(sep_net) as sep_net ,sum(oct_net) as oct_net ,sum(nov_net) as nov_net ,sum(dec_net) as dec_net from ( select w_warehouse_name ,w_warehouse_sq_ft ,w_city ,w_county ,w_state ,w_country ,'GREAT EASTERN' || ',' || 'UPS' as ship_carriers ,d_year as year ,sum(case when d_moy = 1 then ws_ext_sales_price* ws_quantity else 0 end) as jan_sales ,sum(case when d_moy = 2 then ws_ext_sales_price* ws_quantity else 0 end) as feb_sales ,sum(case when d_moy = 3 then ws_ext_sales_price* ws_quantity else 0 end) as mar_sales ,sum(case when d_moy = 4 then ws_ext_sales_price* ws_quantity else 0 end) as apr_sales ,sum(case when d_moy = 5 then ws_ext_sales_price* ws_quantity else 0 end) as may_sales ,sum(case when d_moy = 6 then ws_ext_sales_price* ws_quantity else 0 end) as jun_sales ,sum(case when d_moy = 7 then ws_ext_sales_price* ws_quantity else 0 end) as jul_sales ,sum(case when d_moy = 8 then ws_ext_sales_price* ws_quantity else 0 end) as aug_sales ,sum(case when d_moy = 9 then ws_ext_sales_price* ws_quantity else 0 end) as sep_sales ,sum(case when d_moy = 10 then ws_ext_sales_price* ws_quantity else 0 end) as oct_sales ,sum(case when d_moy = 11 then ws_ext_sales_price* ws_quantity else 0 end) as nov_sales ,sum(case when d_moy = 12 then ws_ext_sales_price* ws_quantity else 0 end) as dec_sales ,sum(case when d_moy = 1 then ws_net_paid * ws_quantity else 0 end) as jan_net ,sum(case when d_moy = 2 then ws_net_paid * ws_quantity else 0 end) as feb_net ,sum(case when d_moy = 3 then ws_net_paid * ws_quantity else 0 end) as mar_net ,sum(case when d_moy = 4 then ws_net_paid * ws_quantity else 0 end) as apr_net ,sum(case when d_moy = 5 then ws_net_paid * ws_quantity else 0 end) as may_net ,sum(case when d_moy = 6 then ws_net_paid * ws_quantity else 0 end) as jun_net ,sum(case when d_moy = 7 then ws_net_paid * ws_quantity else 0 end) as jul_net ,sum(case when d_moy = 8 then ws_net_paid * ws_quantity else 0 end) as aug_net ,sum(case when d_moy = 9 then ws_net_paid * ws_quantity else 0 end) as sep_net ,sum(case when d_moy = 10 then ws_net_paid * ws_quantity else 0 end) as oct_net ,sum(case when d_moy = 11 then ws_net_paid * ws_quantity else 0 end) as nov_net ,sum(case when d_moy = 12 then ws_net_paid * ws_quantity else 0 end) as dec_net from web_sales ,warehouse ,date_dim ,time_dim ,ship_mode where ws_warehouse_sk =  w_warehouse_sk and ws_sold_date_sk = d_date_sk and ws_sold_time_sk = t_time_sk and ws_ship_mode_sk = sm_ship_mode_sk and d_year = {x6} and t_time between 46866 and 46866+28800 and sm_carrier in ('GREAT EASTERN','UPS') group by w_warehouse_name ,w_warehouse_sq_ft ,w_city ,w_county ,w_state ,w_country ,d_year union all select w_warehouse_name ,w_warehouse_sq_ft ,w_city ,w_county ,w_state ,w_country ,'GREAT EASTERN' || ',' || 'UPS' as ship_carriers ,d_year as year ,sum(case when d_moy = 1 then cs_sales_price* cs_quantity else 0 end) as jan_sales ,sum(case when d_moy = 2 then cs_sales_price* cs_quantity else 0 end) as feb_sales ,sum(case when d_moy = 3 then cs_sales_price* cs_quantity else 0 end) as mar_sales ,sum(case when d_moy = 4 then cs_sales_price* cs_quantity else 0 end) as apr_sales ,sum(case when d_moy = 5 then cs_sales_price* cs_quantity else 0 end) as may_sales ,sum(case when d_moy = 6 then cs_sales_price* cs_quantity else 0 end) as jun_sales ,sum(case when d_moy = 7 then cs_sales_price* cs_quantity else 0 end) as jul_sales ,sum(case when d_moy = 8 then cs_sales_price* cs_quantity else 0 end) as aug_sales ,sum(case when d_moy = 9 then cs_sales_price* cs_quantity else 0 end) as sep_sales ,sum(case when d_moy = 10 then cs_sales_price* cs_quantity else 0 end) as oct_sales ,sum(case when d_moy = 11 then cs_sales_price* cs_quantity else 0 end) as nov_sales ,sum(case when d_moy = 12 then cs_sales_price* cs_quantity else 0 end) as dec_sales ,sum(case when d_moy = 1 then cs_net_paid * cs_quantity else 0 end) as jan_net ,sum(case when d_moy = 2 then cs_net_paid * cs_quantity else 0 end) as feb_net ,sum(case when d_moy = 3 then cs_net_paid * cs_quantity else 0 end) as mar_net ,sum(case when d_moy = 4 then cs_net_paid * cs_quantity else 0 end) as apr_net ,sum(case when d_moy = 5 then cs_net_paid * cs_quantity else 0 end) as may_net ,sum(case when d_moy = 6 then cs_net_paid * cs_quantity else 0 end) as jun_net ,sum(case when d_moy = 7 then cs_net_paid * cs_quantity else 0 end) as jul_net ,sum(case when d_moy = 8 then cs_net_paid * cs_quantity else 0 end) as aug_net ,sum(case when d_moy = 9 then cs_net_paid * cs_quantity else 0 end) as sep_net ,sum(case when d_moy = 10 then cs_net_paid * cs_quantity else 0 end) as oct_net ,sum(case when d_moy = 11 then cs_net_paid * cs_quantity else 0 end) as nov_net ,sum(case when d_moy = 12 then cs_net_paid * cs_quantity else 0 end) as dec_net from catalog_sales ,warehouse ,date_dim ,time_dim ,ship_mode where cs_warehouse_sk =  w_warehouse_sk and cs_sold_date_sk = d_date_sk and cs_sold_time_sk = t_time_sk and cs_ship_mode_sk = sm_ship_mode_sk and d_year = {x6} and t_time between 46866 AND 46866+28800 and sm_carrier in ('GREAT EASTERN','UPS') group by w_warehouse_name ,w_warehouse_sq_ft ,w_city ,w_county ,w_state ,w_country ,d_year ) x group by w_warehouse_name ,w_warehouse_sq_ft ,w_city ,w_county ,w_state ,w_country ,ship_carriers ,year order by w_warehouse_name limit 100;"
        return query

   # Populate selectbox  
  
    x6 = st.selectbox("Select YEAR", distinct_years)
    if st.button("Run"):
        # Generate query
        query = run_q64(x6) 
        # Execute query
        results = connection.execute(query)
        # Convert to DataFrame
        df = pd.DataFrame(results.fetchall())
        st.subheader('Query 66')
        st.write('Compute web and catalog sales and profits by warehouse. Report results by month for a given year during a given 8-hour period.')
        st.divider()
        # Display results
        st.dataframe(df)

#    # Query 66

#    def 
#    query = f"select w_warehouse_name ,w_warehouse_sq_ft ,w_city ,w_county ,w_state ,w_country ,ship_carriers ,year ,sum(jan_sales) as jan_sales ,sum(feb_sales) as feb_sales ,sum(mar_sales) as mar_sales ,sum(apr_sales) as apr_sales ,sum(may_sales) as may_sales ,sum(jun_sales) as jun_sales ,sum(jul_sales) as jul_sales ,sum(aug_sales) as aug_sales ,sum(sep_sales) as sep_sales ,sum(oct_sales) as oct_sales ,sum(nov_sales) as nov_sales ,sum(dec_sales) as dec_sales ,sum(jan_sales/w_warehouse_sq_ft) as jan_sales_per_sq_foot ,sum(feb_sales/w_warehouse_sq_ft) as feb_sales_per_sq_foot ,sum(mar_sales/w_warehouse_sq_ft) as mar_sales_per_sq_foot ,sum(apr_sales/w_warehouse_sq_ft) as apr_sales_per_sq_foot ,sum(may_sales/w_warehouse_sq_ft) as may_sales_per_sq_foot ,sum(jun_sales/w_warehouse_sq_ft) as jun_sales_per_sq_foot ,sum(jul_sales/w_warehouse_sq_ft) as jul_sales_per_sq_foot ,sum(aug_sales/w_warehouse_sq_ft) as aug_sales_per_sq_foot ,sum(sep_sales/w_warehouse_sq_ft) as sep_sales_per_sq_foot ,sum(oct_sales/w_warehouse_sq_ft) as oct_sales_per_sq_foot ,sum(nov_sales/w_warehouse_sq_ft) as nov_sales_per_sq_foot ,sum(dec_sales/w_warehouse_sq_ft) as dec_sales_per_sq_foot ,sum(jan_net) as jan_net ,sum(feb_net) as feb_net ,sum(mar_net) as mar_net ,sum(apr_net) as apr_net ,sum(may_net) as may_net ,sum(jun_net) as jun_net ,sum(jul_net) as jul_net ,sum(aug_net) as aug_net ,sum(sep_net) as sep_net ,sum(oct_net) as oct_net ,sum(nov_net) as nov_net ,sum(dec_net) as dec_net from ( select w_warehouse_name ,w_warehouse_sq_ft ,w_city ,w_county ,w_state ,w_country ,'GREAT EASTERN' || ',' || 'UPS' as ship_carriers ,d_year as year ,sum(case when d_moy = 1 then ws_ext_sales_price* ws_quantity else 0 end) as jan_sales ,sum(case when d_moy = 2 then ws_ext_sales_price* ws_quantity else 0 end) as feb_sales ,sum(case when d_moy = 3 then ws_ext_sales_price* ws_quantity else 0 end) as mar_sales ,sum(case when d_moy = 4 then ws_ext_sales_price* ws_quantity else 0 end) as apr_sales ,sum(case when d_moy = 5 then ws_ext_sales_price* ws_quantity else 0 end) as may_sales ,sum(case when d_moy = 6 then ws_ext_sales_price* ws_quantity else 0 end) as jun_sales ,sum(case when d_moy = 7 then ws_ext_sales_price* ws_quantity else 0 end) as jul_sales ,sum(case when d_moy = 8 then ws_ext_sales_price* ws_quantity else 0 end) as aug_sales ,sum(case when d_moy = 9 then ws_ext_sales_price* ws_quantity else 0 end) as sep_sales ,sum(case when d_moy = 10 then ws_ext_sales_price* ws_quantity else 0 end) as oct_sales ,sum(case when d_moy = 11 then ws_ext_sales_price* ws_quantity else 0 end) as nov_sales ,sum(case when d_moy = 12 then ws_ext_sales_price* ws_quantity else 0 end) as dec_sales ,sum(case when d_moy = 1 then ws_net_paid * ws_quantity else 0 end) as jan_net ,sum(case when d_moy = 2 then ws_net_paid * ws_quantity else 0 end) as feb_net ,sum(case when d_moy = 3 then ws_net_paid * ws_quantity else 0 end) as mar_net ,sum(case when d_moy = 4 then ws_net_paid * ws_quantity else 0 end) as apr_net ,sum(case when d_moy = 5 then ws_net_paid * ws_quantity else 0 end) as may_net ,sum(case when d_moy = 6 then ws_net_paid * ws_quantity else 0 end) as jun_net ,sum(case when d_moy = 7 then ws_net_paid * ws_quantity else 0 end) as jul_net ,sum(case when d_moy = 8 then ws_net_paid * ws_quantity else 0 end) as aug_net ,sum(case when d_moy = 9 then ws_net_paid * ws_quantity else 0 end) as sep_net ,sum(case when d_moy = 10 then ws_net_paid * ws_quantity else 0 end) as oct_net ,sum(case when d_moy = 11 then ws_net_paid * ws_quantity else 0 end) as nov_net ,sum(case when d_moy = 12 then ws_net_paid * ws_quantity else 0 end) as dec_net from web_sales ,warehouse ,date_dim ,time_dim ,ship_mode where ws_warehouse_sk =  w_warehouse_sk and ws_sold_date_sk = d_date_sk and ws_sold_time_sk = t_time_sk and ws_ship_mode_sk = sm_ship_mode_sk and d_year = 1998 and t_time between 46866 and 46866+28800 and sm_carrier in ('GREAT EASTERN','UPS') group by w_warehouse_name ,w_warehouse_sq_ft ,w_city ,w_county ,w_state ,w_country ,d_year union all select w_warehouse_name ,w_warehouse_sq_ft ,w_city ,w_county ,w_state ,w_country ,'GREAT EASTERN' || ',' || 'UPS' as ship_carriers ,d_year as year ,sum(case when d_moy = 1 then cs_sales_price* cs_quantity else 0 end) as jan_sales ,sum(case when d_moy = 2 then cs_sales_price* cs_quantity else 0 end) as feb_sales ,sum(case when d_moy = 3 then cs_sales_price* cs_quantity else 0 end) as mar_sales ,sum(case when d_moy = 4 then cs_sales_price* cs_quantity else 0 end) as apr_sales ,sum(case when d_moy = 5 then cs_sales_price* cs_quantity else 0 end) as may_sales ,sum(case when d_moy = 6 then cs_sales_price* cs_quantity else 0 end) as jun_sales ,sum(case when d_moy = 7 then cs_sales_price* cs_quantity else 0 end) as jul_sales ,sum(case when d_moy = 8 then cs_sales_price* cs_quantity else 0 end) as aug_sales ,sum(case when d_moy = 9 then cs_sales_price* cs_quantity else 0 end) as sep_sales ,sum(case when d_moy = 10 then cs_sales_price* cs_quantity else 0 end) as oct_sales ,sum(case when d_moy = 11 then cs_sales_price* cs_quantity else 0 end) as nov_sales ,sum(case when d_moy = 12 then cs_sales_price* cs_quantity else 0 end) as dec_sales ,sum(case when d_moy = 1 then cs_net_paid * cs_quantity else 0 end) as jan_net ,sum(case when d_moy = 2 then cs_net_paid * cs_quantity else 0 end) as feb_net ,sum(case when d_moy = 3 then cs_net_paid * cs_quantity else 0 end) as mar_net ,sum(case when d_moy = 4 then cs_net_paid * cs_quantity else 0 end) as apr_net ,sum(case when d_moy = 5 then cs_net_paid * cs_quantity else 0 end) as may_net ,sum(case when d_moy = 6 then cs_net_paid * cs_quantity else 0 end) as jun_net ,sum(case when d_moy = 7 then cs_net_paid * cs_quantity else 0 end) as jul_net ,sum(case when d_moy = 8 then cs_net_paid * cs_quantity else 0 end) as aug_net ,sum(case when d_moy = 9 then cs_net_paid * cs_quantity else 0 end) as sep_net ,sum(case when d_moy = 10 then cs_net_paid * cs_quantity else 0 end) as oct_net ,sum(case when d_moy = 11 then cs_net_paid * cs_quantity else 0 end) as nov_net ,sum(case when d_moy = 12 then cs_net_paid * cs_quantity else 0 end) as dec_net from catalog_sales ,warehouse ,date_dim ,time_dim ,ship_mode where cs_warehouse_sk =  w_warehouse_sk and cs_sold_date_sk = d_date_sk and cs_sold_time_sk = t_time_sk and cs_ship_mode_sk = sm_ship_mode_sk and d_year = 1998 and t_time between 46866 AND 46866+28800 and sm_carrier in ('GREAT EASTERN','UPS') group by w_warehouse_name ,w_warehouse_sq_ft ,w_city ,w_county ,w_state ,w_country ,d_year ) x group by w_warehouse_name ,w_warehouse_sq_ft ,w_city ,w_county ,w_state ,w_country ,ship_carriers ,year order by w_warehouse_name limit 100;"
   
   
#    results = connection.execute(query)  
#    df = pd.DataFrame(results.fetchall())
#    st.subheader('Query 66')
#    st.write('Compute web and catalog sales and profits by warehouse. Report results by month for a given year during a given 8-hour period.')
#    st.divider()
#    # Display as table
#    st.dataframe(df)

with tab7:
   # Query 67
   query = f"select  * from (select i_category ,i_class ,i_brand ,i_product_name ,d_year ,d_qoy ,d_moy ,s_store_id ,sumsales ,rank() over (partition by i_category order by sumsales desc) rk from (select i_category ,i_class ,i_brand ,i_product_name ,d_year ,d_qoy ,d_moy ,s_store_id ,sum(coalesce(ss_sales_price*ss_quantity,0)) sumsales from store_sales ,date_dim ,store ,item where  ss_sold_date_sk=d_date_sk and ss_item_sk=i_item_sk and ss_store_sk = s_store_sk and d_month_seq between 1213 and 1213+11 group by  rollup(i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy,s_store_id))dw1) dw2 where rk <= 100 order by i_category ,i_class ,i_brand ,i_product_name ,d_year ,d_qoy ,d_moy ,s_store_id ,sumsales ,rk limit 100;"
   
   
   results = connection.execute(query)  
   df = pd.DataFrame(results.fetchall())
   st.subheader('Query 67')
   st.write('Find top stores for each category based on store sales in a specific year')
   st.divider()
   # Display as table
   st.dataframe(df)


with tab8:
    # Query 64
    def run_q68(x8,y8,z8):
        query=f"select  c_last_name ,c_first_name ,ca_city ,bought_city ,ss_ticket_number ,extended_price ,extended_tax ,list_price from (select ss_ticket_number ,ss_customer_sk ,ca_city bought_city ,sum(ss_ext_sales_price) extended_price ,sum(ss_ext_list_price) list_price ,sum(ss_ext_tax) extended_tax from store_sales ,date_dim ,store ,household_demographics ,customer_address where store_sales.ss_sold_date_sk = date_dim.d_date_sk and store_sales.ss_store_sk = store.s_store_sk and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk and store_sales.ss_addr_sk = customer_address.ca_address_sk and date_dim.d_dom between 1 and 2 and (household_demographics.hd_dep_count = 3 or household_demographics.hd_vehicle_count= 3) and date_dim.d_year in ({x8},{x8}+1,{x8}+2) and store.s_city in ('{y8}','{z8}') group by ss_ticket_number ,ss_customer_sk ,ss_addr_sk,ca_city) dn ,customer ,customer_address current_addr where ss_customer_sk = c_customer_sk and customer.c_current_addr_sk = current_addr.ca_address_sk and current_addr.ca_city <> bought_city order by c_last_name ,ss_ticket_number limit 100;"
        return query

   # Populate selectbox  
  
    x8 = st.selectbox("SELECT YEAR", distinct_years)
    y8 = st.selectbox("City 1", distinct_city)
    z8 = st.selectbox("City 2", distinct_city)
    if st.button("EXECUTE"):
        # Generate query
        query = run_q68(x8,y8,z8) 
        # Execute query
        results = connection.execute(query)
        # Convert to DataFrame
        df = pd.DataFrame(results.fetchall())
        st.subheader('Query 68')
        st.write('Compute the per customer extended sales price, extended list price and extended tax for "out of town" shoppers buying from stores located in two cities in the first two days of each month of three consecutive years. Only consider customers with specific dependent and vehicle counts.')
        st.divider()
        # Display results
        st.dataframe(df)


with tab9:
    # Query 69
    def run_q69(x9,y9):
        query=f"select cd_gender, cd_marital_status, cd_education_status, count(*) cnt1, cd_purchase_estimate, count(*) cnt2, cd_credit_rating, count(*) cnt3 from customer c,customer_address ca,customer_demographics where c.c_current_addr_sk = ca.ca_address_sk and ca_state in ('IA','GA','MN') and cd_demo_sk = c.c_current_cdemo_sk and exists (select * from store_sales,date_dim where c.c_customer_sk = ss_customer_sk and ss_sold_date_sk = d_date_sk and d_year = {x9} and d_moy between {y9} and {y9}+2) and (not exists (select * from web_sales,date_dim where c.c_customer_sk = ws_bill_customer_sk and ws_sold_date_sk = d_date_sk and d_year = {x9} and d_moy between {y9} and {y9}+2) and not exists (select * from catalog_sales,date_dim where c.c_customer_sk = cs_ship_customer_sk and cs_sold_date_sk = d_date_sk and d_year = {x9} and d_moy between {y9} and {y9}+2)) group by cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating order by cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating limit 100;"
        return query

   # Populate selectbox  
  
    x9 = st.selectbox("CHOOSE YEAR", distinct_years)
    y9 = st.selectbox("CHOOSE MONTH", distinct_months)
    if st.button("RUN QUERY"):
        # Generate query
        query = run_q69(x9,y9) 
        # Execute query
        results = connection.execute(query)
        # Convert to DataFrame
        df = pd.DataFrame(results.fetchall())
        st.subheader('Query 69')
        st.write('Count the customers with the same gender, marital status, education status, education status, purchase estimate and credit rating who live in certain states and who have purchased from stores but neither form the catalog nor from the web during a two month time period of a given year.')
        st.divider()
        # Display results
        st.dataframe(df)



with tab10:
   # Query 70
   query = f"select sum(ss_net_profit) as total_sum ,s_state ,s_county ,grouping(s_state)+grouping(s_county) as lochierarchy ,rank() over ( partition by grouping(s_state)+grouping(s_county), case when grouping(s_county) = 0 then s_state end order by sum(ss_net_profit) desc) as rank_within_parent from store_sales ,date_dim       d1 ,store where d1.d_month_seq between 1210 and 1210+11 and d1.d_date_sk = ss_sold_date_sk and s_store_sk  = ss_store_sk and s_state in ( select s_state from  (select s_state as s_state, rank() over ( partition by s_state order by sum(ss_net_profit) desc) as ranking from   store_sales, store, date_dim where  d_month_seq between 1210 and 1210+11 and d_date_sk = ss_sold_date_sk and s_store_sk  = ss_store_sk group by s_state ) tmp1 where ranking <= 5 ) group by rollup(s_state,s_county) order by lochierarchy desc ,case when lochierarchy = 0 then s_state end ,rank_within_parent limit 100;"
   
   
   results = connection.execute(query)  
   df = pd.DataFrame(results.fetchall())
   st.subheader('Query 70')
   st.write('Compute store sales net profit ranking by state and county for a given year and determine the five most profitable states.')
   st.divider()
   # Display as table
   st.dataframe(df)

#df = pd.read_sql_query(run_q61('Music',9,1998), engine)
#results = connection.execute(run_q61('Music',9,1998))

#st.write('query 61: Find the ratio of items sold with and without promotions in a given month and year. Only items in certain categories sold to customers living in a specific time zone are considered.')
#st.write('query 62: For web sales, create a report showing the counts of orders shipped within 30 days, from 31 to 60 days, from 61 to 90 days, from 91 to 120 days and over 120 days within a given year, grouped by warehouse, shipping mode and web site')
#st.write('query 63: For a given year calculate the monthly sales of items of specific categories, classes and brands that were sold in stores and group the results by store manager. Additionally, for every month and manager print the yearly average sales of those items.')
#st.write('query 64: Find those stores that sold more cross-sales items from one year to another. Cross-sale items are items that are sold over the Internet, by catalog and in store')
#st.write('query 65: In a given period, for each store, report the list of items with revenue less than 10% the average revenue for all the items in that store.')
#st.write('query 66: Compute web and catalog sales and profits by warehouse. Report results by month for a given year during a given 8-hour period.')
#st.write('query 67: Find top stores for each category based on store sales in a specific year.')
#st.write('query 68: Compute the per customer extended sales price, extended list price and extended tax for "out of town" shoppers buying from stores located in two cities in the first two days of each month of three consecutive years. Only consider customers with specific dependent and vehicle counts.')
#st.write('query 69: Count the customers with the same gender, marital status, education status, education status, purchase estimate and credit rating who live in certain states and who have purchased from stores but neither form the catalog nor from the web during a two month time period of a given year ')
#st.write('query 70: Compute store sales net profit ranking by state and county for a given year and determine the five most profitable states.')'''



connection.close()
