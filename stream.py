import streamlit as st
import pandas as pd
import plotly.express as px
import snowflake.connector

# Streamlit page setup
st.set_page_config(page_title="Job Analytics Dashboard", layout="wide")
st.title("📊 Job Analytics Dashboard (Snowflake + Plotly)")

# Snowflake config
config = {
    "user": "bodkhevj",
    "password": "JayeshShete@1512",
    "account": "FECHZKS-MI93361",
    "warehouse": "PROJECT_WH",
    "database": "JOBS_DB",
    "schema": "JOBS_SCHEMA"
}

# Connect to Snowflake
@st.cache_resource
def connect_snowflake():
    return snowflake.connector.connect(**config)

conn = connect_snowflake()

# Run SQL query and return DataFrame
@st.cache_data(ttl=300)
def run_query(query):
    cur = conn.cursor()
    try:
        cur.execute(query)
        results = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return pd.DataFrame(results, columns=columns)
    finally:
        cur.close()

# 1️⃣ Job Posting Trends Over Time
st.subheader("📅 Job Posting Trends Over Time")
job_trends_df = run_query("SELECT posting_month, job_count FROM job_posting_trends ORDER BY posting_month")
fig1 = px.line(job_trends_df, x='POSTING_MONTH', y='JOB_COUNT', markers=True)
st.plotly_chart(fig1, use_container_width=True)

# 2️⃣ Top 10 Companies by Job Count
st.subheader("🏢 Top 10 Companies by Job Count")
top_companies_df = run_query("""
    SELECT company, job_count
    FROM company_job_counts
    ORDER BY job_count DESC
    LIMIT 10
""")
fig2 = px.bar(top_companies_df, x='JOB_COUNT', y='COMPANY', orientation='h', color='COMPANY', title="Top Companies")
st.plotly_chart(fig2, use_container_width=True)

# 3️⃣ Top 10 Industries by Job Count
st.subheader("🏭 Top 10 Industries by Job Count")
top_industries_df = run_query("""
    SELECT industry, job_count
    FROM industry_analysis
    ORDER BY job_count DESC
    LIMIT 10
""")
fig3 = px.bar(top_industries_df, x='JOB_COUNT', y='INDUSTRY', orientation='h', color='INDUSTRY', title="Top Industries")
st.plotly_chart(fig3, use_container_width=True)

# 4️⃣ Remote vs. On-site Distribution
st.subheader("🌐 Remote vs On-site Job Distribution")
remote_work_df = run_query("""
    SELECT
        CASE WHEN remote_flag = TRUE THEN 'Remote' ELSE 'On-site' END AS work_type,
        COUNT(*) AS job_count
    FROM raw_jobs
    GROUP BY work_type
""")
fig4 = px.pie(remote_work_df, names='WORK_TYPE', values='JOB_COUNT', title='Remote vs. On-site Jobs', hole=0.4)
st.plotly_chart(fig4, use_container_width=True)

# 5️⃣ Experience Level Distribution
st.subheader("🧠 Experience Level Distribution")
experience_df = run_query("""
    SELECT
        experience_level,
        COUNT(*) AS job_count
    FROM raw_jobs
    GROUP BY experience_level
    ORDER BY job_count DESC
""")
fig5 = px.bar(experience_df, x='JOB_COUNT', y='EXPERIENCE_LEVEL', orientation='h',
              color='EXPERIENCE_LEVEL', title="Job Count by Experience Level")
st.plotly_chart(fig5, use_container_width=True)

# Close connection on app exit
conn.close()
