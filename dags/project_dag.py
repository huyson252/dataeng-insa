import airflow
import datetime
import json 
import pandas as pd
import re
import os
import warnings
import time
import psycopg2
import datetime
import praw
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from zipfile import ZipFile
from pymongo import MongoClient
from random import randint
from pprint import pprint
from fuzzywuzzy import fuzz
import logging

warnings.filterwarnings('ignore')

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

project_dag = DAG(
    dag_id='project_dag',
    default_args=default_args_dict,
    catchup=False,
)

# pull kaggle dataset
first_node = BashOperator(
    task_id='get_kaggle',
    dag=project_dag,
    bash_command="export KAGGLE_CONFIG_DIR=/home/***/.kaggle && \
        chmod 600 /home/***/.kaggle/kaggle.json && \
        kaggle datasets download -d andrewmvd/data-engineer-jobs && \
        cp data-engineer-jobs.zip /home/airflow/data-engineer-jobs.zip",
)

# clean kaggle csv data and dump to json
def _save_kaggle(output_folder: str):
    zf = ZipFile('/home/airflow/data-engineer-jobs.zip')
    zf.extractall() 
    zf.close()

    df = pd.read_csv('DataEngineer.csv')
    df = df[['Job Title', 'Salary Estimate', 'Job Description', 'Company Name', 'Location', 'Size', 'Industry', 'Sector']]
    df = df.rename(columns = {})
    df.to_json(f'{output_folder}/kaggle_data.json', orient='records')

# pull kaggle dataset
second_node = PythonOperator(
    task_id='save_kaggle',
    dag=project_dag,
    trigger_rule='none_failed',
    python_callable=_save_kaggle,
    op_kwargs={
        "output_folder": os.getcwd(),
    },
    depends_on_past=False,
    )

# get and preclean reddit dataset
def _get_reddit(output_folder: str):
    reddit = praw.Reddit(
        client_id="gHlIBmnNOzt1GWDbqrWK7w",
        client_secret="nkMZJRmyJOs0InnyAvtQYD20oq-QXQ",
        user_agent="Comment Extraction (by u/rbe-0000)",
    )

    def isNone(rank, name, comment):
        if name is None:
            res =  comment[2+comment.rfind(rank):]
        else:
            res = name.group(1)
        return res.strip()

    collection = reddit.subreddit("dataengineering").collections("ef3eb514-328d-4549-a705-94c26963d79b");
    dictionaryList = []
    stackList = [];
    for link in collection.link_ids:
        new = link[1+link.rfind('_'):];
        submission = reddit.submission(f"{new}")
        submission.comments.replace_more(limit=None)
        commentList = [top_level_comment.body for top_level_comment in submission.comments]
        
        for comment in commentList:
            if "1." in comment and "\n4." in comment: 
                title = re.search('1.(.*)\n', comment).group(1).strip()
                salary = isNone('4.', re.search('\n4.(.*)\n',comment), comment)
                dict = { "title": title, "salary_min": salary, "salary_max": salary }
                if "\n3." in comment: 
                    location = re.search('\n3.(.*)\n',comment).group(1).strip()
                    dict["location"] = location
                if "\n7." in comment:
                    stack = (isNone('7.', re.search('\n7.(.*)\n',comment), comment)).lower().replace(';',',').replace(', ',',').replace('/',',').replace('+',',').replace('(',',').replace(')',',').replace('and ',',').replace('&',',').replace('-',',').replace(':',',').replace(' ,',',').split(",")
                    dict["tech_stack"] = stack
                    stackList = stackList + stack

                dictionaryList.append(dict)

    with open(f'{output_folder}/reddit.json', 'w') as f:
        json.dump(dictionaryList, f, ensure_ascii=False)


third_node = PythonOperator(
    task_id='get_reddit',
    dag=project_dag,
    trigger_rule='none_failed',
    python_callable=_get_reddit,
    op_kwargs={
        "output_folder": os.getcwd(),
    },
    depends_on_past=False,
)


# ingest json data to mongodb 
def _ingest_mongodb(output_folder: str):
    client = MongoClient('mongodb://mongo:27017/')
    db = client['data_eng']

    kaggle_collection = db['kaggle']
    with open(f'{output_folder}/kaggle_data.json',) as f_kaggle:
        kaggle_data = json.load(f_kaggle)
    kaggle_collection.insert_many(kaggle_data)

    reddit_collection = db['reddit']
    with open(f'{output_folder}/reddit.json',) as f_reddit:
        reddit_data = json.load(f_reddit)
    reddit_collection.insert_many(reddit_data)


forth_node = PythonOperator(
    task_id='ingest_mongodb',
    dag=project_dag,
    trigger_rule='none_failed',
    python_callable=_ingest_mongodb,
    op_kwargs={
        "output_folder": os.getcwd(),
    },
    depends_on_past=False,
)

# clean all data
def _clean_all_data():
    # get and clean kaggle data
    client = MongoClient('mongodb://mongo:27017/')
    db = client['data_eng']

    kaggle_collection = db['kaggle']
    reddit_collection = db['reddit']
    kaggle_mongo = kaggle_collection.find()
    df_kaggle = pd.DataFrame(kaggle_mongo)
    df_kaggle = df_kaggle.drop(['_id'], axis=1)

    # clean salary info
    df_salaries = df_kaggle['Salary Estimate'].str.split('-', expand=True, n = 1)
    df_kaggle['salary_min'] = df_salaries.iloc[:,0].str.replace('K','000').str.extract('(\d+)')
    df_kaggle['salary_max'] = df_salaries.iloc[:,1].str.replace('K','000').str.extract('(\d+)')

    # clean company name
    df_kaggle['Company Name'] = df_kaggle['Company Name'].str.split('\\n').str[0]

    # conform to the defined schema
    df_kaggle = df_kaggle.rename(columns= {'Job Title':'title', 'Job Description': 'tech_stack', 
                                           'Company Name': 'company_name', 'Location': 'location', 'Industry': 'industry'})
    df_kaggle = df_kaggle[['title', 'location', 'salary_min', 'salary_max', 'company_name', 'industry','tech_stack']]

    # get and clean reddit data
    reddit_mongo = reddit_collection.find()
    df_reddit = pd.DataFrame(reddit_mongo)
    df_reddit = df_reddit.drop(['_id'], axis=1)
    df_reddit['salary_min'] = df_reddit['salary_min'].str.lower().str.replace('k','000').str.replace(',','')
    df_reddit['salary_min'] = df_reddit['salary_min'].str.extract('(\d+)')
    df_reddit['salary_max'] = df_reddit['salary_min']

    # filter out outliers
    df_reddit = df_reddit[df_reddit['salary_min'].notna()]
    df_reddit = df_reddit[df_reddit['tech_stack'].notna()]
    df_reddit['salary_min'] = df_reddit['salary_min'].astype(int)
    df_reddit = df_reddit.drop(df_reddit[df_reddit['salary_min'] < 5000].index)

    df_reddit['industry'] = ''
    df_reddit['company_name'] = ''
    df_reddit = df_reddit[['title', 'location', 'salary_min', 'salary_max', 'company_name', 'industry','tech_stack']]

    start_time = time.time()
    # define a list of tech stack as standard reference
    list_tech_standard = ['python', 'sql','java', 'scala', 'talend', 'kafka', 'airflow', 'spark', 'azure', 
                     'aws', 'gcp','talend', 'dbt', 'snowflake', 'databricks', 'docker', 'hadoop', 
                     'powerbi', 'looker', 'tableau', 'mongodb', 'redis', 'neo4j', 'terraform'
                    ]

    # extract tech stack from kaggle data
    # ATTENTION: very time-consuming step (30s/each tech)
    list_tech_kaggle = []
    list_raw_kaggle = list(df_kaggle['tech_stack'])
    logging.info(len(list_raw_kaggle))
    i = 0
    for des in list_raw_kaggle:
        i += 1
        if (i % 100 == 0):
            logging.info(i)
        list_word = des.split()
        list_tech_line = []
        for tech in list_tech_standard:
            for word in list_word:
                if fuzz.token_sort_ratio(tech, word) > 80:
                    if tech not in list_tech_line:
                        list_tech_line.append(tech)
        list_tech_kaggle.append(list_tech_line)
        
    df_kaggle['tech_stack'] = list_tech_kaggle  

    list_tech_reddit = []
    list_raw_reddit = list(df_reddit['tech_stack'])
    for list_tech_raw in list_raw_reddit:
        list_tech_line = []
        for tech in list_tech_standard: 
            for element in list_tech_raw:
                if fuzz.token_sort_ratio(tech, element) > 80:
                    if tech not in list_tech_line:
                        list_tech_line.append(tech)
        list_tech_reddit.append(list_tech_line)

    df_reddit['tech_stack'] = list_tech_reddit

    df_all = df_kaggle.append(df_reddit)
    for column in ['title','company_name','location']:
        df_all[column] = df_all[column].str.replace('\'', '')
    df_all.to_csv('/home/airflow/dataeng.csv')

fifth_node = PythonOperator(
    task_id='_clean_all_data',
    dag=project_dag,
    trigger_rule='none_failed',
    python_callable=_clean_all_data,
    depends_on_past=False,
)


# ingest mongodb data to postgreSQL    
def _ingest_postgresql():
    # Connect to your postgres DB
    conn = psycopg2.connect(dbname='airflow', user='airflow', password='airflow', host='postgres', port='5432')
    conn.autocommit = True

    # Open a cursor to perform database operations
    cur = conn.cursor()
    cur.execute('drop table data_engineering, tech_stack cascade')
    # Create company Dimension Table 
    cur.execute("CREATE TABLE IF NOT EXISTS company (id SERIAL PRIMARY KEY, name TEXT, industry TEXT)")
    cur.execute("CREATE INDEX IF NOT EXISTS dim_company_index ON company (name, industry)")

    # Create location Dimension Table 
    cur.execute("CREATE TABLE IF NOT EXISTS location (id serial PRIMARY KEY, name text)")
    cur.execute("CREATE INDEX IF NOT EXISTS dim_location_index ON location (name)")

    # Create salary Dimension Table
    cur.execute("CREATE TABLE IF NOT EXISTS salary (id serial PRIMARY KEY, salary_min text, salary_max text)")
    cur.execute("CREATE INDEX IF NOT EXISTS dim_salary_index ON salary (salary_min, salary_max);")

    # Create stack_list Dimension Table
    cur.execute("CREATE TABLE IF NOT EXISTS stack_list (id serial PRIMARY KEY, list TEXT [])")
    cur.execute("CREATE INDEX IF NOT EXISTS dim_stack_list_index ON stack_list (list)")

    # Create tech_stack Table 
    cur.execute("CREATE TABLE IF NOT EXISTS tech_stack (id serial PRIMARY KEY, name text, frequency integer, salary_average integer)")
    cur.execute("CREATE INDEX IF NOT EXISTS dim_tech_stack_index ON tech_stack (name, frequency, salary_average)")

    # Create data_engineering Fact Table 
    cur.execute("CREATE TABLE IF NOT EXISTS data_engineering (id serial PRIMARY KEY, title text NOT NULL, location_id integer NOT NULL REFERENCES location (id), salary_id integer NOT NULL REFERENCES salary (id), company_id integer REFERENCES company(id), stack_list_id integer REFERENCES stack_list (id))")

    df = pd.read_csv('/home/airflow/dataeng.csv')

    for ind in df.index:
        cur.execute("INSERT INTO company (name,industry) VALUES ('{0}', '{1}') RETURNING id".format(df['company_name'][ind], df['industry'][ind]))
        company_id = cur.fetchone()
        cur.execute("INSERT INTO salary (salary_min, salary_max) VALUES ('{0}', '{1}') RETURNING id".format(df['salary_min'][ind], df['salary_max'][ind]))
        salary_id = cur.fetchone()
        cur.execute("INSERT INTO location (name) VALUES ('{0}') RETURNING id".format(df['location'][ind]))
        location_id = cur.fetchone()
        values_list = "title, location_id, salary_id, company_id"
        values = df['title'][ind] + "', " + str(location_id[0]) + ", " + str(salary_id[0]) + ", " + str(company_id[0])
        df_tech_list = df['tech_stack'][ind]
        if df_tech_list != '[]':
            cur.execute("INSERT INTO stack_list (list) VALUES (ARRAY {0}) RETURNING id".format(df['tech_stack'][ind]))
            values_list = values_list + ", stack_list_id"
            values = values + ", " + str(cur.fetchone()[0])
        cur.execute("INSERT INTO data_engineering (" + values_list + ") VALUES ('" + values + ")")
    
    df['salary_average_line'] = df[['salary_max', 'salary_min']].mean(axis=1).astype(int)
    df['tech_stack'] = df['tech_stack'].str.strip('[]').str.replace('\'','').str.split(', ')
    list_tech_stack = list(df['tech_stack'])
    list_tech_concat = [j for i in list_tech_stack for j in i]
    frequency = {}
    while("" in list_tech_concat):
        list_tech_concat.remove("")
    # iterating over the list
    for item in list_tech_concat:
       # checking the element in dictionary
        if item in frequency:
          # incrementing the counr
            frequency[item] += 1
        else:
          # initializing the count
            frequency[item] = 1
    df_tech_stack = pd.DataFrame(frequency.items(), columns=['tech_name', 'frequency'])
    
    # append average value for each tech
    list_salary_average = []
    for ind_tech in df_tech_stack.index:
        list_salary = []
        for ind in df.index:
            if df_tech_stack['tech_name'][ind_tech] in df['tech_stack'][ind]:
                list_salary.append(df['salary_average_line'][ind])
        avg = sum(list_salary) / len(list_salary)
        list_salary_average.append(avg)

    list_salary_average = [int(x) for x in list_salary_average]
    df_tech_stack['salary_average'] = list_salary_average
    for inde in df_tech_stack.index:
        cur.execute("INSERT INTO tech_stack (name,frequency,salary_average) VALUES ('{0}', '{1}', '{2}')".format(df_tech_stack['tech_name'][inde], df_tech_stack['frequency'][inde], df_tech_stack['salary_average'][inde]))

    conn.close()

sixth_node = PythonOperator(
    task_id='ingest_postgresql',
    dag=project_dag,
    trigger_rule='all_success',
    python_callable=_ingest_postgresql,
    op_kwargs={},
    depends_on_past=False
)

first_node >> second_node
[second_node, third_node] >> forth_node >> fifth_node >> sixth_node
