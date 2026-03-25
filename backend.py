from jobspy import scrape_jobs
import pandas as pd
from supabase import create_client
from os import getenv
from numpy import nan
import re

 
#################################################
# SCRAPING DATA + CONNECTION TO DATABASE
#################################################

def clean_job_data(jobs):
    # Select only the necessary rows and columns 
    try:
        cleaned_jobs = pd.DataFrame(jobs[['id', 'title', 'company', 'job_type', 'job_level', 'tech_stack']]).drop_duplicates(subset=['title', 'company', 'job_type'], keep='first').replace(nan,None)
    except Exception as e:
        print(f"Cleaning data failed with error: {e}\n")
        return None,pd.DataFrame([])
    return cleaned_jobs

def extract_tech_stack(description, tech_keywords):
    if not isinstance(description, str):
        return []
    try:
        description_lower = description.lower()
        tech_stack = []
        
        for keyword in tech_keywords:
            pattern = r'(?<![\w\-])' + re.escape(keyword.lower()) + r'(?![\w\-\+#])'
            if re.search(pattern, description_lower):
                tech_stack.append(keyword)
                
        return tech_stack
        
    except Exception as e:
        print(f"Error occurred while extracting Tech stacks from Descriptions with error: {e}\n")
        return None,[]

def insert_data_to_supabase(cleaned_jobs):
    try:
        supabase_key = getenv("SUPABASE_KEY")
        supabase_url = getenv("PROJECT_URL")
        supabase = create_client(supabase_url, supabase_key)

        # Insert jobs into the 'jobs' table
        for _, job in cleaned_jobs.iterrows():
            job_data = {
                "id": job["id"],
                "title": job["title"],
                "company": job["company"],
                "job_type": job["job_type"],
                "job_level": job["job_level"]
            }
            response = supabase.from_("jobs").upsert([job_data]).execute()

        # Insert tech stack into the 'tech_stack' table
        for _, job in cleaned_jobs.iterrows():
            for tech in job["tech_stack"]:
                tech_stack_data = {
                    "job_id": job["id"],
                    "technology": tech
                }
                response = supabase.from_("tech_stack").upsert([tech_stack_data]).execute()
    except Exception as e:
        print(f"Error occured when inserting data into database with error : {e}\n")

# list of technology keywords to look for in job descriptions
tech_keywords = [
    # --- Programming Languages ---
    'python', 'java', 'c', 'c++', 'c#', 'javascript', 'typescript', 'sql', 'ruby', 
    'go', 'golang', 'swift', 'kotlin', 'scala', 'perl', 'php', 'rust', 'haskell', 
    'elixir', 'clojure', 'groovy', 'objective-c', 'f#', 'r', 'matlab', 'dart', 
    'lua', 'julia', 'shell', 'bash', 'powershell', 'vba', 'assembly', 'cobol', 
    'fortran', 'solidity', 'erlang',

    # --- Frontend & UI ---
    'html', 'css', 'sass', 'less', 'tailwind', 'bootstrap', 'material-ui', 
    'react', 'react.js', 'angular', 'vue', 'vue.js', 'ember.js', 'svelte', 
    'backbone.js', 'polymer', 'jquery', 'next.js', 'nuxt.js', 'gatsby', 
    'webpack', 'vite', 'babel', 'redux', 'mobx', 'rxjs', 'd3.js', 'three.js', 
    'webgl', 'webassembly', 'wasm',

    # --- Backend & Frameworks ---
    'node.js', 'node', 'express', 'django', 'flask', 'fastapi', 'tornado', 
    'rails', 'ruby on rails', 'sinatra', 'laravel', 'symfony', 'asp.net', 
    '.net', '.net core', 'spring', 'spring boot', 'hibernate', 'jpa', 'mybatis', 
    'sequelize', 'mongoose', 'typeorm', 'phoenix', 'play', 'ktor', 'nestjs', 
    'meteor', 'strapi', 'celery',

    # --- Mobile Development ---
    'flutter', 'react-native', 'ionic', 'cordova', 'android', 'ios', 
    'xamarin', 'maui', 'appium',

    # --- Databases & Data Warehouses ---
    'mongodb', 'mysql', 'postgresql', 'postgres', 'oracle', 'sqlite', 'redis', 
    'memcached', 'cassandra', 'elasticsearch', 'neo4j', 'mariadb', 'sql server', 
    'mssql', 'db2', 'couchbase', 'couchdb', 'dynamodb', 'cosmosdb', 'firebase', 
    'supabase', 'snowflake', 'redshift', 'bigquery', 'databricks', 'teradata', 
    'presto', 'athena', 'hbase', 'influxdb', 'timescaledb', 'realm',

    # --- Message Brokers & Streaming ---
    'kafka', 'rabbitmq', 'activemq', 'zeromq', 'pulsar', 'kinesis', 'spark', 
    'flink', 'hadoop', 'hive', 'storm',

    # --- AI, Machine Learning & Data Science ---
    'pandas', 'numpy', 'scipy', 'scikit-learn', 'tensorflow', 'pytorch', 
    'keras', 'opencv', 'nltk', 'spacy', 'hugging face', 'langchain', 'jupyter', 
    'matplotlib', 'seaborn', 'llm', 'openai',

    # --- DevOps, CI/CD & Infrastructure as Code ---
    'jenkins', 'gitlab-ci', 'github-actions', 'circleci', 'travis-ci', 
    'bitbucket-pipelines', 'ansible', 'terraform', 'puppet', 'chef', 'saltstack', 
    'argocd', 'flux', 'sonarcloud', 'sonarqube',

    # --- Containers & Orchestration ---
    'docker', 'docker-compose', 'kubernetes', 'k8s', 'eks', 'gke', 'aks', 
    'openshift', 'helm',

    # --- Cloud Platforms & Services ---
    'aws', 'gcp', 'azure', 'heroku', 'digitalocean', 'linode', 'vercel', 
    'netlify', 'cloudflare', 'fastly', 'aws-s3', 'azure-blob-storage', 
    'google-cloud-storage', 'aws-sqs', 'azure-queue-storage', 'google-cloud-pubsub', 
    'aws-dynamodb', 'azure-cosmosdb', 'google-cloud-datastore', 'aws-rds', 
    'azure-sql', 'google-cloud-sql', 'aws-elb', 'azure-lb', 'google-cloud-ilb', 
    'aws-ecs', 'azure-acr', 'google-cloud-container-builder', 'aws-codebuild', 
    'azure-devops', 'google-cloud-build', 'aws-vpc', 'azure-vnet', 'google-cloud-vpc', 
    'aws-eks', 'azure-aks', 'google-cloud-gke', 'aws-lambda', 'azure-functions', 
    'google-cloud-functions', 'firebase-functions',

    # --- Servers, Proxies & API Gateways ---
    'apache', 'nginx', 'haproxy', 'traefik', 'tomcat', 'api-gateway', 
    'nginx-ingress', 'haproxy-load-balancer', 'traefik-router', 'kong-api-gateway',

    # --- Monitoring, Logging & Observability ---
    'kibana', 'grafana', 'prometheus', 'graylog', 'splunk', 'logstash', 
    'datadog', 'new relic', 'appdynamics', 'dynatrace', 'sentry', 'elk',

    # --- Security, Identity & Auth ---
    'istio', 'linkerd', 'consul', 'vault', 'opa', 'csp', 'mfa', 'oauth2', 
    'jwt', 'openapi', 'graphql', 'grpc', 'protobuf', 'restful', 'aws-iam', 
    'azure-ad', 'okta', 'auth0', 'keycloak', 'openid-connect', 'saml', 
    'jwt-auth', 'oauth2-auth', 'aws-waf', 'azure-waf', 'google-cloud-waf', 
    'gcp-firewall',

    # --- Architecture & Concepts (Often listed as requirements) ---
    'microservices', 'serverless', 'lambda', 'rest', 'soap', 'tdd', 'bdd', 
    'agile', 'scrum', 'kanban',

    # --- Testing ---
    'selenium', 'cypress', 'puppeteer', 'playwright', 'jest', 'mocha', 
    'jasmine', 'karma', 'pytest', 'junit', 'nunit', 'testng', 'postman', 'soapui',

    # --- Version Control & OS ---
    'git', 'github', 'gitlab', 'bitbucket', 'svn', 'mercurial', 'linux', 
    'unix', 'windows', 'macos', 'ubuntu', 'centos', 'debian', 'redhat', 'alpine'
]
try:
    jobs = scrape_jobs(
        site_name=["linkedin","indeed"],
        search_term="Software engineer",
        location="Germany",
        hours_old=720,  # Data not older than 30 days (720 hours)
        country_indeed='Germany',
        results_wanted=20000,
        linkedin_fetch_description=True,
        proxies=None # I do not have any :)
    )
    
        # Add a new column for tech stack
    jobs['tech_stack'] = jobs['description'].apply(lambda desc: extract_tech_stack(desc, tech_keywords))

    cleaned_jobs = clean_job_data(jobs)

    # Insert cleaned data into Supabase
    insert_data_to_supabase(cleaned_jobs)
except Exception as e:
    print(f"Could not scrape Jobs, failed with error: {e}\n")
