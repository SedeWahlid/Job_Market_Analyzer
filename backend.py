from jobspy import scrape_jobs
import pandas as pd
from supabase import create_client
from os import getenv
from numpy import nan
import re
import time

#################################################
# SCRAPING DATA + CONNECTION TO DATABASE
#################################################

def clean_job_data(jobs):
    try:
        cleaned_jobs = pd.DataFrame(jobs[['id', 'title', 'company', 'job_type', 'job_level', 'tech_stack']]).drop_duplicates(subset=['title', 'company', 'job_type'], keep='first').replace(nan,None)
    except Exception as e:
        print(f"Cleaning data failed with error: {e}\n")
        return pd.DataFrame([])
    return cleaned_jobs

# list of technology keywords to look for in job descriptions
tech_keywords_map = {
    # --- Programming Languages ---
    'python': 'python',
    'java': 'java',
    'c': 'c',
    'c++': 'c++',
    'c#': 'c#',
    'javascript': 'javascript',
    'typescript': 'typescript',
    'sql': 'sql',
    'ruby': 'ruby',
    'go': 'go', 'golang': 'go',
    'swift': 'swift',
    'kotlin': 'kotlin',
    'scala': 'scala',
    'perl': 'perl',
    'php': 'php',
    'rust': 'rust',
    'haskell': 'haskell',
    'elixir': 'elixir',
    'clojure': 'clojure',
    'groovy': 'groovy',
    'objective-c': 'objective-c',
    'f#': 'f#',
    'r': 'r',
    'matlab': 'matlab',
    'dart': 'dart',
    'lua': 'lua',
    'julia': 'julia',
    'shell': 'shell',
    'bash': 'bash',
    'powershell': 'powershell',
    'vba': 'vba',
    'assembly': 'assembly',
    'cobol': 'cobol',
    'fortran': 'fortran',
    'solidity': 'solidity',
    'erlang': 'erlang',

    # --- Frontend & UI ---
    'html': 'html',
    'css': 'css',
    'sass': 'sass',
    'less': 'less',
    'tailwind': 'tailwind',
    'bootstrap': 'bootstrap',
    'material-ui': 'material-ui',
    'react': 'react', 'react.js': 'react', 'reactjs': 'react',
    'angular': 'angular',
    'vue': 'vue', 'vue.js': 'vue',
    'ember.js': 'ember.js',
    'svelte': 'svelte',
    'backbone.js': 'backbone.js',
    'polymer': 'polymer',
    'jquery': 'jquery',
    'next.js': 'next.js',
    'nuxt.js': 'nuxt.js',
    'gatsby': 'gatsby',
    'webpack': 'webpack',
    'vite': 'vite',
    'babel': 'babel',
    'redux': 'redux',
    'mobx': 'mobx',
    'rxjs': 'rxjs',
    'd3.js': 'd3.js',
    'three.js': 'three.js',
    'webgl': 'webgl',
    'webassembly': 'webassembly', 'wasm': 'webassembly',

    # --- Backend & Frameworks ---
    'node.js': 'node.js', 'node': 'node.js',
    'express': 'express',
    'django': 'django',
    'flask': 'flask',
    'fastapi': 'fastapi',
    'tornado': 'tornado',
    'rails': 'ruby on rails', 'ruby on rails': 'ruby on rails',
    'sinatra': 'sinatra',
    'laravel': 'laravel',
    'symfony': 'symfony',
    'asp.net': 'asp.net', '.net': 'asp.net', '.net core': 'asp.net',
    'spring': 'spring', 'spring boot': 'spring boot',
    'hibernate': 'hibernate',
    'jpa': 'jpa',
    'mybatis': 'mybatis',
    'sequelize': 'sequelize',
    'mongoose': 'mongoose',
    'typeorm': 'typeorm',
    'phoenix': 'phoenix',
    'play': 'play',
    'ktor': 'ktor',
    'nestjs': 'nestjs',
    'meteor': 'meteor',
    'strapi': 'strapi',
    'celery': 'celery',

    # --- Mobile Development ---
    'flutter': 'flutter',
    'react-native': 'react-native',
    'ionic': 'ionic',
    'cordova': 'cordova',
    'android': 'android',
    'ios': 'ios',
    'xamarin': 'xamarin',
    'maui': 'maui',
    'appium': 'appium',

    # --- Databases & Data Warehouses ---
    'mongodb': 'mongodb',
    'mysql': 'mysql',
    'postgresql': 'postgresql', 'postgres': 'postgresql',
    'oracle': 'oracle',
    'sqlite': 'sqlite',
    'redis': 'redis',
    'memcached': 'memcached',
    'cassandra': 'cassandra',
    'elasticsearch': 'elasticsearch',
    'neo4j': 'neo4j',
    'mariadb': 'mariadb',
    'sql server': 'sql server', 'mssql': 'sql server',
    'db2': 'db2',
    'couchbase': 'couchbase',
    'couchdb': 'couchdb',
    'dynamodb': 'dynamodb',
    'cosmosdb': 'cosmosdb',
    'firebase': 'firebase',
    'supabase': 'supabase',
    'snowflake': 'snowflake',
    'redshift': 'redshift',
    'bigquery': 'bigquery',
    'databricks': 'databricks',
    'teradata': 'teradata',
    'presto': 'presto',
    'athena': 'athena',
    'hbase': 'hbase',
    'influxdb': 'influxdb',
    'timescaledb': 'timescaledb',
    'realm': 'realm',

    # --- Message Brokers & Streaming ---
    'kafka': 'kafka',
    'rabbitmq': 'rabbitmq',
    'activemq': 'activemq',
    'zeromq': 'zeromq',
    'pulsar': 'pulsar',
    'kinesis': 'kinesis',
    'spark': 'spark',
    'flink': 'flink',
    'hadoop': 'hadoop',
    'hive': 'hive',
    'storm': 'storm',

    # --- AI, Machine Learning & Data Science ---
    'pandas': 'pandas',
    'numpy': 'numpy',
    'scipy': 'scipy',
    'scikit-learn': 'scikit-learn',
    'tensorflow': 'tensorflow',
    'pytorch': 'pytorch',
    'keras': 'keras',
    'opencv': 'opencv',
    'nltk': 'nltk',
    'spacy': 'spacy',
    'hugging face': 'hugging face',
    'langchain': 'langchain',
    'jupyter': 'jupyter',
    'matplotlib': 'matplotlib',
    'seaborn': 'seaborn',
    'llm': 'llm',
    'openai': 'openai',

    # --- DevOps, CI/CD & Infrastructure as Code ---
    'jenkins': 'jenkins',
    'gitlab-ci': 'gitlab-ci',
    'github-actions': 'github-actions',
    'circleci': 'circleci',
    'travis-ci': 'travis-ci',
    'bitbucket-pipelines': 'bitbucket-pipelines',
    'ansible': 'ansible',
    'terraform': 'terraform',
    'puppet': 'puppet',
    'chef': 'chef',
    'saltstack': 'saltstack',
    'argocd': 'argocd',
    'flux': 'flux',
    'sonarcloud': 'sonarcloud',
    'sonarqube': 'sonarqube',

    # --- Containers & Orchestration ---
    'docker': 'docker', 'docker-compose': 'docker-compose',
    'kubernetes': 'kubernetes', 'k8s': 'kubernetes',
    'eks': 'eks', 'gke': 'gke', 'aks': 'aks',
    'openshift': 'openshift',
    'helm': 'helm',

    # --- Cloud Platforms & Services ---
    'aws': 'aws', 'gcp': 'gcp', 'azure': 'azure',
    'heroku': 'heroku', 'digitalocean': 'digitalocean', 'linode': 'linode',
    'vercel': 'vercel', 'netlify': 'netlify', 'cloudflare': 'cloudflare', 'fastly': 'fastly',
    'aws-s3': 'aws-s3', 'azure-blob-storage': 'azure-blob-storage', 'google-cloud-storage': 'google-cloud-storage',
    'aws-sqs': 'aws-sqs', 'azure-queue-storage': 'azure-queue-storage', 'google-cloud-pubsub': 'google-cloud-pubsub',
    'aws-dynamodb': 'aws-dynamodb', 'azure-cosmosdb': 'azure-cosmosdb', 'google-cloud-datastore': 'google-cloud-datastore',
    'aws-rds': 'aws-rds', 'azure-sql': 'azure-sql', 'google-cloud-sql': 'google-cloud-sql',
    'aws-elb': 'aws-elb', 'azure-lb': 'azure-lb', 'google-cloud-ilb': 'google-cloud-ilb',
    'aws-ecs': 'aws-ecs', 'azure-acr': 'azure-acr', 'google-cloud-container-builder': 'google-cloud-container-builder',
    'aws-codebuild': 'aws-codebuild', 'azure-devops': 'azure-devops', 'google-cloud-build': 'google-cloud-build',
    'aws-vpc': 'aws-vpc', 'azure-vnet': 'azure-vnet', 'google-cloud-vpc': 'google-cloud-vpc',
    'aws-eks': 'aws-eks', 'azure-aks': 'azure-aks', 'google-cloud-gke': 'google-cloud-gke',
    'aws-lambda': 'aws-lambda', 'azure-functions': 'azure-functions', 'google-cloud-functions': 'google-cloud-functions',
    'firebase-functions': 'firebase-functions',

    # --- Servers, Proxies & API Gateways ---
    'apache': 'apache',
    'nginx': 'nginx',
    'haproxy': 'haproxy',
    'traefik': 'traefik',
    'tomcat': 'tomcat',
    'api-gateway': 'api-gateway',
    'nginx-ingress': 'nginx-ingress',
    'haproxy-load-balancer': 'haproxy-load-balancer',
    'traefik-router': 'traefik-router',
    'kong-api-gateway': 'kong-api-gateway',

    # --- Monitoring, Logging & Observability ---
    'kibana': 'kibana',
    'grafana': 'grafana',
    'prometheus': 'prometheus',
    'graylog': 'graylog',
    'splunk': 'splunk',
    'logstash': 'logstash',
    'datadog': 'datadog',
    'new relic': 'new relic',
    'appdynamics': 'appdynamics',
    'dynatrace': 'dynatrace',
    'sentry': 'sentry',
    'elk': 'elk',

    # --- Security, Identity & Auth ---
    'istio': 'istio',
    'linkerd': 'linkerd',
    'consul': 'consul',
    'vault': 'vault',
    'opa': 'opa',
    'csp': 'csp',
    'mfa': 'mfa',
    'oauth2': 'oauth2', 'oauth2-auth': 'oauth2',
    'jwt': 'jwt', 'jwt-auth': 'jwt',
    'openapi': 'openapi',
    'graphql': 'graphql',
    'grpc': 'grpc',
    'protobuf': 'protobuf',
    'restful': 'restful',
    'aws-iam': 'aws-iam',
    'azure-ad': 'azure-ad',
    'okta': 'okta',
    'auth0': 'auth0',
    'keycloak': 'keycloak',
    'openid-connect': 'openid-connect',
    'saml': 'saml',
    'aws-waf': 'aws-waf',
    'azure-waf': 'azure-waf',
    'google-cloud-waf': 'google-cloud-waf',
    'gcp-firewall': 'gcp-firewall',

    # --- Architecture & Concepts ---
    'microservices': 'microservices',
    'serverless': 'serverless',
    'lambda': 'lambda',
    'rest': 'rest',
    'soap': 'soap',
    'tdd': 'tdd',
    'bdd': 'bdd',
    'agile': 'agile',
    'scrum': 'scrum',
    'kanban': 'kanban',

    # --- Testing ---
    'selenium': 'selenium',
    'cypress': 'cypress',
    'puppeteer': 'puppeteer',
    'playwright': 'playwright',
    'jest': 'jest',
    'mocha': 'mocha',
    'jasmine': 'jasmine',
    'karma': 'karma',
    'pytest': 'pytest',
    'junit': 'junit',
    'nunit': 'nunit',
    'testng': 'testng',
    'postman': 'postman',
    'soapui': 'soapui',

    # --- Version Control & OS ---
    'git': 'git',
    'github': 'github',
    'gitlab': 'gitlab',
    'bitbucket': 'bitbucket',
    'svn': 'svn',
    'mercurial': 'mercurial',
    'linux': 'linux',
    'unix': 'unix',
    'windows': 'windows',
    'macos': 'macos',
    'ubuntu': 'ubuntu',
    'centos': 'centos',
    'debian': 'debian',
    'redhat': 'redhat',
    'alpine': 'alpine'
}   

def extract_tech_stack(description, tech_map):
    if not isinstance(description, str):
        return []
    try:
        description_lower = description.lower()
        tech_stack = set() # Using a set automatically prevents duplicates
        
        # Sort keys by length descending so 'react.js' is checked BEFORE 'react'
        sorted_keywords = sorted(tech_map.keys(), key=len, reverse=True)
        
        for keyword in sorted_keywords:
            master_name = tech_map[keyword]
            
            # If we already found this technology
            if master_name in tech_stack:
                continue
                
            pattern = r'(?<![\w\-])' + re.escape(keyword.lower()) + r'(?![\w\-\+#])'
            if re.search(pattern, description_lower):
                tech_stack.add(master_name) 
                
        return list(tech_stack)
        
    except Exception as e:
        print(f"Error occurred while extracting Tech stacks from Descriptions with error: {e}\n")
        return []

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
            supabase.from_("jobs").upsert([job_data]).execute()

        # Insert tech stack into the 'tech_stack' table
        for _, job in cleaned_jobs.iterrows():
            for tech in job["tech_stack"]:
                tech_stack_data = {
                    "job_id": job["id"],
                    "technology": tech
                }
                supabase.from_("tech_stack").upsert(
                    [tech_stack_data], 
                    on_conflict="job_id, technology"
                ).execute()

    except Exception as e:
        print(f"Error occured when inserting data into database with error : {e}\n")

try:
    j = 0
    jobs = pd.DataFrame()
    for i in range(12):
        new_data = scrape_jobs(
            site_name=["linkedin","indeed"],
            search_term="Software engineer",
            location="Germany",
            country_indeed='Germany',
            results_wanted=1000,
            linkedin_fetch_description=True,
            proxies=None, # I do not have any :)
            offset=j
        )
        jobs = pd.concat([jobs, new_data], ignore_index=True)
        j+= 1000
        time.sleep(i)
    
    jobs['tech_stack'] = jobs['description'].apply(lambda desc: extract_tech_stack(desc, tech_keywords_map))

    cleaned_jobs = clean_job_data(jobs)

    insert_data_to_supabase(cleaned_jobs)
except Exception as e:
    print(f"Could not scrape Jobs, failed with error: {e}\n")
