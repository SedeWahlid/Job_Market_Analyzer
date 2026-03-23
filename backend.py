from jobspy import scrape_jobs
import pandas as pd
from supabase import create_client
from os import getenv
from numpy import nan

 
#################################################
# SCRAPING DATA + CONNECTION TO DATABASE
#################################################

def clean_job_data(jobs):
    # Select only the necessary rows and columns 
    cleaned_jobs = pd.DataFrame(jobs[['id', 'title', 'company', 'job_type', 'job_level', 'tech_stack']]).drop_duplicates(subset=['title', 'company', 'job_type'], keep='first').replace(nan,None)
    return cleaned_jobs

def extract_tech_stack(description, tech_keywords):
    # Convert description to lowercase for case-insensitive matching
    description_lower = description.lower()
    
    # Find all tech stack items present in the description
    tech_stack = [keyword for keyword in tech_keywords if keyword.lower() in description_lower]
    
    return tech_stack

def insert_data_to_supabase(cleaned_jobs):

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

# list of technology keywords to look for in job descriptions
tech_keywords = [
    'python', 'java', 'c++', 'javascript', 'sql', 'aws', 'docker', 'kubernetes', 
    'react', 'angular', 'ruby', 'go', 'swift', 'kotlin', 'typescript', 'scala', 
    'perl', 'php', 'rust', 'haskell', 'elixir', 'clojure', 'groovy', 'objective-c', 
    'vue.js', 'ember.js', 'svelte', 'backbone.js', 'polymer', 'flutter', 'react-native', 
    'ionic', 'cordova', 'node.js', 'express', 'django', 'flask', 'rails', 'sinatra', 
    'laravel', 'symfony', 'asp.net', 'spring', 'hibernate', 'jpa', 'mybatis', 'sequelize', 
    'mongoose', 'typeorm', 'mongodb', 'mysql', 'postgresql', 'oracle', 'sqlite', 'redis', 
    'memcached', 'cassandra', 'elasticsearch', 'neo4j', 'apache', 'nginx', 'haproxy', 
    'traefik', 'jenkins', 'gitlab-ci', 'github-actions', 'circleci', 'travis-ci', 'bitbucket-pipelines', 
    'kibana', 'grafana', 'prometheus', 'graylog', 'splunk', 'logstash', 'ansible', 
    'terraform', 'puppet', 'chef', 'saltstack', 'docker-compose', 'k8s', 'eks', 
    'gke', 'aks', 'istio', 'linkerd', 'consul', 'vault', 'opa', 'csp', 'mfa', 
    'oauth2', 'jwt', 'openapi', 'graphql', 'grpc', 'protobuf', 'restful', 'microservices', 
    'serverless', 'lambda', 'azure-functions', 'google-cloud-functions', 'firebase-functions', 
    'gcp', 'aws-iam', 'azure-ad', 'okta', 'auth0', 'keycloak', 'openid-connect', 
    'saml', 'jwt-auth', 'oauth2-auth', 'api-gateway', 'nginx-ingress', 'haproxy-load-balancer', 
    'traefik-router', 'kong-api-gateway', 'aws-waf', 'azure-waf', 'google-cloud-waf', 
    'gcp-firewall', 'aws-s3', 'azure-blob-storage', 'google-cloud-storage', 'aws-sqs', 
    'azure-queue-storage', 'google-cloud-pubsub', 'aws-dynamodb', 'azure-cosmosdb', 
    'google-cloud-datastore', 'aws-rds', 'azure-sql', 'google-cloud-sql', 'aws-elb', 
    'azure-lb', 'google-cloud-ilb', 'aws-ecs', 'azure-acr', 'google-cloud-container-builder', 
    'aws-codebuild', 'azure-devops', 'google-cloud-build', 'aws-vpc', 'azure-vnet', 
    'google-cloud-vpc', 'aws-eks', 'azure-aks', 'google-cloud-gke', 'aws-lambda', 
    'azure-functions', 'google-cloud-functions'
]

jobs = scrape_jobs(
    site_name=["linkedin","indeed"],
    search_term="Software engineer",
    location="Germany",
    hours_old=720,  # Data not older than 30 days (720 hours)
    country_indeed='Germany',
    results_wanted=40,
    linkedin_fetch_description=True,
    proxies=None # I do not have any :)
)

# Add a new column for tech stack
jobs['tech_stack'] = jobs['description'].apply(lambda desc: extract_tech_stack(desc, tech_keywords))

cleaned_jobs = clean_job_data(jobs)

# Insert cleaned data into Supabase
insert_data_to_supabase(cleaned_jobs)
