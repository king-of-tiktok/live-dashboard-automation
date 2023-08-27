#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import requests
import pandas as pd
import numpy as np
import time
import json
import re


# ### Set Up for API Calls

# In[2]:


GitHub_token = os.environ["API_KEY_GITHUB_PROJECTBOARD_DASHBOARD"]
user = 'kimberlytanyh'


# ### Get Cards in Project Board Columns

# In[3]:


### Get Cards in Ice Box

url = 'https://api.github.com/projects/columns/7198227/cards'
headers = {"Authorization": GitHub_token}
ice_box = pd.DataFrame()

for i in range(1, 11):
    params = {"per_page": 100, "page": i}
    response = requests.get(url, auth=(user, GitHub_token), params = params)
    df = pd.DataFrame(response.json())
    if len(df) > 0:
        ice_box = pd.concat([ice_box, df], ignore_index = True)
    else: 
        break


# In[4]:


### Get Cards in ER Column

url = 'https://api.github.com/projects/columns/19403960/cards'
headers = {"Authorization": GitHub_token}
er = pd.DataFrame()

for i in range(1, 11):
    params = {"per_page": 100, "page": i}
    response = requests.get(url, auth=(user, GitHub_token), params = params)
    df = pd.DataFrame(response.json())
    if len(df) > 0:
        er = pd.concat([er, df], ignore_index = True)
    else: 
        break


# In[5]:


### Get Cards in New Issue Approval Column

url = 'https://api.github.com/projects/columns/15235217/cards'
headers = {"Authorization": GitHub_token}
newissue_approval = pd.DataFrame()

for i in range(1, 11):
    params = {"per_page": 100, "page": i}
    response = requests.get(url, auth=(user, GitHub_token), params = params)
    df = pd.DataFrame(response.json())
    if len(df) > 0:
        newissue_approval = pd.concat([newissue_approval, df], ignore_index = True)
    else: 
        break


# In[6]:


### Get Cards in Prioritized Backlog Column

url = 'https://api.github.com/projects/columns/7198257/cards'
headers = {"Authorization": GitHub_token}
prioritized_backlog = pd.DataFrame()

for i in range(1, 11):
    params = {"per_page": 100, "page": i}
    response = requests.get(url, auth=(user, GitHub_token), params = params)
    df = pd.DataFrame(response.json())
    if len(df) > 0:
        prioritized_backlog = pd.concat([prioritized_backlog, df], ignore_index = True)
    else: 
        break


# In[7]:


### Get Cards in "In Progress (Actively Working)" Column

url = 'https://api.github.com/projects/columns/7198228/cards'
headers = {"Authorization": GitHub_token}
in_progress = pd.DataFrame()

for i in range(1, 11):
    params = {"per_page": 100, "page": i}
    response = requests.get(url, auth=(user, GitHub_token), params = params)
    df = pd.DataFrame(response.json())
    if len(df) > 0:
        in_progress = pd.concat([in_progress, df], ignore_index = True)
    else: 
        break


# In[8]:


### Get Cards in Questions/In Review Column

url = 'https://api.github.com/projects/columns/8178690/cards'
headers = {"Authorization": GitHub_token}
questions = pd.DataFrame()

for i in range(1, 11):
    params = {"per_page": 100, "page": i}
    response = requests.get(url, auth=(user, GitHub_token), params = params)
    df = pd.DataFrame(response.json())
    if len(df) > 0:
        questions = pd.concat([questions, df], ignore_index = True)
    else: 
        break


# In[9]:


### Get Cards in QA Column

url = 'https://api.github.com/projects/columns/15490305/cards'
headers = {"Authorization": GitHub_token}
QA = pd.DataFrame()

for i in range(1, 11):
    params = {"per_page": 100, "page": i}
    response = requests.get(url, auth=(user, GitHub_token), params = params)
    df = pd.DataFrame(response.json())
    if len(df) > 0:
        QA = pd.concat([QA, df], ignore_index = True)
    else: 
        break


# In[10]:


### Get Cards in UAT Column

url = 'https://api.github.com/projects/columns/17206624/cards'
headers = {"Authorization": GitHub_token}
UAT = pd.DataFrame()

for i in range(1, 11):
    params = {"per_page": 100, "page": i}
    response = requests.get(url, auth=(user, GitHub_token), params = params)
    df = pd.DataFrame(response.json())
    if len(df) > 0:
        UAT = pd.concat([UAT, df], ignore_index = True)
    else: 
        break


# In[11]:


### Get Cards in "QA - senior review" Column

url = 'https://api.github.com/projects/columns/19257634/cards'
headers = {"Authorization": GitHub_token}
QA_review = pd.DataFrame()

for i in range(1, 11):
    params = {"per_page": 100, "page": i}
    response = requests.get(url, auth=(user, GitHub_token), params = params)
    df = pd.DataFrame(response.json())
    if len(df) > 0:
        QA_review = pd.concat([QA_review, df], ignore_index = True)
    else: 
        break


# ### Get Issue Links in Project Board Columns

# In[12]:


### Get Issue Links in Ice Box and Get Issue Data

icebox_issues = list(ice_box[~ice_box['content_url'].isna()]['content_url'])  

icebox_issues_df = pd.DataFrame()

try:
    for url in icebox_issues:
        response = requests.get(url, auth=(user, GitHub_token))
        issue_data = pd.json_normalize(response.json())
        icebox_issues_df = pd.concat([icebox_issues_df, issue_data], ignore_index = True)
except ValueError:
    time.sleep(3600)
    for url in icebox_issues:
        response = requests.get(url, auth=(user, GitHub_token))
        issue_data = pd.json_normalize(response.json())
        icebox_issues_df = pd.concat([icebox_issues_df, issue_data], ignore_index = True)
        
from datetime import datetime
import pytz

# Get the timezone object for New York
tz_LA = pytz.timezone('US/Pacific') 

# Get the current time in New York
datetime_LA = datetime.now(tz_LA)

# Format the time as a string and print it
print("LA time:", datetime_LA.strftime("%m/%d/%Y %H:%M:%S"))

icebox_issues_df["Runtime"] = "LA time: "+datetime_LA.strftime("%m/%d/%Y %H:%M:%S")


# In[13]:


icebox_issues_df.drop(columns = ['url', 'repository_url', 'labels_url', 'comments_url', 'events_url', 'id',
 'node_id', 'number', 'state', 'locked', 'assignee', 'assignees', 'comments', 'created_at',
 'updated_at', 'closed_at', 'author_association', 'active_lock_reason', 'body', 'closed_by',
 'timeline_url', 'performed_via_github_app', 'state_reason', 'user.login', 'user.id',
 'user.node_id', 'user.avatar_url', 'user.gravatar_id', 'user.url', 'user.html_url', 'user.followers_url',
 'user.following_url', 'user.gists_url', 'user.starred_url', 'user.subscriptions_url',
 'user.organizations_url', 'user.repos_url', 'user.events_url', 'user.received_events_url',
 'user.type', 'user.site_admin', 'milestone.url', 'milestone.html_url', 'milestone.labels_url',
 'milestone.id', 'milestone.node_id', 'milestone.number', 'milestone.title', 'milestone.description',
 'milestone.creator.login', 'milestone.creator.id', 'milestone.creator.node_id', 'milestone.creator.avatar_url',
 'milestone.creator.gravatar_id', 'milestone.creator.url', 'milestone.creator.html_url',
 'milestone.creator.followers_url', 'milestone.creator.following_url', 'milestone.creator.gists_url', 'milestone.creator.starred_url',
 'milestone.creator.subscriptions_url', 'milestone.creator.organizations_url', 'milestone.creator.repos_url',
 'milestone.creator.events_url', 'milestone.creator.received_events_url',
 'milestone.creator.type', 'milestone.creator.site_admin', 'milestone.open_issues', 'milestone.closed_issues',
 'milestone.state', 'milestone.created_at', 'milestone.updated_at', 'milestone.due_on',
 'milestone.closed_at', 'reactions.url', 'reactions.total_count', 'reactions.+1', 'reactions.-1', 'reactions.laugh',
 'reactions.hooray', 'reactions.confused', 'reactions.heart', 'reactions.rocket', 'reactions.eyes',
 'assignee.login', 'assignee.id', 'assignee.node_id', 'assignee.avatar_url', 'assignee.gravatar_id', 'assignee.url',
 'assignee.html_url', 'assignee.followers_url', 'assignee.following_url', 'assignee.gists_url',
 'assignee.starred_url', 'assignee.subscriptions_url', 'assignee.organizations_url',
 'assignee.repos_url', 'assignee.events_url', 'assignee.received_events_url', 'assignee.type',
 'assignee.site_admin', 'closed_by.login',
 'closed_by.id', 'closed_by.node_id', 'closed_by.avatar_url', 'closed_by.gravatar_id', 'closed_by.url',
 'closed_by.html_url', 'closed_by.followers_url', 'closed_by.following_url', 'closed_by.gists_url',
 'closed_by.starred_url', 'closed_by.subscriptions_url', 'closed_by.organizations_url', 'closed_by.repos_url',
 'closed_by.events_url', 'closed_by.received_events_url', 'closed_by.type', 'closed_by.site_admin'], inplace = True)


# In[14]:


# Flatten labels column

flatten_icebox = icebox_issues_df.to_json(orient = "records")
parsed_icebox = json.loads(flatten_icebox)
icebox_issues_df2 = pd.json_normalize(parsed_icebox, record_path = ["labels"], record_prefix = "labels.", meta = ["Runtime", "html_url", "title"])

icebox_issues_df2.drop(columns = ['labels.id', 'labels.node_id', 'labels.url', 'labels.description',
       'labels.color', 'labels.default'], inplace = True)

# Remove issues with ignore labels in icebox column
if len([label for label in icebox_issues_df2["labels.name"].unique() if re.search('ignore', label.lower())])>0:
    remove = list(icebox_issues_df2[icebox_issues_df2["labels.name"].str.contains("gnore")]["html_url"])
    icebox_issues_df2 = icebox_issues_df2[~icebox_issues_df2["html_url"].isin(remove)]
else:
    remove = []

# Finishing touches for icebox dataset (include issues with no labels)
icebox_difference = list(set(icebox_issues_df["html_url"]).difference(set(icebox_issues_df2["html_url"])))
icebox_no_labels = list(set(icebox_difference).difference(set(remove)))
icebox_no_labels_df = icebox_issues_df[icebox_issues_df["html_url"].isin(icebox_no_labels)][["Runtime", "html_url", "title"]]
icebox_no_labels_df["labels.name"] = ""
icebox_no_labels_df = icebox_no_labels_df.iloc[:, [3,0,1,2]]

# CHANGES MADE HERE!!!!!
icebox_issues_df3 = pd.concat([icebox_issues_df2, icebox_no_labels_df], ignore_index = True)
############################################################################################

icebox_issues_df3["Project Board Column"] = "1 - Icebox"


# ### Digress: Create Variables with List of Complexity Labels and Status Breakdowns

# In[15]:


complexity_labels = ["Complexity: Prework", "Complexity: Missing", "Complexity: Large", 
                     "Complexity: Extra Large", "Complexity: Small", "good first issue", 
                     "Complexity: Medium", "Complexity: See issue making label", "prework", 
                     "Complexity: Good second issue"]


# In[16]:


extra_breakdown = ["Draft", "2 weeks inactive", "ready for product", 
                   "ready for dev lead", "Ready for Prioritization"]


# ### Back to Getting Issue Links and Creating Datasets for Each Project Board Column

# In[17]:


#### Finish up creating Icebox dataset

# retain only labels with "role" in it or complexity labels, and "Draft", "ready for product", "ready for prioritization", "ready for dev lead"
final_icebox = icebox_issues_df2[(icebox_issues_df2["labels.name"].str.contains("role") | icebox_issues_df2["labels.name"].isin(complexity_labels) | 
                          icebox_issues_df2["labels.name"].isin(extra_breakdown))]

# Make combined label for issues with front and backend labels
icebox_wdataset = final_icebox[final_icebox["labels.name"].str.contains("front end") | final_icebox["labels.name"].str.contains("back end")]
icebox_wdataset["front/back end count"] = icebox_wdataset.groupby(["html_url", "title"])["labels.name"].transform("count")

final_icebox.loc[list(icebox_wdataset[icebox_wdataset["front/back end count"] == 2].index), "labels.name"] = "role: front end and backend/DevOps"

final_icebox.drop_duplicates(inplace = True)

final_icebox2 = final_icebox[["Runtime", "labels.name", "html_url", "title"]]


# In[18]:


### Get Issue Links in ER Column and Get Issue Data

er_issues = list(er[~er['content_url'].isna()]['content_url'])  

ER_issues_df = pd.DataFrame()

try:
    for url in er_issues:
        response = requests.get(url, auth=(user, GitHub_token))
        issue_data = pd.json_normalize(response.json())
        ER_issues_df = pd.concat([ER_issues_df, issue_data], ignore_index = True)
except ValueError:
    time.sleep(3600)
    for url in er_issues:
        response = requests.get(url, auth=(user, GitHub_token))
        issue_data = pd.json_normalize(response.json())
        ER_issues_df = pd.concat([ER_issues_df, issue_data], ignore_index = True)

# Get the timezone object for New York
tz_LA = pytz.timezone('US/Pacific') 

# Get the current time in New York
datetime_LA = datetime.now(tz_LA)

# Format the time as a string and print it
print("LA time:", datetime_LA.strftime("%m/%d/%Y %H:%M:%S"))

ER_issues_df["Runtime"] = "LA time: "+datetime_LA.strftime("%m/%d/%Y %H:%M:%S")

# Drop unneeded columns
ER_issues_df.drop(columns = ['url', 'repository_url', 'labels_url', 'comments_url', 'events_url', 'id',
 'node_id', 'number', 'state', 'locked', 'assignee', 'assignees', 'comments', 'created_at',
 'updated_at', 'closed_at', 'author_association', 'active_lock_reason', 'body', 'closed_by',
 'timeline_url', 'performed_via_github_app', 'state_reason', 'user.login', 'user.id',
 'user.node_id', 'user.avatar_url', 'user.gravatar_id', 'user.url', 'user.html_url', 'user.followers_url',
 'user.following_url', 'user.gists_url', 'user.starred_url', 'user.subscriptions_url',
 'user.organizations_url', 'user.repos_url', 'user.events_url', 'user.received_events_url',
 'user.type', 'user.site_admin', 'milestone.url', 'milestone.html_url', 'milestone.labels_url',
 'milestone.id', 'milestone.node_id', 'milestone.number', 'milestone.title', 'milestone.description',
 'milestone.creator.login', 'milestone.creator.id', 'milestone.creator.node_id', 'milestone.creator.avatar_url',
 'milestone.creator.gravatar_id', 'milestone.creator.url', 'milestone.creator.html_url',
 'milestone.creator.followers_url', 'milestone.creator.following_url', 'milestone.creator.gists_url', 'milestone.creator.starred_url',
 'milestone.creator.subscriptions_url', 'milestone.creator.organizations_url', 'milestone.creator.repos_url',
 'milestone.creator.events_url', 'milestone.creator.received_events_url',
 'milestone.creator.type', 'milestone.creator.site_admin', 'milestone.open_issues', 'milestone.closed_issues',
 'milestone.state', 'milestone.created_at', 'milestone.updated_at', 'milestone.due_on',
 'milestone.closed_at', 'reactions.url', 'reactions.total_count', 'reactions.+1', 'reactions.-1', 'reactions.laugh',
 'reactions.hooray', 'reactions.confused', 'reactions.heart', 'reactions.rocket', 'reactions.eyes',
 'assignee.login', 'assignee.id', 'assignee.node_id', 'assignee.avatar_url', 'assignee.gravatar_id', 'assignee.url',
 'assignee.html_url', 'assignee.followers_url', 'assignee.following_url', 'assignee.gists_url',
 'assignee.starred_url', 'assignee.subscriptions_url', 'assignee.organizations_url',
 'assignee.repos_url', 'assignee.events_url', 'assignee.received_events_url', 'assignee.type',
 'assignee.site_admin', 'closed_by.login',
 'closed_by.id', 'closed_by.node_id', 'closed_by.avatar_url', 'closed_by.gravatar_id', 'closed_by.url',
 'closed_by.html_url', 'closed_by.followers_url', 'closed_by.following_url', 'closed_by.gists_url',
 'closed_by.starred_url', 'closed_by.subscriptions_url', 'closed_by.organizations_url', 'closed_by.repos_url',
 'closed_by.events_url', 'closed_by.received_events_url', 'closed_by.type', 'closed_by.site_admin'], inplace = True)

# Flatten labels column

flatten_ER = ER_issues_df.to_json(orient = "records")
parsed_ER = json.loads(flatten_ER)
ER_issues_df2 = pd.json_normalize(parsed_ER, record_path = ["labels"], record_prefix = "labels.", meta = ["Runtime", "html_url", "title"])


ER_issues_df2.drop(columns = ['labels.id', 'labels.node_id', 'labels.url', 'labels.description',
       'labels.color', 'labels.default'], inplace = True)

# Remove issues with ignore labels in ER column
if len([label for label in ER_issues_df2["labels.name"].unique() if re.search('ignore', label.lower())])>0:
    remove = list(ER_issues_df2[ER_issues_df2["labels.name"].str.contains("gnore")]["html_url"])
    ER_issues_df2 = ER_issues_df2[~ER_issues_df2["html_url"].isin(remove)]
else:
    remove = []

# Finishing touches for ER dataset (include issues with no labels)
ER_difference = list(set(ER_issues_df["html_url"]).difference(set(ER_issues_df2["html_url"])))
ER_no_labels = list(set(ER_difference).difference(set(remove)))
ER_no_labels_df = ER_issues_df[ER_issues_df["html_url"].isin(ER_no_labels)][["Runtime", "html_url", "title"]]
ER_no_labels_df["labels.name"] = ""
ER_no_labels_df = ER_no_labels_df.iloc[:, [3,0,1,2]]

# CHANGES MADE HERE!!!!!
ER_issues_df3 = pd.concat([ER_issues_df2, ER_no_labels_df], ignore_index = True)
########################################################################################

ER_issues_df3["Project Board Column"] = "2- ER"

# retain only labels with "role" in it or complexity labels, and "Draft", "ready for product", "ready for prioritization", "ready for dev lead"
final_ER = ER_issues_df2[(ER_issues_df2["labels.name"].str.contains("role") | ER_issues_df2["labels.name"].isin(complexity_labels) | 
                          ER_issues_df2["labels.name"].isin(extra_breakdown))]

# Make combined label for issues with front and backend labels
ER_wdataset = final_ER[final_ER["labels.name"].str.contains("front end") | final_ER["labels.name"].str.contains("back end")]
ER_wdataset["front/back end count"] = ER_wdataset.groupby(["html_url", "title"])["labels.name"].transform("count")

final_ER.loc[list(ER_wdataset[ER_wdataset["front/back end count"] == 2].index), "labels.name"] = "role: front end and backend/DevOps"

final_ER.drop_duplicates(inplace = True)

final_ER2 = final_ER[["Runtime", "labels.name", "html_url", "title"]]


# In[19]:


### Get Issue Links in New Issue Approval Column and Get Issue Data

# Retain only issue cards (take out description cards)

newissue_approval = newissue_approval[~newissue_approval["content_url"].isna()]

NIA_issues = list(newissue_approval['content_url'])

NIA_issues_df = pd.DataFrame()

try:
    for url in NIA_issues:
        response = requests.get(url, auth=(user, GitHub_token))
        issue_data = pd.json_normalize(response.json())
        NIA_issues_df = pd.concat([NIA_issues_df, issue_data], ignore_index = True)
except ValueError:
    time.sleep(3600)
    for url in NIA_issues:
        response = requests.get(url, auth=(user, GitHub_token))
        issue_data = pd.json_normalize(response.json())
        NIA_issues_df = pd.concat([NIA_issues_df, issue_data], ignore_index = True)


# Get the timezone object for New York
tz_LA = pytz.timezone('US/Pacific') 

# Get the current time in New York
datetime_LA = datetime.now(tz_LA)

# Format the time as a string and print it
print("LA time:", datetime_LA.strftime("%m/%d/%Y %H:%M:%S"))

NIA_issues_df["Runtime"] = "LA time: "+datetime_LA.strftime("%m/%d/%Y %H:%M:%S")

# Drop unneeded columns
NIA_issues_df.drop(columns = ['url', 'repository_url', 'labels_url', 'comments_url', 'events_url', 'id',
 'node_id', 'number', 'state', 'locked', 'assignee', 'assignees', 'comments', 'created_at',
 'updated_at', 'closed_at', 'author_association', 'active_lock_reason', 'body', 'closed_by',
 'timeline_url', 'performed_via_github_app', 'state_reason', 'user.login', 'user.id',
 'user.node_id', 'user.avatar_url', 'user.gravatar_id', 'user.url', 'user.html_url', 'user.followers_url',
 'user.following_url', 'user.gists_url', 'user.starred_url', 'user.subscriptions_url',
 'user.organizations_url', 'user.repos_url', 'user.events_url', 'user.received_events_url',
 'user.type', 'user.site_admin', 'milestone.url', 'milestone.html_url', 'milestone.labels_url',
 'milestone.id', 'milestone.node_id', 'milestone.number', 'milestone.title', 'milestone.description',
 'milestone.creator.login', 'milestone.creator.id', 'milestone.creator.node_id', 'milestone.creator.avatar_url',
 'milestone.creator.gravatar_id', 'milestone.creator.url', 'milestone.creator.html_url',
 'milestone.creator.followers_url', 'milestone.creator.following_url', 'milestone.creator.gists_url', 'milestone.creator.starred_url',
 'milestone.creator.subscriptions_url', 'milestone.creator.organizations_url', 'milestone.creator.repos_url',
 'milestone.creator.events_url', 'milestone.creator.received_events_url',
 'milestone.creator.type', 'milestone.creator.site_admin', 'milestone.open_issues', 'milestone.closed_issues',
 'milestone.state', 'milestone.created_at', 'milestone.updated_at', 'milestone.due_on',
 'milestone.closed_at', 'reactions.url', 'reactions.total_count', 'reactions.+1', 'reactions.-1', 'reactions.laugh',
 'reactions.hooray', 'reactions.confused', 'reactions.heart', 'reactions.rocket', 'reactions.eyes',
 'assignee.login', 'assignee.id', 'assignee.node_id', 'assignee.avatar_url', 'assignee.gravatar_id', 'assignee.url',
 'assignee.html_url', 'assignee.followers_url', 'assignee.following_url', 'assignee.gists_url',
 'assignee.starred_url', 'assignee.subscriptions_url', 'assignee.organizations_url',
 'assignee.repos_url', 'assignee.events_url', 'assignee.received_events_url', 'assignee.type',
 'assignee.site_admin', 'closed_by.login',
 'closed_by.id', 'closed_by.node_id', 'closed_by.avatar_url', 'closed_by.gravatar_id', 'closed_by.url',
 'closed_by.html_url', 'closed_by.followers_url', 'closed_by.following_url', 'closed_by.gists_url',
 'closed_by.starred_url', 'closed_by.subscriptions_url', 'closed_by.organizations_url', 'closed_by.repos_url',
 'closed_by.events_url', 'closed_by.received_events_url', 'closed_by.type', 'closed_by.site_admin'], inplace = True)

flatten_NIA = NIA_issues_df.to_json(orient = "records")
parsed_NIA = json.loads(flatten_NIA)
NIA_issues_df2 = pd.json_normalize(parsed_NIA, record_path = ["labels"], record_prefix = "labels.", meta = ["Runtime", "html_url", "title"])


NIA_issues_df2.drop(columns = ['labels.id', 'labels.node_id', 'labels.url', 'labels.description',
       'labels.color', 'labels.default'], inplace = True)

# Remove issues with ignore labels in NIA column
if len([label for label in NIA_issues_df2["labels.name"].unique() if re.search('ignore', label.lower())])>0:
    remove = list(NIA_issues_df2[NIA_issues_df2["labels.name"].str.contains("gnore")]["html_url"])
    NIA_issues_df2 = NIA_issues_df2[~NIA_issues_df2["html_url"].isin(remove)]
else:
    remove = []
    
# Finishing touches for ER dataset (include issues with no labels)
NIA_difference = list(set(NIA_issues_df["html_url"]).difference(set(NIA_issues_df2["html_url"])))
NIA_no_labels = list(set(NIA_difference).difference(set(remove)))
NIA_no_labels_df = NIA_issues_df[NIA_issues_df["html_url"].isin(NIA_no_labels)][["Runtime", "html_url", "title"]]
NIA_no_labels_df["labels.name"] = ""
NIA_no_labels_df = NIA_no_labels_df.iloc[:, [3,0,1,2]]

# CHANGES MADE HERE!!!!!
NIA_issues_df3 = pd.concat([NIA_issues_df2, NIA_no_labels_df], ignore_index = True)
#############################################################################################

NIA_issues_df3["Project Board Column"] = "3 - New Issue Approval"
len(NIA_issues_df3)

# retain only labels with "role" in it or complexity labels, and "Draft", "ready for product", "ready for prioritization", "ready for dev lead"
final_NIA = NIA_issues_df2[(NIA_issues_df2["labels.name"].str.contains("role") | NIA_issues_df2["labels.name"].isin(complexity_labels) | 
                          NIA_issues_df2["labels.name"].isin(extra_breakdown))]

# Make combined label for issues with front and backend labels
NIA_wdataset = final_NIA[final_NIA["labels.name"].str.contains("front end") | final_NIA["labels.name"].str.contains("back end")]
NIA_wdataset["front/back end count"] = NIA_wdataset.groupby(["html_url", "title"])["labels.name"].transform("count")

final_NIA.loc[list(NIA_wdataset[NIA_wdataset["front/back end count"] == 2].index), "labels.name"] = "role: front end and backend/DevOps"

final_NIA.drop_duplicates(inplace = True)

final_NIA2 = final_NIA[["Runtime", "labels.name", "html_url", "title"]]


# In[20]:


### Get Issue Links in Prioritized Backlog Column and Get Issue Data

# Retain only issue cards (take out description cards)

prioritized_backlog = prioritized_backlog[~prioritized_backlog["content_url"].isna()]

pb_issues = list(prioritized_backlog["content_url"])

pb_issues_df = pd.DataFrame()

try:
    for url in pb_issues:
        response = requests.get(url, auth=(user, GitHub_token))
        issue_data = pd.json_normalize(response.json())
        pb_issues_df = pd.concat([pb_issues_df, issue_data], ignore_index = True)
except ValueError:
    time.sleep(3600)
    for url in pb_issues:
        response = requests.get(url, auth=(user, GitHub_token))
        issue_data = pd.json_normalize(response.json())
        pb_issues_df = pd.concat([pb_issues_df, issue_data], ignore_index = True)
        
# Get the timezone object for New York
tz_LA = pytz.timezone('US/Pacific') 

# Get the current time in New York
datetime_LA = datetime.now(tz_LA)

# Format the time as a string and print it
print("LA time:", datetime_LA.strftime("%m/%d/%Y %H:%M:%S"))

pb_issues_df["Runtime"] = "LA time: "+datetime_LA.strftime("%m/%d/%Y %H:%M:%S")

# Drop unneeded columns
pb_issues_df.drop(columns = ['url', 'repository_url', 'labels_url', 'comments_url', 'events_url', 'id',
 'node_id', 'number', 'state', 'locked', 'assignee', 'assignees', 'comments', 'created_at',
 'updated_at', 'closed_at', 'author_association', 'active_lock_reason', 'body', 'closed_by',
 'timeline_url', 'performed_via_github_app', 'state_reason', 'user.login', 'user.id',
 'user.node_id', 'user.avatar_url', 'user.gravatar_id', 'user.url', 'user.html_url', 'user.followers_url',
 'user.following_url', 'user.gists_url', 'user.starred_url', 'user.subscriptions_url',
 'user.organizations_url', 'user.repos_url', 'user.events_url', 'user.received_events_url',
 'user.type', 'user.site_admin', 'milestone.url', 'milestone.html_url', 'milestone.labels_url',
 'milestone.id', 'milestone.node_id', 'milestone.number', 'milestone.title', 'milestone.description',
 'milestone.creator.login', 'milestone.creator.id', 'milestone.creator.node_id', 'milestone.creator.avatar_url',
 'milestone.creator.gravatar_id', 'milestone.creator.url', 'milestone.creator.html_url',
 'milestone.creator.followers_url', 'milestone.creator.following_url', 'milestone.creator.gists_url', 'milestone.creator.starred_url',
 'milestone.creator.subscriptions_url', 'milestone.creator.organizations_url', 'milestone.creator.repos_url',
 'milestone.creator.events_url', 'milestone.creator.received_events_url',
 'milestone.creator.type', 'milestone.creator.site_admin', 'milestone.open_issues', 'milestone.closed_issues',
 'milestone.state', 'milestone.created_at', 'milestone.updated_at', 'milestone.due_on',
 'milestone.closed_at', 'reactions.url', 'reactions.total_count', 'reactions.+1', 'reactions.-1', 'reactions.laugh',
 'reactions.hooray', 'reactions.confused', 'reactions.heart', 'reactions.rocket', 'reactions.eyes', 'closed_by.login',
 'closed_by.id', 'closed_by.node_id', 'closed_by.avatar_url', 'closed_by.gravatar_id', 'closed_by.url',
 'closed_by.html_url', 'closed_by.followers_url', 'closed_by.following_url', 'closed_by.gists_url',
 'closed_by.starred_url', 'closed_by.subscriptions_url', 'closed_by.organizations_url', 'closed_by.repos_url',
 'closed_by.events_url', 'closed_by.received_events_url', 'closed_by.type', 'closed_by.site_admin'], inplace = True)

# Expand labels column (flatten)

flatten_pb = pb_issues_df.to_json(orient = "records")
parsed_pb = json.loads(flatten_pb)
pb_issues_df2 = pd.json_normalize(parsed_pb, record_path = ['labels'], record_prefix = 'labels.', meta = ["Runtime", "html_url", "title"])

# Drop unneeded columns

pb_issues_df2.drop(columns = ['labels.id', 'labels.node_id', 'labels.url', 'labels.description',
       'labels.color', 'labels.default'], inplace = True)

# Remove issues with ignore labels in Prioritized Backlog column
if len([label for label in pb_issues_df2["labels.name"].unique() if re.search('ignore', label.lower())])>0:
    remove = list(pb_issues_df2[pb_issues_df2["labels.name"].str.contains("gnore")]["html_url"])
    pb_issues_df2 = pb_issues_df2[~pb_issues_df2["html_url"].isin(remove)]
else:
    remove = []
    
# Finishing touches for Prioritized Backlog dataset (include issues with no labels)
pb_difference = list(set(pb_issues_df["html_url"]).difference(set(pb_issues_df2["html_url"])))
pb_no_labels = list(set(pb_difference).difference(set(remove)))
pb_no_labels_df = pb_issues_df[pb_issues_df["html_url"].isin(pb_no_labels)][["Runtime", "html_url", "title"]]
pb_no_labels_df["labels.name"] = ""
pb_no_labels_df = pb_no_labels_df.iloc[:, [3,0,1,2]]

# CHANGES MADE HERE!!!!!
pb_issues_df3 = pd.concat([pb_issues_df2, pb_no_labels_df], ignore_index = False)
##############################################################################################################

pb_issues_df3["Project Board Column"] = "4 - Prioritized Backlog"

# retain only labels with "role" in it, complexity labels, and "Draft", "ready for product", "ready for prioritization", "ready for dev lead"

final_pb = pb_issues_df2[(pb_issues_df2["labels.name"].str.contains("role") | pb_issues_df2["labels.name"].isin(complexity_labels) | 
                          pb_issues_df2["labels.name"].isin(extra_breakdown))]

# Make combined label for issues with front and backend labels
pb_wdataset = final_pb[final_pb["labels.name"].str.contains("front end") | final_pb["labels.name"].str.contains("back end")]
pb_wdataset["front/back end count"] = pb_wdataset.groupby(["html_url", "title"])["labels.name"].transform("count")

final_pb.loc[list(pb_wdataset[pb_wdataset["front/back end count"] == 2].index), "labels.name"] = "role: front end and backend/DevOps"

final_pb.drop_duplicates(inplace = True)

final_pb2 = final_pb[["Runtime","labels.name", "html_url", "title"]]


# In[21]:


### Get Issue Links in "In Progress Column" and Get Issue Data

in_progress_issues = list(in_progress[~in_progress['content_url'].isna()]['content_url']) 

ip_df = pd.DataFrame()

try:
    for url in in_progress_issues:
        response = requests.get(url, auth=(user, GitHub_token))
        issue_data = pd.json_normalize(response.json())
        ip_df = pd.concat([ip_df, issue_data], ignore_index = True)
except ValueError:
    time.sleep(3600)
    for url in in_progress_issues:
        response = requests.get(url, auth=(user, GitHub_token))
        issue_data = pd.json_normalize(response.json())
        ip_df = pd.concat([ip_df, issue_data], ignore_index = True)
        
# Get the timezone object for New York
tz_LA = pytz.timezone('US/Pacific') 

# Get the current time in New York
datetime_LA = datetime.now(tz_LA)

# Format the time as a string and print it
print("LA time:", datetime_LA.strftime("%m/%d/%Y %H:%M:%S"))

ip_df["Runtime"] = "LA time: "+datetime_LA.strftime("%m/%d/%Y %H:%M:%S")

# Drop unneeded columns
ip_df.drop(columns = ['url', 'repository_url', 'labels_url', 'comments_url', 'events_url', 'id',
 'node_id', 'number', 'state', 'locked', 'assignee', 'assignees', 'comments', 'created_at',
 'updated_at', 'closed_at', 'author_association', 'active_lock_reason', 'body', 'closed_by',
 'timeline_url', 'performed_via_github_app', 'state_reason', 'user.login', 'user.id',
 'user.node_id', 'user.avatar_url', 'user.gravatar_id', 'user.url', 'user.html_url', 'user.followers_url',
 'user.following_url', 'user.gists_url', 'user.starred_url', 'user.subscriptions_url',
 'user.organizations_url', 'user.repos_url', 'user.events_url', 'user.received_events_url',
 'user.type', 'user.site_admin', 'milestone.url', 'milestone.html_url', 'milestone.labels_url',
 'milestone.id', 'milestone.node_id', 'milestone.number', 'milestone.title', 'milestone.description',
 'milestone.creator.login', 'milestone.creator.id', 'milestone.creator.node_id', 'milestone.creator.avatar_url',
 'milestone.creator.gravatar_id', 'milestone.creator.url', 'milestone.creator.html_url',
 'milestone.creator.followers_url', 'milestone.creator.following_url', 'milestone.creator.gists_url', 'milestone.creator.starred_url',
 'milestone.creator.subscriptions_url', 'milestone.creator.organizations_url', 'milestone.creator.repos_url',
 'milestone.creator.events_url', 'milestone.creator.received_events_url',
 'milestone.creator.type', 'milestone.creator.site_admin', 'milestone.open_issues', 'milestone.closed_issues',
 'milestone.state', 'milestone.created_at', 'milestone.updated_at', 'milestone.due_on',
 'milestone.closed_at', 'reactions.url', 'reactions.total_count', 'reactions.+1', 'reactions.-1', 'reactions.laugh',
 'reactions.hooray', 'reactions.confused', 'reactions.heart', 'reactions.rocket', 'reactions.eyes',
 'assignee.login', 'assignee.id', 'assignee.node_id', 'assignee.avatar_url', 'assignee.gravatar_id', 'assignee.url',
 'assignee.html_url', 'assignee.followers_url', 'assignee.following_url', 'assignee.gists_url',
 'assignee.starred_url', 'assignee.subscriptions_url', 'assignee.organizations_url',
 'assignee.repos_url', 'assignee.events_url', 'assignee.received_events_url', 'assignee.type',
 'assignee.site_admin', 'closed_by.login',
 'closed_by.id', 'closed_by.node_id', 'closed_by.avatar_url', 'closed_by.gravatar_id', 'closed_by.url',
 'closed_by.html_url', 'closed_by.followers_url', 'closed_by.following_url', 'closed_by.gists_url',
 'closed_by.starred_url', 'closed_by.subscriptions_url', 'closed_by.organizations_url', 'closed_by.repos_url',
 'closed_by.events_url', 'closed_by.received_events_url', 'closed_by.type', 'closed_by.site_admin'], inplace = True)

# Flatten labels column

flatten_ip = ip_df.to_json(orient = "records")
parsed_ip = json.loads(flatten_ip)
ip_df2 = pd.json_normalize(parsed_ip, record_path = ["labels"], record_prefix = "labels.", meta = ["Runtime", "html_url", "title"])

ip_df2.drop(columns = ['labels.id', 'labels.node_id', 'labels.url', 'labels.description',
       'labels.color', 'labels.default'], inplace = True)

# Remove issues with ignore labels in In Progress column
if len([label for label in ip_df2["labels.name"].unique() if re.search('ignore', label.lower())])>0:
    remove = list(ip_df2[ip_df2["labels.name"].str.contains("gnore")]["html_url"])
    ip_df2 = ip_df2[~ip_df2["html_url"].isin(remove)]
else:
    remove = []
    
# Finishing touches for In Progress dataset (include issues with no labels)
ip_difference = list(set(ip_df["html_url"]).difference(set(ip_df2["html_url"])))
ip_no_labels = list(set(ip_difference).difference(set(remove)))
ip_no_labels_df = ip_df[ip_df["html_url"].isin(ip_no_labels)][["Runtime", "html_url", "title"]]
ip_no_labels_df["labels.name"] = ""
ip_no_labels_df = ip_no_labels_df.iloc[:, [3,0,1,2]]

# CHANGES MADE HERE!!!!!
ip_issues_df3 = pd.concat([ip_df2, ip_no_labels_df], ignore_index = True)
#############################################################################################

ip_issues_df3["Project Board Column"] = "5 - In Progress"

# retain only labels with "role" in it or complexity labels, and "Draft", "ready for product", "ready for prioritization", "ready for dev lead"
final_ip = ip_df2[(ip_df2["labels.name"].str.contains("role") | ip_df2["labels.name"].isin(complexity_labels) | 
                          ip_df2["labels.name"].isin(extra_breakdown))]

# Make combined label for issues with front and backend labels
ip_wdataset = final_ip[final_ip["labels.name"].str.contains("front end") | final_ip["labels.name"].str.contains("back end")]
ip_wdataset["front/back end count"] = ip_wdataset.groupby(["html_url", "title"])["labels.name"].transform("count")

final_ip.loc[list(ip_wdataset[ip_wdataset["front/back end count"] == 2].index), "labels.name"] = "role: front end and backend/DevOps"

final_ip.drop_duplicates(inplace = True)

final_ip2 = final_ip[["Runtime", "labels.name", "html_url", "title"]]


# In[22]:


### Get Issue Links in "Questions/ In Review" and Get Issue Data

questions_issues = list(questions[~questions['content_url'].isna()]['content_url'])  

questions_issues_df = pd.DataFrame()

try:
    for url in questions_issues:
        response = requests.get(url, auth=(user, GitHub_token))
        issue_data = pd.json_normalize(response.json())
        questions_issues_df = pd.concat([questions_issues_df, issue_data], ignore_index = True)
except ValueError:
    time.sleep(3600)
    for url in questions_issues:
        response = requests.get(url, auth=(user, GitHub_token))
        issue_data = pd.json_normalize(response.json())
        questions_issues_df = pd.concat([questions_issues_df, issue_data], ignore_index = True)
        
# Get the timezone object for New York
tz_LA = pytz.timezone('US/Pacific') 

# Get the current time in New York
datetime_LA = datetime.now(tz_LA)

# Format the time as a string and print it
print("LA time:", datetime_LA.strftime("%m/%d/%Y %H:%M:%S"))

questions_issues_df["Runtime"] = "LA time: "+datetime_LA.strftime("%m/%d/%Y %H:%M:%S")

# Drop unneeded columns
questions_issues_df.drop(columns = ['url', 'repository_url', 'labels_url', 'comments_url', 'events_url', 'id',
 'node_id', 'number', 'state', 'locked', 'assignees', 'comments', 'created_at',
 'updated_at', 'closed_at', 'author_association', 'active_lock_reason', 'body', 'closed_by',
 'timeline_url', 'performed_via_github_app', 'state_reason', 'user.login', 'user.id',
 'user.node_id', 'user.avatar_url', 'user.gravatar_id', 'user.url', 'user.html_url', 'user.followers_url',
 'user.following_url', 'user.gists_url', 'user.starred_url', 'user.subscriptions_url',
 'user.organizations_url', 'user.repos_url', 'user.events_url', 'user.received_events_url',
 'user.type', 'user.site_admin', 'milestone.url', 'milestone.html_url', 'milestone.labels_url',
 'milestone.id', 'milestone.node_id', 'milestone.number', 'milestone.title', 'milestone.description',
 'milestone.creator.login', 'milestone.creator.id', 'milestone.creator.node_id', 'milestone.creator.avatar_url',
 'milestone.creator.gravatar_id', 'milestone.creator.url', 'milestone.creator.html_url',
 'milestone.creator.followers_url', 'milestone.creator.following_url', 'milestone.creator.gists_url', 'milestone.creator.starred_url',
 'milestone.creator.subscriptions_url', 'milestone.creator.organizations_url', 'milestone.creator.repos_url',
 'milestone.creator.events_url', 'milestone.creator.received_events_url',
 'milestone.creator.type', 'milestone.creator.site_admin', 'milestone.open_issues', 'milestone.closed_issues',
 'milestone.state', 'milestone.created_at', 'milestone.updated_at', 'milestone.due_on',
 'milestone.closed_at', 'reactions.url', 'reactions.total_count', 'reactions.+1', 'reactions.-1', 'reactions.laugh',
 'reactions.hooray', 'reactions.confused', 'reactions.heart', 'reactions.rocket', 'reactions.eyes',
 'assignee.login', 'assignee.id', 'assignee.node_id', 'assignee.avatar_url', 'assignee.gravatar_id', 'assignee.url',
 'assignee.html_url', 'assignee.followers_url', 'assignee.following_url', 'assignee.gists_url',
 'assignee.starred_url', 'assignee.subscriptions_url', 'assignee.organizations_url',
 'assignee.repos_url', 'assignee.events_url', 'assignee.received_events_url', 'assignee.type',
 'assignee.site_admin'], inplace = True)

# Flatten labels column

flatten_questions = questions_issues_df.to_json(orient = "records")
parsed_questions= json.loads(flatten_questions)
questions_issues_df2 = pd.json_normalize(parsed_questions, record_path = ["labels"], record_prefix = "labels.", meta = ["Runtime", "html_url", "title"])

questions_issues_df2.drop(columns = ['labels.id', 'labels.node_id', 'labels.url', 'labels.description',
       'labels.color', 'labels.default'], inplace = True)

# Remove issues with ignore labels in Questions/In Review column
if len([label for label in questions_issues_df2["labels.name"].unique() if re.search('ignore', label.lower())])>0:
    remove = list(questions_issues_df2[questions_issues_df2["labels.name"].str.contains("gnore")]["html_url"])
    questions_issues_df2 = questions_issues_df2[~questions_issues_df2["html_url"].isin(remove)]
else:
    remove = []

# Finishing touches for Questions/ In Review dataset (include issues with no labels)
questions_difference = list(set(questions_issues_df["html_url"]).difference(set(questions_issues_df2["html_url"])))
questions_no_labels = list(set(questions_difference).difference(set(remove)))
questions_no_labels_df = questions_issues_df[questions_issues_df["html_url"].isin(questions_no_labels)][["Runtime", "html_url", "title"]]
questions_no_labels_df["labels.name"] = ""
questions_no_labels_df = questions_no_labels_df.iloc[:, [3,0,1,2]]

# CHANGES MADE HERE!!!!!
questions_issues_df3 = pd.concat([questions_issues_df2, questions_no_labels_df], ignore_index = True)
###################################################################################

questions_issues_df3["Project Board Column"] = "6 - Questions/ In Review"

# retain only labels with "role" in it or complexity labels, and "Draft", "ready for product", "ready for prioritization", "ready for dev lead"
final_questions = questions_issues_df2[(questions_issues_df2["labels.name"].str.contains("role") | questions_issues_df2["labels.name"].isin(complexity_labels) 
                                        | questions_issues_df2["labels.name"].isin(extra_breakdown))]

# Make combined label for issues with front and backend labels
questions_wdataset = final_questions[final_questions["labels.name"].str.contains("front end") | final_questions["labels.name"].str.contains("back end")]
questions_wdataset["front/back end count"] = questions_wdataset.groupby(["html_url", "title"])["labels.name"].transform("count")

final_questions.loc[list(questions_wdataset[questions_wdataset["front/back end count"] == 2].index), "labels.name"] = "role: front end and backend/DevOps"

final_questions.drop_duplicates(inplace = True)

final_questions2 = final_questions[["Runtime","labels.name", "html_url", "title"]]


# In[23]:


### Get Issue Links in QA Column and Get Issue Data

QA_issues = list(QA[~QA['content_url'].isna()]['content_url'])  

QA_issues_df = pd.DataFrame()

try:
    for url in QA_issues:
        response = requests.get(url, auth=(user, GitHub_token))
        issue_data = pd.json_normalize(response.json())
        QA_issues_df = pd.concat([QA_issues_df, issue_data], ignore_index = True)
except ValueError:
    time.sleep(3600)
    for url in QA_issues:
        response = requests.get(url, auth=(user, GitHub_token))
        issue_data = pd.json_normalize(response.json())
        QA_issues_df = pd.concat([QA_issues_df, issue_data], ignore_index = True)
        
# Get the timezone object for New York
tz_LA = pytz.timezone('US/Pacific') 

# Get the current time in New York
datetime_LA = datetime.now(tz_LA)

# Format the time as a string and print it
print("LA time:", datetime_LA.strftime("%m/%d/%Y %H:%M:%S"))

QA_issues_df["Runtime"] = "LA time: "+datetime_LA.strftime("%m/%d/%Y %H:%M:%S")

# Drop unneeded columns
QA_issues_df.drop(columns = ['url', 'repository_url', 'labels_url', 'comments_url', 'events_url', 'id',
 'node_id', 'number', 'state', 'locked', 'assignees', 'comments', 'created_at',
 'updated_at', 'closed_at', 'author_association', 'active_lock_reason', 'body', 'closed_by',
 'timeline_url', 'performed_via_github_app', 'state_reason', 'user.login', 'user.id',
 'user.node_id', 'user.avatar_url', 'user.gravatar_id', 'user.url', 'user.html_url', 'user.followers_url',
 'user.following_url', 'user.gists_url', 'user.starred_url', 'user.subscriptions_url',
 'user.organizations_url', 'user.repos_url', 'user.events_url', 'user.received_events_url',
 'user.type', 'user.site_admin', 'milestone.url', 'milestone.html_url', 'milestone.labels_url',
 'milestone.id', 'milestone.node_id', 'milestone.number', 'milestone.title', 'milestone.description',
 'milestone.creator.login', 'milestone.creator.id', 'milestone.creator.node_id', 'milestone.creator.avatar_url',
 'milestone.creator.gravatar_id', 'milestone.creator.url', 'milestone.creator.html_url',
 'milestone.creator.followers_url', 'milestone.creator.following_url', 'milestone.creator.gists_url', 'milestone.creator.starred_url',
 'milestone.creator.subscriptions_url', 'milestone.creator.organizations_url', 'milestone.creator.repos_url',
 'milestone.creator.events_url', 'milestone.creator.received_events_url',
 'milestone.creator.type', 'milestone.creator.site_admin', 'milestone.open_issues', 'milestone.closed_issues',
 'milestone.state', 'milestone.created_at', 'milestone.updated_at', 'milestone.due_on',
 'milestone.closed_at', 'reactions.url', 'reactions.total_count', 'reactions.+1', 'reactions.-1', 'reactions.laugh',
 'reactions.hooray', 'reactions.confused', 'reactions.heart', 'reactions.rocket', 'reactions.eyes',
 'assignee.login', 'assignee.id', 'assignee.node_id', 'assignee.avatar_url', 'assignee.gravatar_id', 'assignee.url',
 'assignee.html_url', 'assignee.followers_url', 'assignee.following_url', 'assignee.gists_url',
 'assignee.starred_url', 'assignee.subscriptions_url', 'assignee.organizations_url',
 'assignee.repos_url', 'assignee.events_url', 'assignee.received_events_url', 'assignee.type',
 'assignee.site_admin'], inplace = True)

# Flatten labels column

flatten_QA = QA_issues_df.to_json(orient = "records")
parsed_QA = json.loads(flatten_QA)
QA_issues_df2 = pd.json_normalize(parsed_QA, record_path = ["labels"], record_prefix = "labels.", meta = ["Runtime", "html_url", "title"])

QA_issues_df2.drop(columns = ['labels.id', 'labels.node_id', 'labels.url', 'labels.description',
       'labels.color', 'labels.default'], inplace = True)

# Remove issues with ignore labels in QA column
if len([label for label in QA_issues_df2["labels.name"].unique() if re.search('ignore', label.lower())])>0:
    remove = list(QA_issues_df2[QA_issues_df2["labels.name"].str.contains("gnore")]["html_url"])
    QA_issues_df2 = QA_issues_df2[~QA_issues_df2["html_url"].isin(remove)]
else:
    remove = []

# Finishing touches for QA dataset (include issues with no labels)
QA_difference = list(set(QA_issues_df["html_url"]).difference(set(QA_issues_df2["html_url"])))
QA_no_labels = list(set(QA_difference).difference(set(remove)))
QA_no_labels_df = QA_issues_df[QA_issues_df["html_url"].isin(QA_no_labels)][["Runtime", "html_url", "title"]]
QA_no_labels_df["labels.name"] = ""
QA_no_labels_df = QA_no_labels_df.iloc[:, [3,0,1,2]]

# CHANGES MADE HERE!!!!!
QA_issues_df3 = pd.concat([QA_issues_df2, QA_no_labels_df], ignore_index = True)
##############################################################################################################

QA_issues_df3["Project Board Column"] = "7 - QA"

# retain only labels with "role" in it or complexity labels, and "Draft", "ready for product", "ready for prioritization", "ready for dev lead"
final_QA = QA_issues_df2[(QA_issues_df2["labels.name"].str.contains("role") | QA_issues_df2["labels.name"].isin(complexity_labels) | 
                          QA_issues_df2["labels.name"].isin(extra_breakdown))]

# Make combined label for issues with front and backend labels
QA_wdataset = final_QA[final_QA["labels.name"].str.contains("front end") | final_QA["labels.name"].str.contains("back end")]
QA_wdataset["front/back end count"] = QA_wdataset.groupby(["html_url", "title"])["labels.name"].transform("count")

final_QA.loc[list(QA_wdataset[QA_wdataset["front/back end count"] == 2].index), "labels.name"] = "role: front end and backend/DevOps"

final_QA.drop_duplicates(inplace = True)

final_QA2 = final_QA[["Runtime", "labels.name", "html_url", "title"]]


# In[24]:


### Get Issue Links in UAT Column and Get Issue Data

UAT_issues = list(UAT[~UAT['content_url'].isna()]['content_url'])

UAT_issues_df = pd.DataFrame()

try:
    for url in UAT_issues:
        response = requests.get(url, auth=(user, GitHub_token))
        issue_data = pd.json_normalize(response.json())
        UAT_issues_df = pd.concat([UAT_issues_df, issue_data], ignore_index = True)
except ValueError:
    time.sleep(3600)
    for url in UAT_issues:
        response = requests.get(url, auth=(user, GitHub_token))
        issue_data = pd.json_normalize(response.json())
        UAT_issues_df = pd.concat([UAT_issues_df, issue_data], ignore_index = True)
        
# Get the timezone object for New York
tz_LA = pytz.timezone('US/Pacific') 

# Get the current time in New York
datetime_LA = datetime.now(tz_LA)

# Format the time as a string and print it
print("LA time:", datetime_LA.strftime("%m/%d/%Y %H:%M:%S"))

UAT_issues_df["Runtime"] = "LA time: "+ datetime_LA.strftime("%m/%d/%Y %H:%M:%S")

# Drop unneeded columns
UAT_issues_df.drop(columns = ['url', 'repository_url', 'labels_url', 'comments_url', 'events_url', 'id',
 'node_id', 'number', 'state', 'locked', 'assignees', 'comments', 'created_at',
 'updated_at', 'closed_at', 'author_association', 'active_lock_reason', 'body',
 'timeline_url', 'performed_via_github_app', 'state_reason', 'user.login', 'user.id',
 'user.node_id', 'user.avatar_url', 'user.gravatar_id', 'user.url', 'user.html_url', 'user.followers_url',
 'user.following_url', 'user.gists_url', 'user.starred_url', 'user.subscriptions_url',
 'user.organizations_url', 'user.repos_url', 'user.events_url', 'user.received_events_url',
 'user.type', 'user.site_admin', 'milestone.url', 'milestone.html_url', 'milestone.labels_url',
 'milestone.id', 'milestone.node_id', 'milestone.number', 'milestone.title', 'milestone.description',
 'milestone.creator.login', 'milestone.creator.id', 'milestone.creator.node_id', 'milestone.creator.avatar_url',
 'milestone.creator.gravatar_id', 'milestone.creator.url', 'milestone.creator.html_url',
 'milestone.creator.followers_url', 'milestone.creator.following_url', 'milestone.creator.gists_url', 'milestone.creator.starred_url',
 'milestone.creator.subscriptions_url', 'milestone.creator.organizations_url', 'milestone.creator.repos_url',
 'milestone.creator.events_url', 'milestone.creator.received_events_url',
 'milestone.creator.type', 'milestone.creator.site_admin', 'milestone.open_issues', 'milestone.closed_issues',
 'milestone.state', 'milestone.created_at', 'milestone.updated_at', 'milestone.due_on',
 'milestone.closed_at', 'reactions.url', 'reactions.total_count', 'reactions.+1', 'reactions.-1', 'reactions.laugh',
 'reactions.hooray', 'reactions.confused', 'reactions.heart', 'reactions.rocket', 'reactions.eyes',
 'assignee.login', 'assignee.id', 'assignee.node_id', 'assignee.avatar_url', 'assignee.gravatar_id', 'assignee.url',
 'assignee.html_url', 'assignee.followers_url', 'assignee.following_url', 'assignee.gists_url',
 'assignee.starred_url', 'assignee.subscriptions_url', 'assignee.organizations_url',
 'assignee.repos_url', 'assignee.events_url', 'assignee.received_events_url', 'assignee.type',
 'assignee.site_admin'], inplace = True)

# Flatten labels column

flatten_UAT = UAT_issues_df.to_json(orient = "records")
parsed_UAT= json.loads(flatten_UAT)
UAT_issues_df2 = pd.json_normalize(parsed_UAT, record_path = ["labels"], record_prefix = "labels.", meta = ["Runtime", "html_url", "title"])

UAT_issues_df2.drop(columns = ['labels.id', 'labels.node_id', 'labels.url', 'labels.description',
       'labels.color', 'labels.default'], inplace = True)

# Remove issues with ignore labels in UAT column
if len([label for label in UAT_issues_df2["labels.name"].unique() if re.search('ignore', label.lower())])>0:
    remove = list(UAT_issues_df2[UAT_issues_df2["labels.name"].str.contains("gnore")]["html_url"])
    UAT_issues_df2 = UAT_issues_df2[~UAT_issues_df2["html_url"].isin(remove)]
else:
    remove = []

# Finishing touches for UAT dataset (include issues with no labels)
UAT_difference = list(set(UAT_issues_df["html_url"]).difference(set(UAT_issues_df2["html_url"])))
UAT_no_labels = list(set(UAT_difference).difference(set(remove)))
UAT_no_labels_df = UAT_issues_df[UAT_issues_df["html_url"].isin(UAT_no_labels)][["Runtime", "html_url", "title"]]
UAT_no_labels_df["labels.name"] = ""
UAT_no_labels_df = UAT_no_labels_df.iloc[:, [3,0,1,2]]

# CHANGES MADE HERE!!!!!
UAT_issues_df3 = pd.concat([UAT_issues_df2, UAT_no_labels_df], ignore_index = True)
###############################################################################################################

UAT_issues_df3["Project Board Column"] = "8 - UAT"

# retain only labels with "role" in it or complexity labels, and "Draft", "ready for product", "ready for prioritization", "ready for dev lead"
final_UAT = UAT_issues_df2[(UAT_issues_df2["labels.name"].str.contains("role") | UAT_issues_df2["labels.name"].isin(complexity_labels) | 
                          UAT_issues_df2["labels.name"].isin(extra_breakdown))]

# Make combined label for issues with front and backend labels
UAT_wdataset = final_UAT[final_UAT["labels.name"].str.contains("front end") | final_UAT["labels.name"].str.contains("back end")]
UAT_wdataset["front/back end count"] = UAT_wdataset.groupby(["html_url", "title"])["labels.name"].transform("count")

final_UAT.loc[list(UAT_wdataset[UAT_wdataset["front/back end count"] == 2].index), "labels.name"] = "role: front end and backend/DevOps"

final_UAT.drop_duplicates(inplace = True)

final_UAT2 = final_UAT[["Runtime", "labels.name", "html_url", "title"]]


# In[25]:


### Get Issue Links in "QA - senior review" Column and Get Issue Data

QA_review_issues = list(QA_review[~QA_review['content_url'].isna()]['content_url'])  

QA_review_issues_df = pd.DataFrame()

try:
    for url in QA_review_issues:
        response = requests.get(url, auth=(user, GitHub_token))
        issue_data = pd.json_normalize(response.json())
        QA_review_issues_df = pd.concat([QA_review_issues_df, issue_data], ignore_index = True)
except ValueError:
    time.sleep(3600)
    for url in QA_review_issues:
        response = requests.get(url, auth=(user, GitHub_token))
        issue_data = pd.json_normalize(response.json())
        QA_review_issues_df = pd.concat([QA_review_issues_df, issue_data], ignore_index = True)
        
# Get the timezone object for New York
tz_LA = pytz.timezone('US/Pacific') 

# Get the current time in New York
datetime_LA = datetime.now(tz_LA)

# Format the time as a string and print it
print("LA time:", datetime_LA.strftime("%m/%d/%Y %H:%M:%S"))

QA_review_issues_df["Runtime"] = "LA time: "+datetime_LA.strftime("%m/%d/%Y %H:%M:%S")

# Drop unneeded columns
QA_review_issues_df.drop(columns = ['url', 'repository_url', 'labels_url', 'comments_url', 'events_url', 'id',
 'node_id', 'number', 'state', 'locked', 'assignees', 'comments', 'created_at',
 'updated_at', 'closed_at', 'author_association', 'active_lock_reason', 'body', 'closed_by',
 'timeline_url', 'performed_via_github_app', 'state_reason', 'user.login', 'user.id',
 'user.node_id', 'user.avatar_url', 'user.gravatar_id', 'user.url', 'user.html_url', 'user.followers_url',
 'user.following_url', 'user.gists_url', 'user.starred_url', 'user.subscriptions_url',
 'user.organizations_url', 'user.repos_url', 'user.events_url', 'user.received_events_url',
 'user.type', 'user.site_admin', 'milestone.url', 'milestone.html_url', 'milestone.labels_url',
 'milestone.id', 'milestone.node_id', 'milestone.number', 'milestone.title', 'milestone.description',
 'milestone.creator.login', 'milestone.creator.id', 'milestone.creator.node_id', 'milestone.creator.avatar_url',
 'milestone.creator.gravatar_id', 'milestone.creator.url', 'milestone.creator.html_url',
 'milestone.creator.followers_url', 'milestone.creator.following_url', 'milestone.creator.gists_url', 'milestone.creator.starred_url',
 'milestone.creator.subscriptions_url', 'milestone.creator.organizations_url', 'milestone.creator.repos_url',
 'milestone.creator.events_url', 'milestone.creator.received_events_url',
 'milestone.creator.type', 'milestone.creator.site_admin', 'milestone.open_issues', 'milestone.closed_issues',
 'milestone.state', 'milestone.created_at', 'milestone.updated_at', 'milestone.due_on',
 'milestone.closed_at', 'reactions.url', 'reactions.total_count', 'reactions.+1', 'reactions.-1', 'reactions.laugh',
 'reactions.hooray', 'reactions.confused', 'reactions.heart', 'reactions.rocket', 'reactions.eyes',
 'assignee.login', 'assignee.id', 'assignee.node_id', 'assignee.avatar_url', 'assignee.gravatar_id', 'assignee.url',
 'assignee.html_url', 'assignee.followers_url', 'assignee.following_url', 'assignee.gists_url',
 'assignee.starred_url', 'assignee.subscriptions_url', 'assignee.organizations_url',
 'assignee.repos_url', 'assignee.events_url', 'assignee.received_events_url', 'assignee.type',
 'assignee.site_admin'], inplace = True)

# Flatten labels column

flatten_QA_review = QA_review_issues_df.to_json(orient = "records")
parsed_QA_review= json.loads(flatten_QA_review)
QA_review_issues_df2 = pd.json_normalize(parsed_QA_review, record_path = ["labels"], record_prefix = "labels.", meta = ["Runtime", "html_url", "title"])

QA_review_issues_df2.drop(columns = ['labels.id', 'labels.node_id', 'labels.url', 'labels.description',
       'labels.color', 'labels.default'], inplace = True)

# Remove issues with ignore labels in QA Senior Review column
if len([label for label in QA_review_issues_df2["labels.name"].unique() if re.search('ignore', label.lower())])>0:
    remove = list(QA_review_issues_df2[QA_review_issues_df2["labels.name"].str.contains("gnore")]["html_url"])
    QA_review_issues_df2 = QA_review_issues_df2[~QA_review_issues_df2["html_url"].isin(remove)]
else: 
    remove = []
    
# Finishing touches for QA Senior Review dataset (include issues with no labels)
QA_review_difference = list(set(QA_review_issues_df["html_url"]).difference(set(QA_review_issues_df2["html_url"])))
QA_review_no_labels = list(set(QA_review_difference).difference(set(remove)))
QA_review_no_labels_df = QA_review_issues_df[QA_review_issues_df["html_url"].isin(QA_review_no_labels)][["Runtime", "html_url", "title"]]
QA_review_no_labels_df["labels.name"] = ""
QA_review_no_labels_df = QA_review_no_labels_df.iloc[:, [3,0,1,2]]

# CHANGES MADE HERE!!!!!
QA_review_issues_df3 = pd.concat([QA_review_issues_df2, QA_review_no_labels_df], ignore_index = True)
#########################################################################################################################################

QA_review_issues_df3["Project Board Column"] = "9 - QA (senior review)"

# retain only labels with "role" in it or complexity labels, and "Draft", "ready for product", "ready for prioritization", "ready for dev lead"
final_QA_review = QA_review_issues_df2[(QA_review_issues_df2["labels.name"].str.contains("role") | QA_review_issues_df2["labels.name"].isin(complexity_labels) | 
                                        QA_review_issues_df2["labels.name"].isin(extra_breakdown))]

# Make combined label for issues with front and backend labels
QA_review_wdataset = final_QA_review[final_QA_review["labels.name"].str.contains("front end") | final_QA_review["labels.name"].str.contains("back end")]
QA_review_wdataset["front/back end count"] = QA_review_wdataset.groupby(["html_url", "title"])["labels.name"].transform("count")

final_QA_review.loc[list(QA_review_wdataset[QA_review_wdataset["front/back end count"] == 2].index), "labels.name"] = "role: front end and backend/DevOps"

final_QA_review.drop_duplicates(inplace = True)

final_QA_review2 = final_QA_review[["Runtime", "labels.name", "html_url", "title"]]


# ### Create Data Source for Dashboard

# In[26]:


icebox_role = final_icebox2[final_icebox2["labels.name"].str.contains("role")]
icebox_complexity = final_icebox2[final_icebox2["labels.name"].isin(complexity_labels)]

icebox_dataset = icebox_role.merge(icebox_complexity, how = "outer", on = ["html_url", "title"])
icebox_dataset.rename(columns = {"labels.name_x": "Role Label", "labels.name_y": "Complexity Label", "Runtime_x":"Runtime"}, inplace = True)
        
icebox_runtime_nulls_loc = icebox_dataset[icebox_dataset["Runtime"].isna()].index
icebox_dataset.loc[icebox_runtime_nulls_loc, "Runtime"]= icebox_dataset[~icebox_dataset["Runtime"].isna()].iloc[0,0]
icebox_dataset.drop(columns = ["Runtime_y"], inplace = True)

for label in extra_breakdown:
    if len(final_icebox2[final_icebox2["labels.name"]==label]) > 0:
        icebox_label = final_icebox2[final_icebox2["labels.name"]==label][["html_url", "title", "labels.name"]]
        icebox_dataset = icebox_dataset.merge(icebox_label, how = "left", on = ["html_url", "title"])
        icebox_dataset["labels.name"] = icebox_dataset["labels.name"].map(lambda x: 1 if x == label else 0)
        icebox_dataset.rename(columns = {"labels.name": label}, inplace = True)
    elif len(final_icebox2[final_icebox2["labels.name"]==label]) == 0:
        icebox_dataset[label] = 0
        
icebox_dataset["Project Board Column"] = "1 - Icebox"

icebox_dataset2 = icebox_dataset.iloc[:, [10,0,1,4,2,3,5,6,7,8,9]]


# In[27]:


icebox_issueswithstatus = final_icebox2[final_icebox2["labels.name"].isin(extra_breakdown)]["html_url"].unique()
icebox_dataset2["Unknown Status 1"] = icebox_dataset2["html_url"].map(lambda x: 0 if x in icebox_issueswithstatus else 1) # issues that do not have label in extra_breakdown


# In[28]:


# Add in static link for Unknown Status 1

complexity_link = {"good first issue": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22good+first+issue%22",
                   "Complexity: Small": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+small%22", 
                   "Complexity: Medium":"https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+medium%22",
                   "Complexity: Large": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+large%22",
                   "Complexity: Extra Large": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+extra+large%22",
                   "Complexity: Missing": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+missing%22",
                   "Complexity: Prework": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+prework%22",
                   "Complexity: See issue making label": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+see+issue+making+label%22",
                  "prework": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22prework%22",
                  "Complexity: Good second issue": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+good+second+issue%22"}


# In[29]:


icebox_dataset2["General Link for Unknown Status 1"] = icebox_dataset2["Complexity Label"].map(lambda x: complexity_link[x]) # issues that do not have label in extra_breakdown


# In[30]:


nesteddict_unknownstatus1 = {"role: front end": {"good first issue": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22good+first+issue%22+label%3A%22role%3A+front+end%22+-label%3A%22role%3A+back+end%2Fdevops%22",
                                                 "Complexity: Small": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+small%22+label%3A%22role%3A+front+end%22+-label%3A%22role%3A+back+end%2Fdevops%22",
                                                 "Complexity: Medium": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+medium%22+label%3A%22role%3A+front+end%22+-label%3A%22role%3A+back+end%2Fdevops%22",
                                                 "Complexity: Large": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+large%22+label%3A%22role%3A+front+end%22+-label%3A%22role%3A+back+end%2Fdevops%22",
                                                 "Complexity: Extra Large": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+extra+large%22+label%3A%22role%3A+front+end%22+-label%3A%22role%3A+back+end%2Fdevops%22",
                                                 "Complexity: Missing": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+missing%22+label%3A%22role%3A+front+end%22+-label%3A%22role%3A+back+end%2Fdevops%22",
                                                 "Complexity: Prework": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+prework%22+label%3A%22role%3A+front+end%22+-label%3A%22role%3A+back+end%2Fdevops%22",
                                                 "Complexity: See issue making label": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+see+issue+making+label%22+label%3A%22role%3A+front+end%22+-label%3A%22role%3A+back+end%2Fdevops%22",
                                                "prework": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22prework%22+label%3A%22role%3A+front+end%22+-label%3A%22role%3A+back+end%2Fdevops%22",
                                                "Complexity: Good second issue": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+good+second+issue%22+label%3A%22role%3A+front+end%22+-label%3A%22role%3A+back+end%2Fdevops%22"},
                             "role: back end/devOps": {"good first issue": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22good+first+issue%22+-label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                       "Complexity: Small": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+small%22+-label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                       "Complexity: Medium": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+medium%22+-label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                       "Complexity: Large": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+large%22+-label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                       "Complexity: Extra Large": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+extra+large%22+-label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                       "Complexity: Missing": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+missing%22+-label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                       "Complexity: Prework": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+prework%22+-label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                       "Complexity: See issue making label": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+see+issue+making+label%22+-label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                      "prework": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22prework%22+-label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                      "Complexity: Good second issue": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+good+second+issue%22+-label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22"},
                             "role: front end and backend/DevOps": {"good first issue": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22good+first+issue%22+label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                                    "Complexity: Small": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+small%22+label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                                    "Complexity: Medium": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+medium%22+label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                                    "Complexity: Large": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+large%22+label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                                    "Complexity: Extra Large": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+extra+large%22+label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                                    "Complexity: Missing": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+missing%22+label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                                    "Complexity: Prework": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+prework%22+label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                                    "Complexity: See issue making label": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+see+issue+making+label%22+label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                                   "prework": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22prework%22+label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                                   "Complexity: Good second issue": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+good+second+issue%22+label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22"},
                             "role: dev leads": {"good first issue": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22good+first+issue%22+label%3A%22role%3A+dev+leads%22",
                                                 "Complexity: Small": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+small%22+label%3A%22role%3A+dev+leads%22",
                                                 "Complexity: Medium": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+medium%22+label%3A%22role%3A+dev+leads%22",
                                                 "Complexity: Large": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+large%22+label%3A%22role%3A+dev+leads%22",
                                                 "Complexity: Extra Large": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+extra+large%22+label%3A%22role%3A+dev+leads%22",
                                                 "Complexity: Missing": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+missing%22+label%3A%22role%3A+dev+leads%22",
                                                 "Complexity: Prework": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+prework%22+label%3A%22role%3A+dev+leads%22",
                                                 "Complexity: See issue making label": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+see+issue+making+label%22+label%3A%22role%3A+dev+leads%22",
                                                "prework": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22prework%22+label%3A%22role%3A+dev+leads%22",
                                                "Complexity: Good second issue": "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%222+weeks+inactive%22+-label%3A%22ready+for+product%22+-label%3A%22draft%22+-label%3A%22ready+for+prioritization%22+-label%3A%22ready+for+dev+lead%22+label%3A%22complexity%3A+good+second+issue%22+label%3A%22role%3A+dev+leads%22"}
                            }               


# In[31]:


icebox_dataset2["Role-based Link for Unknown Status 1"] = ""
for role in nesteddict_unknownstatus1.keys():
    df = icebox_dataset2[icebox_dataset2["Role Label"] == role]
    for complexity in nesteddict_unknownstatus1["role: front end"].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        icebox_dataset2.loc[indexes, "Role-based Link for Unknown Status 1"] = nesteddict_unknownstatus1[role][complexity]


# In[32]:


icebox_issues_withstatus_B = final_icebox2[final_icebox2["labels.name"].isin(["role: user research", "role: product", "Ready for Prioritization", "Draft", "ready for dev lead"])]["html_url"].unique()
icebox_dataset2["Unknown Status 2"] = icebox_dataset2["html_url"].map(lambda x: 0 if x in icebox_issues_withstatus_B else 1)


# In[33]:


complexity_link_B = {"good first issue": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22good+first+issue%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22",
                   "Complexity: Small": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+small%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22", 
                   "Complexity: Medium":"https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+medium%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22",
                   "Complexity: Large": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+large%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22",
                   "Complexity: Extra Large": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+extra+large%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22",
                   "Complexity: Missing": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+missing%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22",
                   "Complexity: Prework": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+prework%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22",
                   "Complexity: See issue making label": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+see+issue+making+label%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22",
                    "prework": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22prework%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22",
                    "Complexity: Good second issue": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+good+second+issue%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22"}


# In[34]:


icebox_dataset2["General Link for Unknown Status 2"] = icebox_dataset2["Complexity Label"].map(lambda x: complexity_link_B[x]) 


# In[36]:


nesteddict_unknownstatus2 = {"role: front end": {"good first issue": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22good+first+issue%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+front+end%22+-label%3A%22role%3A+back+end%2Fdevops%22",
                                                 "Complexity: Small": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+small%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+front+end%22+-label%3A%22role%3A+back+end%2Fdevops%22",
                                                 "Complexity: Medium": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+medium%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+front+end%22+-label%3A%22role%3A+back+end%2Fdevops%22",
                                                 "Complexity: Large": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+large%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+front+end%22+-label%3A%22role%3A+back+end%2Fdevops%22",
                                                 "Complexity: Extra Large": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+extra+large%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+front+end%22+-label%3A%22role%3A+back+end%2Fdevops%22",
                                                 "Complexity: Missing": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+missing%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+front+end%22+-label%3A%22role%3A+back+end%2Fdevops%22",
                                                 "Complexity: Prework": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+prework%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+front+end%22+-label%3A%22role%3A+back+end%2Fdevops%22",
                                                 "Complexity: See issue making label": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+see+issue+making+label%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+front+end%22+-label%3A%22role%3A+back+end%2Fdevops%22",
                                                "prework": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22prework%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+front+end%22+-label%3A%22role%3A+back+end%2Fdevops%22",
                                                "Complexity: Good second issue": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+good+second+issue%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+front+end%22+-label%3A%22role%3A+back+end%2Fdevops%22"},
                             "role: back end/devOps": {"good first issue": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22good+first+issue%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+-label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                       "Complexity: Small": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+small%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+-label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                       "Complexity: Medium": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+medium%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+-label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                       "Complexity: Large": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+large%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+-label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                       "Complexity: Extra Large": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+extra+large%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+-label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                       "Complexity: Missing": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+missing%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+-label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                       "Complexity: Prework": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+prework%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+-label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                       "Complexity: See issue making label": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+see+issue+making+label%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+-label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                      "prework": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22prework%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+-label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                      "Complexity: Good second issue": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+good+second+issue%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+-label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22"},
                             "role: front end and backend/DevOps": {"good first issue": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22good+first+issue%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                                    "Complexity: Small": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+small%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                                    "Complexity: Medium": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+medium%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                                    "Complexity: Large": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+large%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                                    "Complexity: Extra Large": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+extra+large%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                                    "Complexity: Missing": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+missing%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                                    "Complexity: Prework": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+prework%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                                    "Complexity: See issue making label": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22prework%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                                   "prework": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22good+first+issue%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22",
                                                                   "Complexity: Good second issue": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+good+second+issue%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+front+end%22+label%3A%22role%3A+back+end%2Fdevops%22"},
                             "role: dev leads": {"good first issue": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22good+first+issue%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+dev+leads%22",
                                                 "Complexity: Small": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+small%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+dev+leads%22",
                                                 "Complexity: Medium": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+medium%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+dev+leads%22",
                                                 "Complexity: Large": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+large%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+dev+leads%22",
                                                 "Complexity: Extra Large": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+extra+large%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+dev+leads%22",
                                                 "Complexity: Missing": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+missing%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+dev+leads%22",
                                                 "Complexity: Prework": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+prework%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+dev+leads%22",
                                                 "Complexity: See issue making label": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+see+issue+making+label%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+dev+leads%22",
                                                "prework": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22prework%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+dev+leads%22",
                                                "Complexity: Good second issue": "https://github.com/hackforla/website/projects/7?card_filter_query=label%3A%22complexity%3A+good+second+issue%22+-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+label%3A%22role%3A+dev+leads%22"}
                            }               


# In[37]:


icebox_dataset2["Role-based Link for Unknown Status 2"] = ""
for role in nesteddict_unknownstatus2.keys():
    df = icebox_dataset2[icebox_dataset2["Role Label"] == role]
    for complexity in nesteddict_unknownstatus2["role: front end"].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        icebox_dataset2.loc[indexes, "Role-based Link for Unknown Status 2"] = nesteddict_unknownstatus2[role][complexity]


# In[38]:


# Emergent Request

ER_role = final_ER2[final_ER2["labels.name"].str.contains("role")]
ER_complexity = final_ER2[final_ER2["labels.name"].isin(complexity_labels)]

ER_dataset = ER_role.merge(ER_complexity, how = "outer", on = ["html_url", "title"])
ER_dataset.rename(columns = {"labels.name_x": "Role Label", "labels.name_y": "Complexity Label", "Runtime_x":"Runtime"}, inplace = True)

ER_runtime_nulls_loc = ER_dataset[ER_dataset["Runtime"].isna()].index
ER_dataset.loc[ER_runtime_nulls_loc, "Runtime"]= ER_dataset[~ER_dataset["Runtime"].isna()].iloc[0,0]
ER_dataset.drop(columns = ["Runtime_y"], inplace = True)

for label in extra_breakdown:
    if len(final_ER2[final_ER2["labels.name"]==label]) > 0:
        ER_label = final_ER2[final_ER2["labels.name"]==label][["html_url", "title", "labels.name"]]
        ER_dataset = ER_dataset.merge(ER_label, how = "left", on = ["html_url", "title"])
        ER_dataset["labels.name"] = ER_dataset["labels.name"].map(lambda x: 1 if x == label else 0)
        ER_dataset.rename(columns = {"labels.name": label}, inplace = True)
    elif len(final_ER2[final_ER2["labels.name"]==label]) == 0:
        ER_dataset[label] = 0
        
ER_dataset["Project Board Column"] = "2 - ER"

# reoder the columns
ER_dataset2 = ER_dataset.iloc[:, [10,0,1,4,2,3,5,6,7,8,9]]

ER_issueswithstatus = final_ER2[final_ER2["labels.name"].isin(extra_breakdown)]["html_url"].unique()
ER_dataset2["Unknown Status 1"] = ER_dataset2["html_url"].map(lambda x: 0 if x in ER_issueswithstatus else 1) # issues that do not have label in extra_breakdown

ER_dataset2["General Link for Unknown Status 1"] = ER_dataset2["Complexity Label"].map(lambda x: complexity_link[x]) # issues that do not have label in extra_breakdown

ER_dataset2["Role-based Link for Unknown Status 1"] = ""
for role in nesteddict_unknownstatus1.keys():
    df = ER_dataset2[ER_dataset2["Role Label"] == role]
    for complexity in nesteddict_unknownstatus1["role: front end"].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        ER_dataset2.loc[indexes, "Role-based Link for Unknown Status 1"] = nesteddict_unknownstatus1[role][complexity]
        
ER_issues_withstatus_B = final_ER2[final_ER2["labels.name"].isin(["role: user research", "role: product", "Ready for Prioritization", "Draft", "ready for dev lead"])]["html_url"].unique()
ER_dataset2["Unknown Status 2"] = ER_dataset2["html_url"].map(lambda x: 0 if x in ER_issues_withstatus_B else 1)

ER_dataset2["General Link for Unknown Status 2"] = ER_dataset2["Complexity Label"].map(lambda x: complexity_link_B[x]) 

ER_dataset2["Role-based Link for Unknown Status 2"] = ""
for role in nesteddict_unknownstatus2.keys():
    df = ER_dataset2[ER_dataset2["Role Label"] == role]
    for complexity in nesteddict_unknownstatus2["role: front end"].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        ER_dataset2.loc[indexes, "Role-based Link for Unknown Status 2"] = nesteddict_unknownstatus2[role][complexity]


# In[39]:


# New Issue Approval
    
NIA_role = final_NIA2[final_NIA2["labels.name"].str.contains("role")]
NIA_complexity = final_NIA2[final_NIA2["labels.name"].isin(complexity_labels)]

# Join the datasets for data source
NIA_dataset = NIA_role.merge(NIA_complexity, how = "outer", on = ["html_url", "title"])
NIA_dataset.rename(columns = {"labels.name_x": "Role Label", "labels.name_y": "Complexity Label", "Runtime_x":"Runtime"}, inplace = True)

NIA_runtime_nulls_loc = NIA_dataset[NIA_dataset["Runtime"].isna()].index
NIA_dataset.loc[NIA_runtime_nulls_loc, "Runtime"]= NIA_dataset[~NIA_dataset["Runtime"].isna()].iloc[0,0]
NIA_dataset.drop(columns = ["Runtime_y"], inplace = True)

for label in extra_breakdown:
    if len(final_NIA2[final_NIA2["labels.name"]==label]) > 0:
        NIA_label = final_NIA2[final_NIA2["labels.name"]==label][["html_url", "title", "labels.name"]]
        NIA_dataset = NIA_dataset.merge(NIA_label, how = "left", on = ["html_url", "title"])
        NIA_dataset["labels.name"] = NIA_dataset["labels.name"].map(lambda x: 1 if x == label else 0)
        NIA_dataset.rename(columns = {"labels.name": label}, inplace = True)
    elif len(final_NIA2[final_NIA2["labels.name"]==label]) == 0:
        NIA_dataset[label] = 0

NIA_dataset["Project Board Column"] = "3 - New Issue Approval"

NIA_dataset2 = NIA_dataset.iloc[:, [10,0,1,4,2,3,5,6,7,8,9]]

# Add in unknown status columns

NIA_issueswithstatus = final_NIA2[final_NIA2["labels.name"].isin(extra_breakdown)]["html_url"].unique()
NIA_dataset2["Unknown Status 1"] = NIA_dataset2["html_url"].map(lambda x: 0 if x in NIA_issueswithstatus else 1) # issues that do not have label in extra_breakdown

NIA_dataset2["General Link for Unknown Status 1"] = NIA_dataset2["Complexity Label"].map(lambda x: complexity_link[x]) # issues that do not have label in extra_breakdown

NIA_dataset2["Role-based Link for Unknown Status 1"] = ""
for role in nesteddict_unknownstatus1.keys():
    df = NIA_dataset2[NIA_dataset2["Role Label"] == role]
    for complexity in nesteddict_unknownstatus1["role: front end"].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        NIA_dataset2.loc[indexes, "Role-based Link for Unknown Status 1"] = nesteddict_unknownstatus1[role][complexity]
        
NIA_issues_withstatus_B = final_NIA2[final_NIA2["labels.name"].isin(["role: user research", "role: product", "Ready for Prioritization", "Draft", "ready for dev lead"])]["html_url"].unique()
NIA_dataset2["Unknown Status 2"] = NIA_dataset2["html_url"].map(lambda x: 0 if x in NIA_issues_withstatus_B else 1)

NIA_dataset2["General Link for Unknown Status 2"] = NIA_dataset2["Complexity Label"].map(lambda x: complexity_link_B[x]) 

NIA_dataset2["Role-based Link for Unknown Status 2"] = ""
for role in nesteddict_unknownstatus2.keys():
    df = NIA_dataset2[NIA_dataset2["Role Label"] == role]
    for complexity in nesteddict_unknownstatus2["role: front end"].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        NIA_dataset2.loc[indexes, "Role-based Link for Unknown Status 2"] = nesteddict_unknownstatus2[role][complexity]


# In[40]:


# Prioritized Backlog

pb_role = final_pb2[final_pb2["labels.name"].str.contains("role")]
pb_complexity = final_pb2[final_pb2["labels.name"].isin(complexity_labels)]

pb_dataset = pb_role.merge(pb_complexity, how = "outer", on = ["html_url", "title"])
pb_dataset.rename(columns = {"labels.name_x": "Role Label", "labels.name_y": "Complexity Label", "Runtime_x":"Runtime"}, inplace = True)

pb_runtime_nulls_loc = pb_dataset[pb_dataset["Runtime"].isna()].index
pb_dataset.loc[pb_runtime_nulls_loc, "Runtime"]= pb_dataset[~pb_dataset["Runtime"].isna()].iloc[0,0]
pb_dataset.drop(columns = ["Runtime_y"], inplace = True)

for label in extra_breakdown:
    if len(final_pb2[final_pb2["labels.name"]==label]) > 0:
        pb_label = final_pb2[final_pb2["labels.name"]==label][["html_url", "title", "labels.name"]]
        pb_dataset = pb_dataset.merge(pb_label, how = "left", on = ["html_url", "title"])
        pb_dataset["labels.name"] = pb_dataset["labels.name"].map(lambda x: 1 if x == label else 0)
        pb_dataset.rename(columns = {"labels.name": label}, inplace = True)
    elif len(final_pb2[final_pb2["labels.name"]==label]) == 0:
        pb_dataset[label] = 0
        
pb_dataset["Project Board Column"] = "4 - Prioritized Backlog"

pb_dataset2 = pb_dataset.iloc[:, [10,0,1,4,2,3,5,6,7,8,9]]

# Add in unknown status columns

pb_issueswithstatus = final_pb2[final_pb2["labels.name"].isin(extra_breakdown)]["html_url"].unique()
pb_dataset2["Unknown Status 1"] = pb_dataset2["html_url"].map(lambda x: 0 if x in pb_issueswithstatus else 1) # issues that do not have label in extra_breakdown

pb_dataset2["General Link for Unknown Status 1"] = pb_dataset2["Complexity Label"].map(lambda x: complexity_link[x]) # issues that do not have label in extra_breakdown

pb_dataset2["Role-based Link for Unknown Status 1"] = ""
for role in nesteddict_unknownstatus1.keys():
    df = pb_dataset2[pb_dataset2["Role Label"] == role]
    for complexity in nesteddict_unknownstatus1["role: front end"].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        pb_dataset2.loc[indexes, "Role-based Link for Unknown Status 1"] = nesteddict_unknownstatus1[role][complexity]
        
pb_issues_withstatus_B = final_pb2[final_pb2["labels.name"].isin(["role: user research", "role: product", "Ready for Prioritization", "Draft", "ready for dev lead"])]["html_url"].unique()
pb_dataset2["Unknown Status 2"] = pb_dataset2["html_url"].map(lambda x: 0 if x in pb_issues_withstatus_B else 1)

pb_dataset2["General Link for Unknown Status 2"] = pb_dataset2["Complexity Label"].map(lambda x: complexity_link_B[x]) 

pb_dataset2["Role-based Link for Unknown Status 2"] = ""
for role in nesteddict_unknownstatus2.keys():
    df = pb_dataset2[pb_dataset2["Role Label"] == role]
    for complexity in nesteddict_unknownstatus2["role: front end"].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        pb_dataset2.loc[indexes, "Role-based Link for Unknown Status 2"] = nesteddict_unknownstatus2[role][complexity]


# In[41]:


# In Progress (actively working)

IP_role = final_ip2[final_ip2["labels.name"].str.contains("role")]
IP_complexity = final_ip2[final_ip2["labels.name"].isin(complexity_labels)]

IP_dataset = IP_role.merge(IP_complexity, how = "outer", on = ["html_url", "title"])
IP_dataset.rename(columns = {"labels.name_x": "Role Label", "labels.name_y": "Complexity Label", "Runtime_x":"Runtime"}, inplace = True)

IP_runtime_nulls_loc = IP_dataset[IP_dataset["Runtime"].isna()].index
IP_dataset.loc[IP_runtime_nulls_loc, "Runtime"]= IP_dataset[~IP_dataset["Runtime"].isna()].iloc[0,0]
IP_dataset.drop(columns = ["Runtime_y"], inplace = True)

for label in extra_breakdown:
    if len(final_ip2[final_ip2["labels.name"]==label]) > 0:
        IP_label = final_ip2[final_ip2["labels.name"]==label][["html_url", "title", "labels.name"]]
        IP_dataset = IP_dataset.merge(IP_label, how = "left", on = ["html_url", "title"])
        IP_dataset["labels.name"] = IP_dataset["labels.name"].map(lambda x: 1 if x == label else 0)
        IP_dataset.rename(columns = {"labels.name": label}, inplace = True)
    elif len(final_ip2[final_ip2["labels.name"]==label]) == 0:
        IP_dataset[label] = 0

IP_dataset["Project Board Column"] = "5 - In Progress"

# reoder the columns
IP_dataset2 = IP_dataset.iloc[:, [10,0,1,4,2,3,5,6,7,8,9]]

# Add in unknown status columns

IP_issueswithstatus = final_ip2[final_ip2["labels.name"].isin(extra_breakdown)]["html_url"].unique()
IP_dataset2["Unknown Status 1"] = IP_dataset2["html_url"].map(lambda x: 0 if x in IP_issueswithstatus else 1) # issues that do not have label in extra_breakdown

IP_dataset2["General Link for Unknown Status 1"] = IP_dataset2["Complexity Label"].map(lambda x: complexity_link[x]) # issues that do not have label in extra_breakdown

IP_dataset2["Role-based Link for Unknown Status 1"] = ""
for role in nesteddict_unknownstatus1.keys():
    df = IP_dataset2[IP_dataset2["Role Label"] == role]
    for complexity in nesteddict_unknownstatus1["role: front end"].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        IP_dataset2.loc[indexes, "Role-based Link for Unknown Status 1"] = nesteddict_unknownstatus1[role][complexity]
        
IP_issues_withstatus_B = final_ip2[final_ip2["labels.name"].isin(["role: user research", "role: product", "Ready for Prioritization", "Draft", "ready for dev lead"])]["html_url"].unique()
IP_dataset2["Unknown Status 2"] = IP_dataset2["html_url"].map(lambda x: 0 if x in IP_issues_withstatus_B else 1)

IP_dataset2["General Link for Unknown Status 2"] = IP_dataset2["Complexity Label"].map(lambda x: complexity_link_B[x]) 

IP_dataset2["Role-based Link for Unknown Status 2"] = ""
for role in nesteddict_unknownstatus2.keys():
    df = IP_dataset2[IP_dataset2["Role Label"] == role]
    for complexity in nesteddict_unknownstatus2["role: front end"].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        IP_dataset2.loc[indexes, "Role-based Link for Unknown Status 2"] = nesteddict_unknownstatus2[role][complexity]


# In[42]:


# Questions / In Review

Q_role = final_questions2[final_questions2["labels.name"].str.contains("role")]
Q_complexity = final_questions2[final_questions2["labels.name"].isin(complexity_labels)]

Q_dataset = Q_role.merge(Q_complexity, how = "outer", on = ["html_url", "title"])
Q_dataset.rename(columns = {"labels.name_x": "Role Label", "labels.name_y": "Complexity Label", "Runtime_x":"Runtime"}, inplace = True)

Q_runtime_nulls_loc = Q_dataset[Q_dataset["Runtime"].isna()].index
Q_dataset.loc[Q_runtime_nulls_loc, "Runtime"]= Q_dataset[~Q_dataset["Runtime"].isna()].iloc[0,0]
Q_dataset.drop(columns = ["Runtime_y"], inplace = True)

for label in extra_breakdown:
    if len(final_questions2[final_questions2["labels.name"]==label]) > 0:
        Q_label = final_questions2[final_questions2["labels.name"]==label][["html_url", "title", "labels.name"]]
        Q_dataset = Q_dataset.merge(Q_label, how = "left", on = ["html_url", "title"])
        Q_dataset["labels.name"] = Q_dataset["labels.name"].map(lambda x: 1 if x == label else 0)
        Q_dataset.rename(columns = {"labels.name": label}, inplace = True)
    elif len(final_questions2[final_questions2["labels.name"]==label]) == 0:
        Q_dataset[label] = 0
        
Q_dataset["Project Board Column"] = "6 - Questions/ In Review"

# reoder the columns
Q_dataset2 = Q_dataset.iloc[:, [10,0,1,4,2,3,5,6,7,8,9]]

# Add in unknown status columns

Q_issueswithstatus = final_questions2[final_questions2["labels.name"].isin(extra_breakdown)]["html_url"].unique()
Q_dataset2["Unknown Status 1"] = Q_dataset2["html_url"].map(lambda x: 0 if x in Q_issueswithstatus else 1) # issues that do not have label in extra_breakdown

Q_dataset2["General Link for Unknown Status 1"] = Q_dataset2["Complexity Label"].map(lambda x: complexity_link[x]) # issues that do not have label in extra_breakdown

Q_dataset2["Role-based Link for Unknown Status 1"] = ""
for role in nesteddict_unknownstatus1.keys():
    df = Q_dataset2[Q_dataset2["Role Label"] == role]
    for complexity in nesteddict_unknownstatus1["role: front end"].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        Q_dataset2.loc[indexes, "Role-based Link for Unknown Status 1"] = nesteddict_unknownstatus1[role][complexity]
        
Q_issues_withstatus_B = final_questions2[final_questions2["labels.name"].isin(["role: user research", "role: product", "Ready for Prioritization", "Draft", "ready for dev lead"])]["html_url"].unique()
Q_dataset2["Unknown Status 2"] = Q_dataset2["html_url"].map(lambda x: 0 if x in Q_issues_withstatus_B else 1)

Q_dataset2["General Link for Unknown Status 2"] = Q_dataset2["Complexity Label"].map(lambda x: complexity_link_B[x]) 

Q_dataset2["Role-based Link for Unknown Status 2"] = ""
for role in nesteddict_unknownstatus2.keys():
    df = Q_dataset2[Q_dataset2["Role Label"] == role]
    for complexity in nesteddict_unknownstatus2["role: front end"].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        Q_dataset2.loc[indexes, "Role-based Link for Unknown Status 2"] = nesteddict_unknownstatus2[role][complexity]


# In[43]:


# QA

QA_role = final_QA2[final_QA2["labels.name"].str.contains("role")]
QA_complexity = final_QA2[final_QA2["labels.name"].isin(complexity_labels)]

QA_dataset = QA_role.merge(QA_complexity, how = "outer", on = ["html_url", "title"])
QA_dataset.rename(columns = {"labels.name_x": "Role Label", "labels.name_y": "Complexity Label", "Runtime_x":"Runtime"}, inplace = True)

QA_runtime_nulls_loc = QA_dataset[QA_dataset["Runtime"].isna()].index
QA_dataset.loc[QA_runtime_nulls_loc, "Runtime"]= QA_dataset[~QA_dataset["Runtime"].isna()].iloc[0,0]
QA_dataset.drop(columns = ["Runtime_y"], inplace = True)

for label in extra_breakdown:
    if len(final_QA2[final_QA2["labels.name"]==label]) > 0:
        QA_label = final_QA2[final_QA2["labels.name"]==label][["html_url", "title", "labels.name"]]
        QA_dataset = QA_dataset.merge(QA_label, how = "left", on = ["html_url", "title"])
        QA_dataset["labels.name"] = QA_dataset["labels.name"].map(lambda x: 1 if x == label else 0)
        QA_dataset.rename(columns = {"labels.name": label}, inplace = True)
    elif len(final_QA2[final_QA2["labels.name"]==label]) == 0:
        QA_dataset[label] = 0
        
QA_dataset["Project Board Column"] = "7 - QA"

# reoder the columns
QA_dataset2 = QA_dataset.iloc[:, [10,0,1,4,2,3,5,6,7,8,9]]

# Add in unknown status columns

QA_issueswithstatus = final_QA2[final_QA2["labels.name"].isin(extra_breakdown)]["html_url"].unique()
QA_dataset2["Unknown Status 1"] = QA_dataset2["html_url"].map(lambda x: 0 if x in QA_issueswithstatus else 1) # issues that do not have label in extra_breakdown

QA_dataset2["General Link for Unknown Status 1"] = QA_dataset2["Complexity Label"].map(lambda x: complexity_link[x]) # issues that do not have label in extra_breakdown

QA_dataset2["Role-based Link for Unknown Status 1"] = ""
for role in nesteddict_unknownstatus1.keys():
    df = QA_dataset2[QA_dataset2["Role Label"] == role]
    for complexity in nesteddict_unknownstatus1["role: front end"].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        QA_dataset2.loc[indexes, "Role-based Link for Unknown Status 1"] = nesteddict_unknownstatus1[role][complexity]
        
QA_issues_withstatus_B = final_QA2[final_QA2["labels.name"].isin(["role: user research", "role: product", "Ready for Prioritization", "Draft", "ready for dev lead"])]["html_url"].unique()
QA_dataset2["Unknown Status 2"] = QA_dataset2["html_url"].map(lambda x: 0 if x in QA_issues_withstatus_B else 1)

QA_dataset2["General Link for Unknown Status 2"] = QA_dataset2["Complexity Label"].map(lambda x: complexity_link_B[x]) 

QA_dataset2["Role-based Link for Unknown Status 2"] = ""
for role in nesteddict_unknownstatus2.keys():
    df = QA_dataset2[QA_dataset2["Role Label"] == role]
    for complexity in nesteddict_unknownstatus2["role: front end"].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        QA_dataset2.loc[indexes, "Role-based Link for Unknown Status 2"] = nesteddict_unknownstatus2[role][complexity]


# In[44]:


# UAT

UAT_role = final_UAT2[final_UAT2["labels.name"].str.contains("role")]
UAT_complexity = final_UAT2[final_UAT2["labels.name"].isin(complexity_labels)]

UAT_dataset = UAT_role.merge(UAT_complexity, how = "outer", on = ["html_url", "title"])
UAT_dataset.rename(columns = {"labels.name_x": "Role Label", "labels.name_y": "Complexity Label", "Runtime_x":"Runtime"}, inplace = True)

UAT_runtime_nulls_loc = UAT_dataset[UAT_dataset["Runtime"].isna()].index
UAT_dataset.loc[UAT_runtime_nulls_loc, "Runtime"]= UAT_dataset[~UAT_dataset["Runtime"].isna()].iloc[0,0]
UAT_dataset.drop(columns = ["Runtime_y"], inplace = True)

for label in extra_breakdown:
    if len(final_UAT2[final_UAT2["labels.name"]==label]) > 0:
        UAT_label = final_UAT2[final_UAT2["labels.name"]==label][["html_url", "title", "labels.name"]]
        UAT_dataset = UAT_dataset.merge(UAT_label, how = "left", on = ["html_url", "title"])
        UAT_dataset["labels.name"] = UAT_dataset["labels.name"].map(lambda x: 1 if x == label else 0)
        UAT_dataset.rename(columns = {"labels.name": label}, inplace = True)
    elif len(final_UAT2[final_UAT2["labels.name"]==label]) == 0:
        UAT_dataset[label] = 0

UAT_dataset["Project Board Column"] = "8 - UAT"

UAT_dataset2 = UAT_dataset.iloc[:, [10,0,1,4,2,3,5,6,7,8,9]]

# Add in unknown status columns

UAT_issueswithstatus = final_UAT2[final_UAT2["labels.name"].isin(extra_breakdown)]["html_url"].unique()
UAT_dataset2["Unknown Status 1"] = UAT_dataset2["html_url"].map(lambda x: 0 if x in UAT_issueswithstatus else 1) # issues that do not have label in extra_breakdown

UAT_dataset2["General Link for Unknown Status 1"] = UAT_dataset2["Complexity Label"].map(lambda x: complexity_link[x]) # issues that do not have label in extra_breakdown

UAT_dataset2["Role-based Link for Unknown Status 1"] = ""
for role in nesteddict_unknownstatus1.keys():
    df = UAT_dataset2[UAT_dataset2["Role Label"] == role]
    for complexity in nesteddict_unknownstatus1["role: front end"].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        UAT_dataset2.loc[indexes, "Role-based Link for Unknown Status 1"] = nesteddict_unknownstatus1[role][complexity]
        
UAT_issues_withstatus_B = final_UAT2[final_UAT2["labels.name"].isin(["role: user research", "role: product", "Ready for Prioritization", "Draft", "ready for dev lead"])]["html_url"].unique()
UAT_dataset2["Unknown Status 2"] = UAT_dataset2["html_url"].map(lambda x: 0 if x in UAT_issues_withstatus_B else 1)

UAT_dataset2["General Link for Unknown Status 2"] = UAT_dataset2["Complexity Label"].map(lambda x: complexity_link_B[x]) 

UAT_dataset2["Role-based Link for Unknown Status 2"] = ""
for role in nesteddict_unknownstatus2.keys():
    df = UAT_dataset2[UAT_dataset2["Role Label"] == role]
    for complexity in nesteddict_unknownstatus2["role: front end"].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        UAT_dataset2.loc[indexes, "Role-based Link for Unknown Status 2"] = nesteddict_unknownstatus2[role][complexity]


# In[45]:


# QA - senior review

QA_review_role = final_QA_review2[final_QA_review2["labels.name"].str.contains("role")]
QA_review_complexity = final_QA_review2[final_QA_review2["labels.name"].isin(complexity_labels)]

QA_review_dataset = QA_review_role.merge(QA_review_complexity, how = "outer", on = ["html_url", "title"])
QA_review_dataset.rename(columns = {"labels.name_x": "Role Label", "labels.name_y": "Complexity Label", "Runtime_x":"Runtime"}, inplace = True)

QA_review_runtime_nulls_loc = QA_review_dataset[QA_review_dataset["Runtime"].isna()].index
QA_review_dataset.loc[QA_review_runtime_nulls_loc, "Runtime"]= QA_review_dataset[~QA_review_dataset["Runtime"].isna()].iloc[0,0]
QA_review_dataset.drop(columns = ["Runtime_y"], inplace = True)

for label in extra_breakdown:
    if len(final_QA_review2[final_QA_review2["labels.name"]==label]) > 0:
        QA_review_label = final_QA_review2[final_QA_review2["labels.name"]==label][["html_url", "title", "labels.name"]]
        QA_review_dataset = QA_review_dataset.merge(QA_review_label, how = "left", on = ["html_url", "title"])
        QA_review_dataset["labels.name"] = QA_review_dataset["labels.name"].map(lambda x: 1 if x == label else 0)
        QA_review_dataset.rename(columns = {"labels.name": label}, inplace = True)
    elif len(final_QA_review2[final_QA_review2["labels.name"]==label]) == 0:
        QA_review_dataset[label] = 0

QA_review_dataset["Project Board Column"] = "9 - QA (senior review)"

QA_review_dataset2 = QA_review_dataset.iloc[:, [10,0,1,4,2,3,5,6,7,8,9]]

# Add in unknown status columns

QA_review_issueswithstatus = final_QA_review2[final_QA_review2["labels.name"].isin(extra_breakdown)]["html_url"].unique()
QA_review_dataset2["Unknown Status 1"] = QA_review_dataset2["html_url"].map(lambda x: 0 if x in QA_review_issueswithstatus else 1) # issues that do not have label in extra_breakdown

QA_review_dataset2["General Link for Unknown Status 1"] = QA_review_dataset2["Complexity Label"].map(lambda x: complexity_link[x]) # issues that do not have label in extra_breakdown

QA_review_dataset2["Role-based Link for Unknown Status 1"] = ""
for role in nesteddict_unknownstatus1.keys():
    df = QA_review_dataset2[QA_review_dataset2["Role Label"] == role]
    for complexity in nesteddict_unknownstatus1["role: front end"].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        QA_review_dataset2.loc[indexes, "Role-based Link for Unknown Status 1"] = nesteddict_unknownstatus1[role][complexity]
        
QA_review_issues_withstatus_B = final_QA_review2[final_QA_review2["labels.name"].isin(["role: user research", "role: product", "Ready for Prioritization", "Draft", "ready for dev lead"])]["html_url"].unique()
QA_review_dataset2["Unknown Status 2"] = QA_review_dataset2["html_url"].map(lambda x: 0 if x in QA_review_issues_withstatus_B else 1)

QA_review_dataset2["General Link for Unknown Status 2"] = QA_review_dataset2["Complexity Label"].map(lambda x: complexity_link_B[x]) 

QA_review_dataset2["Role-based Link for Unknown Status 2"] = ""
for role in nesteddict_unknownstatus2.keys():
    df = QA_review_dataset2[QA_review_dataset2["Role Label"] == role]
    for complexity in nesteddict_unknownstatus2["role: front end"].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        QA_review_dataset2.loc[indexes, "Role-based Link for Unknown Status 2"] = nesteddict_unknownstatus2[role][complexity]


# ### Combine Data from All Project Board Columns

# In[46]:


# Concat the dataset and see whether dashboard would work
final_dataset = pd.concat([icebox_dataset2, ER_dataset2, NIA_dataset2, pb_dataset2, IP_dataset2, Q_dataset2, QA_dataset2, UAT_dataset2, QA_review_dataset2], ignore_index = True)

final_dataset.loc[final_dataset[final_dataset["Complexity Label"] == "good first issue"].index, "Complexity Label"] = "1 - good first issue"
final_dataset.loc[final_dataset[final_dataset["Complexity Label"] == "Complexity: Small"].index, "Complexity Label"] = "2 - Complexity: Small"
final_dataset.loc[final_dataset[final_dataset["Complexity Label"] == "Complexity: Medium"].index, "Complexity Label"] = "3 - Complexity: Medium"
final_dataset.loc[final_dataset[final_dataset["Complexity Label"] == "Complexity: Large"].index, "Complexity Label"] = "4 - Complexity: Large"
final_dataset.loc[final_dataset[final_dataset["Complexity Label"] == "Complexity: Extra Large"].index, "Complexity Label"] = "5 - Complexity: Extra Large"


# ### Create Anomaly Detection Dataset

# In[47]:


# Concat the dataframes from all columns

anomaly_detection = pd.concat([icebox_issues_df3, ER_issues_df3, NIA_issues_df3, pb_issues_df3, ip_issues_df3, questions_issues_df3, QA_issues_df3, UAT_issues_df3, QA_review_issues_df3], ignore_index = True)
anomaly_detection["keep"] = anomaly_detection["labels.name"].map(lambda x: 1 if (re.search(r"(size|feature|role|complexity|good first issue|prework|^$)", str(x).lower())) else 0)
anomaly_detection_df = anomaly_detection[anomaly_detection["keep"] == 1]
anomaly_detection_df.drop(columns = ["keep"], inplace = True)


# ### Send Data to Google Sheets

# In[ ]:


from google.oauth2 import service_account
from gspread_dataframe import set_with_dataframe
from googleapiclient.discovery import build
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

import gspread
import base64

scopes = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

## Read in offical GitHub labels from Google spreadsheet for weekly label check table

key_base64 = os.environ["BASE64_PROJECT_BOARD_GOOGLECREDENTIAL"]
base64_bytes = key_base64.encode('ascii')
key_base64_bytes = base64.b64decode(base64_bytes)
key_content = key_base64_bytes.decode('ascii')

service_account_info = json.loads(key_content)

credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes = scopes)

service_sheets = build('sheets', 'v4', credentials = credentials)
print(service_sheets)

gc = gspread.authorize(credentials)

gauth = GoogleAuth()
drive = GoogleDrive(gauth)

from gspread_dataframe import set_with_dataframe

LabelCheck_GOOGLE_SHEETS_ID = '1-ltg0qMeZSgOnqrCU0nKUDQd1JOXTMWrNTK63VZjXdk'

LabelCheck_sheet_name = 'Official GitHub Labels'

gs = gc.open_by_key(LabelCheck_GOOGLE_SHEETS_ID)

LabelCheck_worksheet = gs.worksheet(LabelCheck_sheet_name)

LC_spreadsheet_data = LabelCheck_worksheet.get_all_records()
LC_df = pd.DataFrame.from_dict(LC_spreadsheet_data)
official_labels = list(LC_df["label_name"].unique())

outdated_labels = list(LC_df[LC_df["in_use?"] == "No"]["label_name"].unique())

anomaly_detection_df["labels_need_action"] = anomaly_detection_df["labels.name"].map(lambda x: 1 if (x not in official_labels or x in outdated_labels) else 0)
anomaly_detection_df = anomaly_detection_df.iloc[:, [4,1,2,3,0,5]]

anomaly_detection_df2_base = anomaly_detection_df.copy()
anomaly_detection_df2_base.drop(columns = ["labels_need_action"], inplace = True)
anomaly_detection_df2_base["Complexity Label"] = anomaly_detection_df2_base["labels.name"].map(lambda x: 1 if (re.search(r"(complexity|good first issue|prework)", str(x).lower())) else 0)
anomaly_detection_df2_base["Feature Label"] = anomaly_detection_df2_base["labels.name"].map(lambda x: 1 if (re.search("feature", str(x).lower())) else 0)
anomaly_detection_df2_base["Role Label"] = anomaly_detection_df2_base["labels.name"].map(lambda x: 1 if (re.search("role", str(x).lower())) else 0)
anomaly_detection_df2_base["Size Label"]= anomaly_detection_df2_base["labels.name"].map(lambda x: 1 if (re.search("size", str(x).lower())) else 0)

anomaly_detection_df2_base["Complexity Missing Label"] = anomaly_detection_df2_base["labels.name"].map(lambda x: 1 if x == "Complexity: Missing" else 0)
anomaly_detection_df2_base["Feature Missing Label"] = anomaly_detection_df2_base["labels.name"].map(lambda x: 1 if x == "Feature Missing" else 0)
anomaly_detection_df2_base["Role Missing Label"] = anomaly_detection_df2_base["labels.name"].map(lambda x: 1 if x == "role missing" else 0)
anomaly_detection_df2_base["Size Missing Label"]= anomaly_detection_df2_base["labels.name"].map(lambda x: 1 if x == "size: missing" else 0)

anomaly_detection_df2 = anomaly_detection_df2_base.groupby(["Project Board Column", "Runtime", "html_url", "title"])[["Complexity Label", "Feature Label", "Role Label", "Size Label", "Complexity Missing Label", "Feature Missing Label", "Role Missing Label", "Size Missing Label"]].sum().reset_index()

anomaly_detection_df2["Complexity defined label"] = anomaly_detection_df2["Complexity Label"]-anomaly_detection_df2["Complexity Missing Label"]
anomaly_detection_df2["Feature defined label"] = anomaly_detection_df2["Feature Label"]-anomaly_detection_df2["Feature Missing Label"]
anomaly_detection_df2["Role defined label"] = anomaly_detection_df2["Role Label"]-anomaly_detection_df2["Role Missing Label"]
anomaly_detection_df2["Size defined label"] = anomaly_detection_df2["Size Label"]-anomaly_detection_df2["Size Missing Label"]

anomaly_detection_df2_join = anomaly_detection_df2_base[anomaly_detection_df2_base["Role Label"] == 1][["html_url", "labels.name"]]
anomaly_detection_df2 = anomaly_detection_df2.merge(anomaly_detection_df2_join, how = "left", on = ["html_url"])

missing_dependency = anomaly_detection[(anomaly_detection["labels.name"] == "dependency missing") & (anomaly_detection["Project Board Column"] == "1 - Icebox")]

missing_dependency = missing_dependency.iloc[:, [1,4,2,3,0]]

if len(missing_dependency) == 0:
    missing_dependency.loc[0] = [" "," "," "," "," "]
else:
    missing_dependency

### Send to Google Sheet

Main_GOOGLE_SHEETS_ID = '1aJ0yHkXYMWTtMz6eEeolTLmAQOBc2DyptmR5SAmUrjM'

sheet_name1 = 'Dataset 2'

gs = gc.open_by_key(Main_GOOGLE_SHEETS_ID)

worksheet1 = gs.worksheet(sheet_name1)

worksheet1.clear()


# Insert dataframe of issues into Google Sheet

set_with_dataframe(worksheet = worksheet1, dataframe = final_dataset, include_index = False, include_column_header = True, resize = True)

sheet_name2 = 'Labels to note'
worksheet2 = gs.worksheet(sheet_name2)
worksheet2.clear()
set_with_dataframe(worksheet = worksheet2, dataframe = anomaly_detection_df, include_index = False, include_column_header = True, resize = True)

sheet_name3 = 'Missing Labels'
worksheet3 = gs.worksheet(sheet_name3)
worksheet3.clear()
set_with_dataframe(worksheet = worksheet3, dataframe = anomaly_detection_df2, include_index = False, include_column_header = True, resize = True)

sheet_name4 = 'Missing Dependency Issues'
worksheet4 = gs.worksheet(sheet_name4)
worksheet4.clear()
set_with_dataframe(worksheet = worksheet4, dataframe = missing_dependency, include_index = False, include_column_header = True, resize = True)


# In[ ]:




