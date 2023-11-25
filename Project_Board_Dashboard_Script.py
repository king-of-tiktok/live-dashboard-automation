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

# Format the time as a string and add it in Runtime column
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
icebox_no_labels_df = icebox_no_labels_df[["labels.name", "Runtime", "html_url", "title"]]

icebox_issues_df3 = pd.concat([icebox_issues_df2, icebox_no_labels_df], ignore_index = True)

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
                          icebox_issues_df2["labels.name"].isin(extra_breakdown) | icebox_issues_df2["labels.name"].str.contains("Ready", case=False))]

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

# Format the time as a string and add to Runtime column
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
ER_no_labels_df = ER_no_labels_df[["labels.name", "Runtime", "html_url", "title"]]

ER_issues_df3 = pd.concat([ER_issues_df2, ER_no_labels_df], ignore_index = True)

ER_issues_df3["Project Board Column"] = "2- ER"

# retain only labels with "role" in it or complexity labels, and "Draft", "ready for product", "ready for prioritization", "ready for dev lead"
final_ER = ER_issues_df2[(ER_issues_df2["labels.name"].str.contains("role") | ER_issues_df2["labels.name"].isin(complexity_labels) | 
                          ER_issues_df2["labels.name"].isin(extra_breakdown) | ER_issues_df2["labels.name"].str.contains("Ready", case=False))]

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

# Format the time as a string and add to Runtime column
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
NIA_no_labels_df = NIA_no_labels_df[["labels.name", "Runtime", "html_url", "title"]]

NIA_issues_df3 = pd.concat([NIA_issues_df2, NIA_no_labels_df], ignore_index = True)

NIA_issues_df3["Project Board Column"] = "3 - New Issue Approval"
len(NIA_issues_df3)

# retain only labels with "role" in it or complexity labels, and "Draft", "ready for product", "ready for prioritization", "ready for dev lead"
final_NIA = NIA_issues_df2[(NIA_issues_df2["labels.name"].str.contains("role") | NIA_issues_df2["labels.name"].isin(complexity_labels) | 
                          NIA_issues_df2["labels.name"].isin(extra_breakdown) | NIA_issues_df2["labels.name"].str.contains("Ready", case=False))]

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

# Format the time as a string and add to Runtime column
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
pb_no_labels_df = pb_no_labels_df[["labels.name", "Runtime", "html_url", "title"]]

pb_issues_df3 = pd.concat([pb_issues_df2, pb_no_labels_df], ignore_index = False)

pb_issues_df3["Project Board Column"] = "4 - Prioritized Backlog"

# retain only labels with "role" in it, complexity labels, and "Draft", "ready for product", "ready for prioritization", "ready for dev lead"

final_pb = pb_issues_df2[(pb_issues_df2["labels.name"].str.contains("role") | pb_issues_df2["labels.name"].isin(complexity_labels) | 
                          pb_issues_df2["labels.name"].isin(extra_breakdown) | pb_issues_df2["labels.name"].str.contains("Ready", case=False))]

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

# Format the time as a string and add it to Runtime column
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
ip_no_labels_df = ip_no_labels_df[["labels.name", "Runtime", "html_url", "title"]]

ip_issues_df3 = pd.concat([ip_df2, ip_no_labels_df], ignore_index = True)

ip_issues_df3["Project Board Column"] = "5 - In Progress"

# retain only labels with "role" in it or complexity labels, and "Draft", "ready for product", "ready for prioritization", "ready for dev lead"
final_ip = ip_df2[(ip_df2["labels.name"].str.contains("role") | ip_df2["labels.name"].isin(complexity_labels) | 
                          ip_df2["labels.name"].isin(extra_breakdown) | ip_df2["labels.name"].str.contains("Ready", case=False))]

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

# Format the time as a string and add to Runtime column
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
questions_no_labels_df = questions_no_labels_df[["labels.name", "Runtime", "html_url", "title"]]


questions_issues_df3 = pd.concat([questions_issues_df2, questions_no_labels_df], ignore_index = True)

questions_issues_df3["Project Board Column"] = "6 - Questions/ In Review"

# retain only labels with "role" in it or complexity labels, and "Draft", "ready for product", "ready for prioritization", "ready for dev lead"
final_questions = questions_issues_df2[(questions_issues_df2["labels.name"].str.contains("role") | questions_issues_df2["labels.name"].isin(complexity_labels) 
                                        | questions_issues_df2["labels.name"].isin(extra_breakdown) | questions_issues_df2["labels.name"].str.contains("Ready", case=False))]

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

# Format the time as a string and add it to Runtime column
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
QA_no_labels_df = QA_no_labels_df[["labels.name", "Runtime", "html_url", "title"]]

QA_issues_df3 = pd.concat([QA_issues_df2, QA_no_labels_df], ignore_index = True)

QA_issues_df3["Project Board Column"] = "7 - QA"

# retain only labels with "role" in it or complexity labels, and "Draft", "ready for product", "ready for prioritization", "ready for dev lead"
final_QA = QA_issues_df2[(QA_issues_df2["labels.name"].str.contains("role") | QA_issues_df2["labels.name"].isin(complexity_labels) | 
                          QA_issues_df2["labels.name"].isin(extra_breakdown) | QA_issues_df2["labels.name"].str.contains("Ready", case=False))]

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

# Format the time as a string and add it to Runtime column
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
UAT_no_labels_df = UAT_no_labels_df[["labels.name", "Runtime", "html_url", "title"]]

UAT_issues_df3 = pd.concat([UAT_issues_df2, UAT_no_labels_df], ignore_index = True)

UAT_issues_df3["Project Board Column"] = "8 - UAT"

# retain only labels with "role" in it or complexity labels, and "Draft", "ready for product", "ready for prioritization", "ready for dev lead"
final_UAT = UAT_issues_df2[(UAT_issues_df2["labels.name"].str.contains("role") | UAT_issues_df2["labels.name"].isin(complexity_labels) | 
                          UAT_issues_df2["labels.name"].isin(extra_breakdown) | UAT_issues_df2["labels.name"].str.contains("Ready", case=False))]

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

# Format the time as a string and add it to Runtime column
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
QA_review_no_labels_df = QA_review_no_labels_df[["labels.name", "Runtime", "html_url", "title"]]

QA_review_issues_df3 = pd.concat([QA_review_issues_df2, QA_review_no_labels_df], ignore_index = True)

QA_review_issues_df3["Project Board Column"] = "9 - QA (senior review)"

# retain only labels with "role" in it or complexity labels, and "Draft", "ready for product", "ready for prioritization", "ready for dev lead"
final_QA_review = QA_review_issues_df2[(QA_review_issues_df2["labels.name"].str.contains("role") | QA_review_issues_df2["labels.name"].isin(complexity_labels) | 
                                        QA_review_issues_df2["labels.name"].isin(extra_breakdown) | QA_review_issues_df2["labels.name"].str.contains("Ready", case=False))]

# Make combined label for issues with front and backend labels
QA_review_wdataset = final_QA_review[final_QA_review["labels.name"].str.contains("front end") | final_QA_review["labels.name"].str.contains("back end")]
QA_review_wdataset["front/back end count"] = QA_review_wdataset.groupby(["html_url", "title"])["labels.name"].transform("count")

final_QA_review.loc[list(QA_review_wdataset[QA_review_wdataset["front/back end count"] == 2].index), "labels.name"] = "role: front end and backend/DevOps"

final_QA_review.drop_duplicates(inplace = True)

final_QA_review2 = final_QA_review[["Runtime", "labels.name", "html_url", "title"]]


# ### Create Data Source for Dashboard

# In[48]:

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

icebox_dataset2 = icebox_dataset[["Project Board Column", "Runtime", "Role Label", "Complexity Label", "html_url", "title", "Draft", "2 weeks inactive", "ready for product", "ready for dev lead", "Ready for Prioritization"]]


# In[27]:


# Create a column to identify issues with unknown status
icebox_unknown_status_wdataset = icebox_issues_df2.copy()
icebox_unknown_status_wdataset["Known Status"] = icebox_unknown_status_wdataset["labels.name"].map(lambda x: 1 if (re.search(r"(ready|draft|^dependency$)", str(x).lower())) else 0)
icebox_known_status_issues = list(icebox_unknown_status_wdataset[icebox_unknown_status_wdataset["Known Status"] == 1]["html_url"].unique())
icebox_dataset2["Unknown Status"] = icebox_dataset2["html_url"].map(lambda x: 0 if x in icebox_known_status_issues else 1) 

# In[28]:


# Create nested dictionary for static links

icebox_unique_roles = [x for x in icebox_dataset2["Role Label"].unique() if pd.isna(x) == False]
icebox_unique_roles2 = [x for x in icebox_unique_roles if x != "role: front end and backend/DevOps"]
icebox_unique_complexity = [x for x in icebox_dataset2["Complexity Label"].unique() if pd.isna(x) == False]  
static_link_base_icebox = 'https://github.com/hackforla/website/projects/7?card_filter_query=-label%3Adraft%22+-label%3A%22dependency%22'

# Transform all ready series labels and add them to the status link
ready_labels = list(LC_df[LC_df["label_series"] == "ready"]["label_name"].unique())
ready_labels_append = ""
for label in ready_labels:
    ready_labels_transformed = label.lower().replace(":", "%3A").replace(" ", "+")
    ready_labels_append = ready_labels_append+"+-label%3A%22"+ ready_labels_transformed+"%22"

# Transform all ignore series labels and add them to the status link
ignore_labels = list(LC_df[LC_df["label_series"] == "ignore"]["label_name"].unique())
ignore_labels_append = ""
for label in ignore_labels:
    ignore_labels_transformed = label.lower().replace(":", "%3A").replace(" ", "+")
    ignore_labels_append = ignore_labels_append+"+-label%3A%22"+ ignore_labels_transformed+"%22"

static_link_base_icebox = static_link_base_icebox + ready_labels_append + ignore_labels_append

# In[29]:

icebox_link_dict = { }

for role in icebox_unique_roles:
    icebox_link_dict[role] = {}
    for complexity in icebox_unique_complexity:
        role_transformed = role.lower().replace(":", "%3A").replace(" ", "+")
        complexity_transformed = complexity.lower().replace(":", "%3A").replace(" ", "+")
        icebox_link_dict[role][complexity] = static_link_base_icebox+"+label%3A%22"+role_transformed+"%22"+"+label%3A%22"+complexity_transformed+"%22"

icebox_link_dict["role: front end and backend/DevOps"] = {}     
for complexity in icebox_unique_complexity:
    frontend_transformed = "role: front end".lower().replace(":", "%3A").replace(" ", "+")
    backend_transformed = "role: back end/devOps".lower().replace(":", "%3A").replace(" ", "+")
    complexity_transformed = complexity.lower().replace(":", "%3A").replace(" ", "+")
    icebox_link_dict["role: front end and backend/DevOps"][complexity] = static_link_base_icebox+"+label%3A%22"+frontend_transformed+"%22"+"+label%3A%22"+backend_transformed+"%22"+"+label%3A%22"+complexity_transformed+"%22"


# In[30]:


icebox_dataset2["Role-based Link for Unknown Status"] = ""
for role in icebox_link_dict.keys():
    df = icebox_dataset2[icebox_dataset2["Role Label"] == role]
    for complexity in icebox_link_dict[list(icebox_link_dict.keys())[0]].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        icebox_dataset2.loc[indexes, "Role-based Link for Unknown Status"] = icebox_link_dict[role][complexity]


# In[31]:


# icebox_dataset2["General Link for Developer Unknown Status"] = "" 


# In[32]:
# Base link for unknown status for all other columns
static_link_base = 'https://github.com/hackforla/website/projects/7?card_filter_query=-label%3Adraft' + ready_labels_append + ignore_labels_append

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
ER_dataset2 = ER_dataset[["Project Board Column", "Runtime", "Role Label", "Complexity Label", "html_url", "title", "Draft", "2 weeks inactive", "ready for product", "ready for dev lead", "Ready for Prioritization"]]


# In[33]:


# Create a column to identify issues with unknown status
ER_unknown_status_wdataset = ER_issues_df2.copy()
ER_unknown_status_wdataset["Known Status"] = ER_unknown_status_wdataset["labels.name"].map(lambda x: 1 if (re.search(r"(ready|draft)", str(x).lower())) else 0)
ER_known_status_issues = list(ER_unknown_status_wdataset[ER_unknown_status_wdataset["Known Status"] == 1]["html_url"].unique())
ER_dataset2["Unknown Status"] = ER_dataset2["html_url"].map(lambda x: 0 if x in ER_known_status_issues else 1) 

# In[34]:


# Create nested dictionary for static links

ER_unique_roles = [x for x in ER_dataset2["Role Label"].unique() if pd.isna(x) == False]
ER_unique_roles2 = [x for x in ER_unique_roles if x != "role: front end and backend/DevOps"]
ER_unique_complexity = [x for x in ER_dataset2["Complexity Label"].unique() if pd.isna(x) == False]

# In[35]:


ER_link_dict = { }

for role in ER_unique_roles:
    ER_link_dict[role] = {}
    for complexity in ER_unique_complexity:
        role_transformed = role.lower().replace(":", "%3A").replace(" ", "+")
        complexity_transformed = complexity.lower().replace(":", "%3A").replace(" ", "+")
        ER_link_dict[role][complexity] = static_link_base+"+label%3A%22"+role_transformed+"%22"+"+label%3A%22"+complexity_transformed+"%22"

ER_link_dict["role: front end and backend/DevOps"] = {}     
for complexity in ER_unique_complexity:
    frontend_transformed = "role: front end".lower().replace(":", "%3A").replace(" ", "+")
    backend_transformed = "role: back end/devOps".lower().replace(":", "%3A").replace(" ", "+")
    complexity_transformed = complexity.lower().replace(":", "%3A").replace(" ", "+")
    ER_link_dict["role: front end and backend/DevOps"][complexity] = static_link_base+"+label%3A%22"+frontend_transformed+"%22"+"+label%3A%22"+backend_transformed+"%22"+"+label%3A%22"+complexity_transformed+"%22"


# In[36]:


ER_dataset2["Role-based Link for Unknown Status"] = ""
for role in ER_link_dict.keys():
    df = ER_dataset2[ER_dataset2["Role Label"] == role]
    for complexity in ER_link_dict[list(ER_link_dict.keys())[0]].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        ER_dataset2.loc[indexes, "Role-based Link for Unknown Status"] = ER_link_dict[role][complexity]


# In[37]:


# ER_dataset2["General Link for Developer Unknown Status"] = "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+-label%3A%22ready+for+product%22+-label%3A%22role%3A+design%22+-label%3A%22role%3A+research+lead%22+-label%3A%22role%3A+writing%22+-label%3A%22ready+for+design+lead%22+-label%3A%22ready+for+org+rep%22+-label%3A%22role%3A+data+analyst%22" 


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

NIA_dataset2 = NIA_dataset[["Project Board Column", "Runtime", "Role Label", "Complexity Label", "html_url", "title", "Draft", "2 weeks inactive", "ready for product", "ready for dev lead", "Ready for Prioritization"]]

# Create a column to identify issues with unknown status
NIA_unknown_status_wdataset = NIA_issues_df2.copy()
NIA_unknown_status_wdataset["Known Status"] = NIA_unknown_status_wdataset["labels.name"].map(lambda x: 1 if (re.search(r"(ready|draft)", str(x).lower())) else 0)
NIA_known_status_issues = list(NIA_unknown_status_wdataset[NIA_unknown_status_wdataset["Known Status"] == 1]["html_url"].unique())
NIA_dataset2["Unknown Status"] = NIA_dataset2["html_url"].map(lambda x: 0 if x in NIA_known_status_issues else 1) 

# Create nested dictionary for static links

NIA_unique_roles = [x for x in NIA_dataset2["Role Label"].unique() if pd.isna(x) == False]
NIA_unique_roles2 = [x for x in NIA_unique_roles if x != "role: front end and backend/DevOps"]
NIA_unique_complexity = [x for x in NIA_dataset2["Complexity Label"].unique() if pd.isna(x) == False]

NIA_link_dict = { }

for role in NIA_unique_roles:
    NIA_link_dict[role] = {}
    for complexity in NIA_unique_complexity:
        role_transformed = role.lower().replace(":", "%3A").replace(" ", "+")
        complexity_transformed = complexity.lower().replace(":", "%3A").replace(" ", "+")
        NIA_link_dict[role][complexity] = static_link_base+"+label%3A%22"+role_transformed+"%22"+"+label%3A%22"+complexity_transformed+"%22"

NIA_link_dict["role: front end and backend/DevOps"] = {}     
for complexity in NIA_unique_complexity:
    frontend_transformed = "role: front end".lower().replace(":", "%3A").replace(" ", "+")
    backend_transformed = "role: back end/devOps".lower().replace(":", "%3A").replace(" ", "+")
    complexity_transformed = complexity.lower().replace(":", "%3A").replace(" ", "+")
    NIA_link_dict["role: front end and backend/DevOps"][complexity] = static_link_base+"+label%3A%22"+frontend_transformed+"%22"+"+label%3A%22"+backend_transformed+"%22"+"+label%3A%22"+complexity_transformed+"%22"
    
NIA_dataset2["Role-based Link for Unknown Status"] = ""
for role in NIA_link_dict.keys():
    df = NIA_dataset2[NIA_dataset2["Role Label"] == role]
    for complexity in NIA_link_dict[list(NIA_link_dict.keys())[0]].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        NIA_dataset2.loc[indexes, "Role-based Link for Unknown Status"] = NIA_link_dict[role][complexity]

# NIA_dataset2["General Link for Developer Unknown Status"] = "https://github.com/hackforla/website/projects/7?card_filter_query=-label%3A%22role%3A+user+research%22+-label%3A%22role%3A+product%22+-label%3A%22ready+for+prioritization%22+-label%3Adraft+-label%3A%22ready+for+dev+lead%22+-label%3A%22ready+for+product%22+-label%3A%22role%3A+design%22+-label%3A%22role%3A+research+lead%22+-label%3A%22role%3A+writing%22+-label%3A%22ready+for+design+lead%22+-label%3A%22ready+for+org+rep%22+-label%3A%22role%3A+data+analyst%22" 


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

pb_dataset2 = pb_dataset[["Project Board Column", "Runtime", "Role Label", "Complexity Label", "html_url", "title", "Draft", "2 weeks inactive", "ready for product", "ready for dev lead", "Ready for Prioritization"]]

# Create a column to identify issues with unknown status
pb_unknown_status_wdataset = pb_issues_df2.copy()
pb_unknown_status_wdataset["Known Status"] = pb_unknown_status_wdataset["labels.name"].map(lambda x: 1 if (re.search(r"(ready|draft)", str(x).lower())) else 0)
pb_known_status_issues = list(pb_unknown_status_wdataset[pb_unknown_status_wdataset["Known Status"] == 1]["html_url"].unique())
pb_dataset2["Unknown Status"] = pb_dataset2["html_url"].map(lambda x: 0 if x in pb_known_status_issues else 1) 

# Create nested dictionary for static links

pb_unique_roles = [x for x in pb_dataset2["Role Label"].unique() if pd.isna(x) == False]
pb_unique_roles2 = [x for x in pb_unique_roles if x != "role: front end and backend/DevOps"]
pb_unique_complexity = [x for x in pb_dataset2["Complexity Label"].unique() if pd.isna(x) == False]

pb_link_dict = { }

for role in pb_unique_roles:
    pb_link_dict[role] = {}
    for complexity in pb_unique_complexity:
        role_transformed = role.lower().replace(":", "%3A").replace(" ", "+")
        complexity_transformed = complexity.lower().replace(":", "%3A").replace(" ", "+")
        pb_link_dict[role][complexity] = static_link_base+"+label%3A%22"+role_transformed+"%22"+"+label%3A%22"+complexity_transformed+"%22"

pb_link_dict["role: front end and backend/DevOps"] = {}     
for complexity in pb_unique_complexity:
    frontend_transformed = "role: front end".lower().replace(":", "%3A").replace(" ", "+")
    backend_transformed = "role: back end/devOps".lower().replace(":", "%3A").replace(" ", "+")
    complexity_transformed = complexity.lower().replace(":", "%3A").replace(" ", "+")
    pb_link_dict["role: front end and backend/DevOps"][complexity] = static_link_base+"+label%3A%22"+frontend_transformed+"%22"+"+label%3A%22"+backend_transformed+"%22"+"+label%3A%22"+complexity_transformed+"%22"
    
pb_dataset2["Role-based Link for Unknown Status"] = ""
for role in pb_link_dict.keys():
    df = pb_dataset2[pb_dataset2["Role Label"] == role]
    for complexity in pb_link_dict[list(pb_link_dict.keys())[0]].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        pb_dataset2.loc[indexes, "Role-based Link for Unknown Status"] = pb_link_dict[role][complexity]

# pb_dataset2["General Link for Developer Unknown Status"] = ""


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
IP_dataset2 = IP_dataset[["Project Board Column", "Runtime", "Role Label", "Complexity Label", "html_url", "title", "Draft", "2 weeks inactive", "ready for product", "ready for dev lead", "Ready for Prioritization"]]

# Create a column to identify issues with unknown status
IP_unknown_status_wdataset = ip_df2.copy()
IP_unknown_status_wdataset["Known Status"] = IP_unknown_status_wdataset["labels.name"].map(lambda x: 1 if (re.search(r"(ready|draft)", str(x).lower())) else 0)
IP_known_status_issues = list(IP_unknown_status_wdataset[IP_unknown_status_wdataset["Known Status"] == 1]["html_url"].unique())
IP_dataset2["Unknown Status"] = IP_dataset2["html_url"].map(lambda x: 0 if x in IP_known_status_issues else 1) 

# Create nested dictionary for static links

IP_unique_roles = [x for x in IP_dataset2["Role Label"].unique() if pd.isna(x) == False]
IP_unique_roles2 = [x for x in IP_unique_roles if x != "role: front end and backend/DevOps"]
IP_unique_complexity = [x for x in IP_dataset2["Complexity Label"].unique() if pd.isna(x) == False]

IP_link_dict = { }

for role in IP_unique_roles:
    IP_link_dict[role] = {}
    for complexity in IP_unique_complexity:
        role_transformed = role.lower().replace(":", "%3A").replace(" ", "+")
        complexity_transformed = complexity.lower().replace(":", "%3A").replace(" ", "+")
        IP_link_dict[role][complexity] = static_link_base+"+label%3A%22"+role_transformed+"%22"+"+label%3A%22"+complexity_transformed+"%22"

IP_link_dict["role: front end and backend/DevOps"] = {}     
for complexity in IP_unique_complexity:
    frontend_transformed = "role: front end".lower().replace(":", "%3A").replace(" ", "+")
    backend_transformed = "role: back end/devOps".lower().replace(":", "%3A").replace(" ", "+")
    complexity_transformed = complexity.lower().replace(":", "%3A").replace(" ", "+")
    IP_link_dict["role: front end and backend/DevOps"][complexity] = static_link_base+"+label%3A%22"+frontend_transformed+"%22"+"+label%3A%22"+backend_transformed+"%22"+"+label%3A%22"+complexity_transformed+"%22"
    
IP_dataset2["Role-based Link for Unknown Status"] = ""
for role in IP_link_dict.keys():
    df = IP_dataset2[IP_dataset2["Role Label"] == role]
    for complexity in IP_link_dict[list(IP_link_dict.keys())[0]].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        IP_dataset2.loc[indexes, "Role-based Link for Unknown Status"] = IP_link_dict[role][complexity]


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
Q_dataset2 = Q_dataset[["Project Board Column", "Runtime", "Role Label", "Complexity Label", "html_url", "title", "Draft", "2 weeks inactive", "ready for product", "ready for dev lead", "Ready for Prioritization"]]

# Add in unknown status columns

# Create a column to identify issues with unknown status
Q_unknown_status_wdataset = questions_issues_df2.copy()
Q_unknown_status_wdataset["Known Status"] = Q_unknown_status_wdataset["labels.name"].map(lambda x: 1 if (re.search(r"(ready|draft)", str(x).lower())) else 0)
Q_known_status_issues = list(Q_unknown_status_wdataset[Q_unknown_status_wdataset["Known Status"] == 1]["html_url"].unique())
Q_dataset2["Unknown Status"] = Q_dataset2["html_url"].map(lambda x: 0 if x in Q_known_status_issues else 1) 

# Create nested dictionary for static links

Q_unique_roles = [x for x in Q_dataset2["Role Label"].unique() if pd.isna(x) == False]
Q_unique_roles2 = [x for x in Q_unique_roles if x != "role: front end and backend/DevOps"]
Q_unique_complexity = [x for x in Q_dataset2["Complexity Label"].unique() if pd.isna(x) == False]

Q_link_dict = { }

for role in Q_unique_roles:
    Q_link_dict[role] = {}
    for complexity in Q_unique_complexity:
        role_transformed = role.lower().replace(":", "%3A").replace(" ", "+")
        complexity_transformed = complexity.lower().replace(":", "%3A").replace(" ", "+")
        Q_link_dict[role][complexity] = static_link_base+"+label%3A%22"+role_transformed+"%22"+"+label%3A%22"+complexity_transformed+"%22"

Q_link_dict["role: front end and backend/DevOps"] = {}     
for complexity in Q_unique_complexity:
    frontend_transformed = "role: front end".lower().replace(":", "%3A").replace(" ", "+")
    backend_transformed = "role: back end/devOps".lower().replace(":", "%3A").replace(" ", "+")
    complexity_transformed = complexity.lower().replace(":", "%3A").replace(" ", "+")
    Q_link_dict["role: front end and backend/DevOps"][complexity] = static_link_base+"+label%3A%22"+frontend_transformed+"%22"+"+label%3A%22"+backend_transformed+"%22"+"+label%3A%22"+complexity_transformed+"%22"
    
Q_dataset2["Role-based Link for Unknown Status"] = ""
for role in Q_link_dict.keys():
    df = Q_dataset2[Q_dataset2["Role Label"] == role]
    for complexity in Q_link_dict[list(Q_link_dict.keys())[0]].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        Q_dataset2.loc[indexes, "Role-based Link for Unknown Status"] = Q_link_dict[role][complexity]


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
QA_dataset2 = QA_dataset[["Project Board Column", "Runtime", "Role Label", "Complexity Label", "html_url", "title", "Draft", "2 weeks inactive", "ready for product", "ready for dev lead", "Ready for Prioritization"]]

# Add in unknown status columns

# Create a column to identify issues with unknown status
QA_unknown_status_wdataset = QA_issues_df2.copy()
QA_unknown_status_wdataset["Known Status"] = QA_unknown_status_wdataset["labels.name"].map(lambda x: 1 if (re.search(r"(ready|draft)", str(x).lower())) else 0)
QA_known_status_issues = list(QA_unknown_status_wdataset[QA_unknown_status_wdataset["Known Status"] == 1]["html_url"].unique())
QA_dataset2["Unknown Status"] = QA_dataset2["html_url"].map(lambda x: 0 if x in QA_known_status_issues else 1) 

# Create nested dictionary for static links

QA_unique_roles = [x for x in QA_dataset2["Role Label"].unique() if pd.isna(x) == False]
QA_unique_roles2 = [x for x in QA_unique_roles if x != "role: front end and backend/DevOps"]
QA_unique_complexity = [x for x in QA_dataset2["Complexity Label"].unique() if pd.isna(x) == False]

QA_link_dict = { }

for role in QA_unique_roles:
    QA_link_dict[role] = {}
    for complexity in QA_unique_complexity:
        role_transformed = role.lower().replace(":", "%3A").replace(" ", "+")
        complexity_transformed = complexity.lower().replace(":", "%3A").replace(" ", "+")
        QA_link_dict[role][complexity] = static_link_base+"+label%3A%22"+role_transformed+"%22"+"+label%3A%22"+complexity_transformed+"%22"

QA_link_dict["role: front end and backend/DevOps"] = {}     
for complexity in QA_unique_complexity:
    frontend_transformed = "role: front end".lower().replace(":", "%3A").replace(" ", "+")
    backend_transformed = "role: back end/devOps".lower().replace(":", "%3A").replace(" ", "+")
    complexity_transformed = complexity.lower().replace(":", "%3A").replace(" ", "+")
    QA_link_dict["role: front end and backend/DevOps"][complexity] = static_link_base+"+label%3A%22"+frontend_transformed+"%22"+"+label%3A%22"+backend_transformed+"%22"+"+label%3A%22"+complexity_transformed+"%22"
    
QA_dataset2["Role-based Link for Unknown Status"] = ""
for role in QA_link_dict.keys():
    df = QA_dataset2[QA_dataset2["Role Label"] == role]
    for complexity in QA_link_dict[list(QA_link_dict.keys())[0]].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        QA_dataset2.loc[indexes, "Role-based Link for Unknown Status"] = QA_link_dict[role][complexity]

# QA_dataset2["General Link for Developer Unknown Status"] = ""


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

UAT_dataset2 = UAT_dataset[["Project Board Column", "Runtime", "Role Label", "Complexity Label", "html_url", "title", "Draft", "2 weeks inactive", "ready for product", "ready for dev lead", "Ready for Prioritization"]]

# Add in unknown status columns

# Create a column to identify issues with unknown status
UAT_unknown_status_wdataset = UAT_issues_df2.copy()
UAT_unknown_status_wdataset["Known Status"] = UAT_unknown_status_wdataset["labels.name"].map(lambda x: 1 if (re.search(r"(ready|draft)", str(x).lower())) else 0)
UAT_known_status_issues = list(UAT_unknown_status_wdataset[UAT_unknown_status_wdataset["Known Status"] == 1]["html_url"].unique())
UAT_dataset2["Unknown Status"] = UAT_dataset2["html_url"].map(lambda x: 0 if x in UAT_known_status_issues else 1) 

# Create nested dictionary for static links

UAT_unique_roles = [x for x in UAT_dataset2["Role Label"].unique() if pd.isna(x) == False]
UAT_unique_roles2 = [x for x in UAT_unique_roles if x != "role: front end and backend/DevOps"]
UAT_unique_complexity = [x for x in UAT_dataset2["Complexity Label"].unique() if pd.isna(x) == False]

UAT_link_dict = { }

for role in UAT_unique_roles:
    UAT_link_dict[role] = {}
    for complexity in UAT_unique_complexity:
        role_transformed = role.lower().replace(":", "%3A").replace(" ", "+")
        complexity_transformed = complexity.lower().replace(":", "%3A").replace(" ", "+")
        UAT_link_dict[role][complexity] = static_link_base+"+label%3A%22"+role_transformed+"%22"+"+label%3A%22"+complexity_transformed+"%22"

UAT_link_dict["role: front end and backend/DevOps"] = {}     
for complexity in UAT_unique_complexity:
    frontend_transformed = "role: front end".lower().replace(":", "%3A").replace(" ", "+")
    backend_transformed = "role: back end/devOps".lower().replace(":", "%3A").replace(" ", "+")
    complexity_transformed = complexity.lower().replace(":", "%3A").replace(" ", "+")
    UAT_link_dict["role: front end and backend/DevOps"][complexity] = static_link_base+"+label%3A%22"+frontend_transformed+"%22"+"+label%3A%22"+backend_transformed+"%22"+"+label%3A%22"+complexity_transformed+"%22"
    
UAT_dataset2["Role-based Link for Unknown Status"] = ""
for role in UAT_link_dict.keys():
    df = UAT_dataset2[UAT_dataset2["Role Label"] == role]
    for complexity in UAT_link_dict[list(UAT_link_dict.keys())[0]].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        UAT_dataset2.loc[indexes, "Role-based Link for Unknown Status"] = UAT_link_dict[role][complexity]


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

QA_review_dataset2 = QA_review_dataset[["Project Board Column", "Runtime", "Role Label", "Complexity Label", "html_url", "title", "Draft", "2 weeks inactive", "ready for product", "ready for dev lead", "Ready for Prioritization"]]

# Add in unknown status columns


# Create a column to identify issues with unknown status
QA_review_unknown_status_wdataset = QA_review_issues_df2.copy()
QA_review_unknown_status_wdataset["Known Status"] = QA_review_unknown_status_wdataset["labels.name"].map(lambda x: 1 if (re.search(r"(ready|draft)", str(x).lower())) else 0)
QA_review_known_status_issues = list(QA_review_unknown_status_wdataset[QA_review_unknown_status_wdataset["Known Status"] == 1]["html_url"].unique())
QA_review_dataset2["Unknown Status"] = QA_review_dataset2["html_url"].map(lambda x: 0 if x in QA_review_known_status_issues else 1) 

# Create nested dictionary for static links

QA_review_unique_roles = [x for x in QA_review_dataset2["Role Label"].unique() if pd.isna(x) == False]
QA_review_unique_roles2 = [x for x in QA_review_unique_roles if x != "role: front end and backend/DevOps"]
QA_review_unique_complexity = [x for x in QA_review_dataset2["Complexity Label"].unique() if pd.isna(x) == False]

QA_review_link_dict = { }

for role in QA_review_unique_roles:
    QA_review_link_dict[role] = {}
    for complexity in QA_review_unique_complexity:
        role_transformed = role.lower().replace(":", "%3A").replace(" ", "+")
        complexity_transformed = complexity.lower().replace(":", "%3A").replace(" ", "+")
        QA_review_link_dict[role][complexity] = static_link_base+"+label%3A%22"+role_transformed+"%22"+"+label%3A%22"+complexity_transformed+"%22"

QA_review_link_dict["role: front end and backend/DevOps"] = {}     
for complexity in QA_review_unique_complexity:
    frontend_transformed = "role: front end".lower().replace(":", "%3A").replace(" ", "+")
    backend_transformed = "role: back end/devOps".lower().replace(":", "%3A").replace(" ", "+")
    complexity_transformed = complexity.lower().replace(":", "%3A").replace(" ", "+")
    QA_review_link_dict["role: front end and backend/DevOps"][complexity] = static_link_base+"+label%3A%22"+frontend_transformed+"%22"+"+label%3A%22"+backend_transformed+"%22"+"+label%3A%22"+complexity_transformed+"%22"
    
QA_review_dataset2["Role-based Link for Unknown Status"] = ""
for role in QA_review_link_dict.keys():
    df = QA_review_dataset2[QA_review_dataset2["Role Label"] == role]
    for complexity in QA_review_link_dict[list(QA_review_link_dict.keys())[0]].keys(): #same for all roles
        df2 = df[df["Complexity Label"] == complexity]
        indexes = df2.index
        QA_review_dataset2.loc[indexes, "Role-based Link for Unknown Status"] = QA_review_link_dict[role][complexity]

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

outdated_labels = list(LC_df[LC_df["in_use?"] == "No"]["label_name"].unique())
official_active_labels = list(set(list(LC_df["label_name"])).difference(set(outdated_labels)))

anomaly_detection_df["labels_need_action"] = anomaly_detection_df["labels.name"].map(lambda x: 1 if x not in official_active_labels else 0)
anomaly_detection_df["outdated_label"] = anomaly_detection_df["labels.name"].map(lambda x: 1 if x in outdated_labels else 0)
anomaly_detection_df["unknown_label"] = anomaly_detection_df["labels.name"].map(lambda x: 1 if (x not in official_active_labels and x not in outdated_labels) else 0)
anomaly_detection_df["Label Transformed"] = anomaly_detection_df["labels.name"].map(lambda x: x.lower().replace(":", "%3A").replace(" ", "+") if pd.isna(x) == False else x)
anomaly_detection_df["Link for Quick Correction"] = anomaly_detection_df["Label Transformed"].map(lambda x: "https://github.com/hackforla/website/issues?q=is%3Aissue+label%3A"+str(x) if pd.isna(x) == False else np.nan)

anomaly_detection_df.drop(columns = ["Label Transformed"], inplace = True)

anomaly_detection_df = anomaly_detection_df[["Project Board Column", "Runtime", "html_url", "title", "labels.name", "labels_need_action", "outdated_label", "unknown_label", "Link for Quick Correction"]]

# In[49]:

anomaly_detection_df2_base = anomaly_detection_df.copy()
anomaly_detection_df2_base.drop(columns = ["labels_need_action"], inplace = True)
anomaly_detection_df2_base.drop(columns = ["outdated_label"], inplace = True)
anomaly_detection_df2_base.drop(columns = ["unknown_label"], inplace = True)
anomaly_detection_df2_base.drop(columns = ["Link for Quick Correction"], inplace = True)

# Includes official labels that are current and outdated
official_complexity = list(LC_df[LC_df["label_series"] == "complexity"]["label_name"])
official_feature = list(LC_df[LC_df["label_series"] == "feature"]["label_name"])
official_role = list(LC_df[LC_df["label_series"] == "role"]["label_name"])
official_size = list(LC_df[LC_df["label_series"] == "size"]["label_name"])

anomaly_detection_df2_base["Complexity Label"] = anomaly_detection_df2_base["labels.name"].map(lambda x: 1 if x in official_complexity else 0)
anomaly_detection_df2_base["Feature Label"] = anomaly_detection_df2_base["labels.name"].map(lambda x: 1 if x in official_feature else 0)
anomaly_detection_df2_base["Role Label"] = anomaly_detection_df2_base["labels.name"].map(lambda x: 1 if x in official_role else 0)
anomaly_detection_df2_base["Size Label"]= anomaly_detection_df2_base["labels.name"].map(lambda x: 1 if x in official_size else 0)

complexity_missing_series = list(LC_df[(LC_df["label_series"] == "complexity") & (LC_df["missing_series?"] == "Yes")]["label_name"])[0]
feature_missing_series = list(LC_df[(LC_df["label_series"] == "feature") & (LC_df["missing_series?"] == "Yes")]["label_name"])[0]
role_missing_series = list(LC_df[(LC_df["label_series"] == "role") & (LC_df["missing_series?"] == "Yes")]["label_name"])[0]
size_missing_series = list(LC_df[(LC_df["label_series"] == "size") & (LC_df["missing_series?"] == "Yes")]["label_name"])[0]

anomaly_detection_df2_base["Complexity Missing Label"] = anomaly_detection_df2_base["labels.name"].map(lambda x: 1 if x == complexity_missing_series else 0)
anomaly_detection_df2_base["Feature Missing Label"] = anomaly_detection_df2_base["labels.name"].map(lambda x: 1 if x == feature_missing_series else 0)
anomaly_detection_df2_base["Role Missing Label"] = anomaly_detection_df2_base["labels.name"].map(lambda x: 1 if x == role_missing_series else 0)
anomaly_detection_df2_base["Size Missing Label"]= anomaly_detection_df2_base["labels.name"].map(lambda x: 1 if x == size_missing_series else 0)
 
anomaly_detection_df2 = anomaly_detection_df2_base.groupby(["Project Board Column", "Runtime", "html_url", "title"])[["Complexity Label", "Feature Label", "Role Label", "Size Label", "Complexity Missing Label", "Feature Missing Label", "Role Missing Label", "Size Missing Label"]].sum().reset_index()

anomaly_detection_df2["Complexity defined label"] = anomaly_detection_df2["Complexity Label"]-anomaly_detection_df2["Complexity Missing Label"]
anomaly_detection_df2["Feature defined label"] = anomaly_detection_df2["Feature Label"]-anomaly_detection_df2["Feature Missing Label"]
anomaly_detection_df2["Role defined label"] = anomaly_detection_df2["Role Label"]-anomaly_detection_df2["Role Missing Label"]
anomaly_detection_df2["Size defined label"] = anomaly_detection_df2["Size Label"]-anomaly_detection_df2["Size Missing Label"]

anomaly_detection_df2_join = anomaly_detection_df2_base[anomaly_detection_df2_base["Role Label"] == 1][["html_url", "labels.name"]]
anomaly_detection_df2 = anomaly_detection_df2.merge(anomaly_detection_df2_join, how = "left", on = ["html_url"])
epic_issues = list(anomaly_detection[anomaly_detection['labels.name'] == 'epic']['html_url'].unique())
ER_issues = list(anomaly_detection[anomaly_detection['labels.name'] == 'ER']['html_url'].unique())
anomaly_detection_df2['Epic Issue?'] = anomaly_detection_df2['html_url'].map(lambda x: 1 if x in epic_issues else 0)
anomaly_detection_df2['ER Issue?'] = anomaly_detection_df2['html_url'].map(lambda x: 1 if x in ER_issues else 0)

# change role label of issues with front end and back end labels
anomaly_detection_wdataset = anomaly_detection_df2[anomaly_detection_df2["labels.name"].str.contains("front end") | anomaly_detection_df2["labels.name"].str.contains("back end")]
anomaly_detection_wdataset["front/back end count"] = anomaly_detection_wdataset.groupby(["html_url", "title"])["labels.name"].transform("count")

anomaly_detection_df2.loc[list(anomaly_detection_wdataset[anomaly_detection_wdataset["front/back end count"] == 2].index), "labels.name"] = "role: front end and backend/DevOps"

# Drop duplicates that have been created by the change
anomaly_detection_df2.drop_duplicates(inplace = True)

# Create dataset that detects issues with missing dependencies in icebox
missing_dependency_label = list(LC_df[(LC_df["label_series"] == "dependency") & (LC_df["missing_series?"] == "Yes")]["label_name"])[0]
missing_dependency = anomaly_detection[(anomaly_detection["labels.name"] == missing_dependency_label) & (anomaly_detection["Project Board Column"] == "1 - Icebox")]
icebox_issues = list(anomaly_detection[anomaly_detection["Project Board Column"] == "1 - Icebox"]["html_url"].unique())
icebox_issues_with_dependency = list(anomaly_detection[(anomaly_detection["Project Board Column"] == "1 - Icebox") & (anomaly_detection["labels.name"] == "Dependency")]["html_url"].unique())
icebox_issues_without_dependency = list(set(icebox_issues).difference(set(icebox_issues_with_dependency)))
no_dependency = anomaly_detection[anomaly_detection["html_url"].isin(icebox_issues_without_dependency)]
missing_dependency = pd.concat([missing_dependency, no_dependency], ignore_index = True)

missing_dependency = missing_dependency.iloc[:, [1,4,2,3,0]]

if len(missing_dependency) == 0:
    missing_dependency.loc[0] = [" "," "," "," "," "]
else:
    missing_dependency

# Create dataset with issues that have labels in missing series (to be joined for anomaly report in Looker)
missingseries_labels = list(LC_df[LC_df["missing_series?"] == "Yes"]["label_name"])
issues_w_missinglabels = anomaly_detection[anomaly_detection['labels.name'].isin(missingseries_labels)][["Project Board Column", "html_url", "title", "labels.name"]]

# Create new table that draws in all issues with ER title that do not have ER label

ER_label_check = ER_issues_df3.copy()
ER_label_check["ER Label?"] = ER_label_check['html_url'].map(lambda x: 1 if x in ER_issues else 0)
No_ER_label = ER_label_check[ER_label_check["ER Label?"] == 0]
No_ER_label_filtered = No_ER_label[~No_ER_label["title"].str.contains("ER from TLDL", case = False)]
No_ER_label_filtered.drop(columns = ["labels.name"], inplace = True)
No_ER_label_filtered.drop_duplicates(inplace = True)

if len(No_ER_label_filtered) == 0:
    No_ER_label_filtered = pd.DataFrame(columns = ["Runtime", "html_url", "title", "Project Board Column", "ER Label?", "state"])
    No_ER_label_filtered.loc[0] = [" "," "," "," "," "," "]
else:
    no_ERlabel_issuestate = pd.DataFrame()

    for url in No_ER_label_filtered["html_url"]:
        issue_number = re.findall(r'[0-9]+$', url)[0]
        html = "https://api.github.com/repos/hackforla/website/issues/"+issue_number
        response = requests.get(html, auth=(user, GitHub_token))
        df = pd.json_normalize(response.json())[["html_url", "state"]]
        no_ERlabel_issuestate = pd.concat([no_ERlabel_issuestate, df], ignore_index = True)

    No_ER_label_filtered = No_ER_label_filtered.merge(no_ERlabel_issuestate, how = "left", on = "html_url")

# Create a table that displays issues with Complexity: Missing label with first comment being an empty description
excluded_columns = ["1 - Icebox", "2 - ER", "3 - New Issue Approval"]
empty_description_search = final_dataset[(~final_dataset["Project Board Column"].isin(excluded_columns)) & (final_dataset["Complexity Label"] == "Complexity: Missing")]

empty_comment = []

for url in empty_description_search["html_url"]:
    issue_number = re.findall(r'[0-9]+$', url)[0]
    html = "https://api.github.com/repos/hackforla/website/issues/"+issue_number+"/timeline"
    response = requests.get(html, auth=(user, GitHub_token))
    df = pd.DataFrame(response.json())
    if ("body" not in list(df.columns)):
        if (df.iloc[0]["actor"]['login'] != 'github-actions[bot]' and df.iloc[0]["event"] == "cross-referenced"):
            empty_comment.append(url)
    elif ("body" in list(df.columns)):
        if (pd.isna(df.iloc[0]["body"]) == True and df.iloc[0]["actor"]['login'] != 'github-actions[bot]' and df.iloc[0]["event"] == "cross-referenced"):
            empty_comment.append(url)
    else:
        continue

complexity_missing_emptycomment = final_dataset[final_dataset["html_url"].isin(empty_comment)][["Project Board Column", "Role Label", "Complexity Label", "html_url", "title"]]
if len(complexity_missing_emptycomment) == 0:
    complexity_missing_emptycomment.loc[0] = [" "," "," "," "," "]
else:
    complexity_missing_emptycomment

# ### Send Data to Google Sheets

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

sheet_name4 = 'Issues with Missing Series Labels'
worksheet4 = gs.worksheet(sheet_name4)
worksheet4.clear()
set_with_dataframe(worksheet = worksheet4, dataframe = issues_w_missinglabels, include_index = False, include_column_header = True, resize = True)

sheet_name5 = 'Icebox Issues with Missing or No Dependency'
worksheet5 = gs.worksheet(sheet_name5)
worksheet5.clear()
set_with_dataframe(worksheet = worksheet5, dataframe = missing_dependency, include_index = False, include_column_header = True, resize = True)

sheet_name6 = 'Missing ER Label'
worksheet6 = gs.worksheet(sheet_name6)
worksheet6.clear()
set_with_dataframe(worksheet = worksheet6, dataframe = No_ER_label_filtered, include_index = False, include_column_header = True, resize = True)

sheet_name7 = 'Complexity Missing Issues with Empty 1st Comment'
worksheet7 = gs.worksheet(sheet_name7)
worksheet7.clear()
set_with_dataframe(worksheet = worksheet7, dataframe = complexity_missing_emptycomment, include_index = False, include_column_header = True, resize = True)