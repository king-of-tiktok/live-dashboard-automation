# -*- coding: utf-8 -*-
"""Data Cleaning and Cohort Analysis (Last Updated: 05/15/2023)

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/166RVisJ07g569FxbOzxx8HUG9F54N6BJ
"""

# import relevant libraries

import re
import pandas as pd
import json
import requests
import time
import datetime
import math
import os 

"""Step 1: Retrieve all closed prework issues to get all onboarded prework authors to date."""

GitHib_token = os.environ["API_KEY_GITHUB"]

# Retrieve all closed prework issues

# By default, the API only retrieves 30 issues per request. 
# We can maximize this to 100 issues per request but the number of closed issues
# are expected to exceed 100.

# For the sake of automation, to identify how many requests are needed
# as number of closed prework issue changes, we need to get the count of 
# closed prework issues in the website repository. 

url = "https://api.github.com/search/issues?q=org:hackforla+repo:hackforla/website+state:closed+label:prework"
request = requests.get(url)

# Put number of closed prework issues into a variable 
c_prework_num = pd.DataFrame(request.json())["total_count"].iloc[0]

# To check number of prework issues, run: 
# c_prework_num

# Number of requests needed to retrieve all closed prework issues:

request_num = math.ceil(c_prework_num/100)

closed_pw = pd.DataFrame()

for i in range(1, request_num+1):
  url = "https://api.github.com/repos/hackforla/website/issues?labels=prework&state=closed&per_page=100"+"&page="+str(i)
  request = requests.get(url)
  issues_df = pd.DataFrame(request.json())
  closed_pw = pd.concat([closed_pw, issues_df], ignore_index = True)

# Check whether number of closed prework issues matches, run:
len(closed_pw) == c_prework_num

# Let's see how dataframe looks like (remove hash to run)
closed_pw.head()

"""Step 2) Clean Data to Get List of Closed Prework Authors"""

# See list of columns in df
closed_pw.columns

# Investigate values in "active_lock_reason", "state_reason", "comments" columns

closed_pw["active_lock_reason"].unique()

closed_pw["state_reason"].unique() # this probably indicates issues that are "closed as not planned" - useful for another analysis

closed_pw["comments"].iloc[0] # shows number of comments - not useful

# remove unneeded columns
closedpw_reduced = closed_pw.drop(columns = ['url', 'repository_url', 'labels_url', 'comments_url', 'events_url', 'id', 
                                             'node_id', 'number', 'user', 'locked', 'milestone', 'comments', 'updated_at', 
                                             'author_association', 'active_lock_reason', 'timeline_url', 'performed_via_github_app', 'reactions', 'assignees'])

closedpw_reduced.head()

"""Remove non-developer prework issues and issues with "Ignore..." labels"""

# Flatten nested json data

# Phase 1
result = closedpw_reduced.to_json(orient = "records")
parsed = json.loads(result)
closedpw_reduced_flat = pd.json_normalize(parsed)
closedpw_reduced_flat.columns

closedpw_reduced_flat.drop(columns = ['assignee.id', 'assignee.node_id', 'assignee.avatar_url', 'assignee.gravatar_id', 'assignee.url', 'assignee.html_url', 'assignee.followers_url', 'assignee.following_url', 'assignee.gists_url', 'assignee.starred_url', 'assignee.subscriptions_url', 'assignee.organizations_url', 'assignee.repos_url', 'assignee.events_url', 'assignee.received_events_url', 'assignee.type', 'assignee.site_admin', 'assignee'], inplace = True)

closedpw_reduced_flat.columns

# Phase 2

result2 = closedpw_reduced_flat.to_json(orient = "records")
parsed2 = json.loads(result2)

closedpw_reduced_flat2 = pd.json_normalize(parsed2, record_path = ["labels"], record_prefix = "labels.", meta = ['html_url', 'title', 'state', 'created_at', 'closed_at', 
                                                                                                                 'body', 'state_reason', 'assignee.login'])

# closedpw_reduced_flat2.head(5)

closedpw_reduced_flat2.drop(columns = ['labels.id', 'labels.node_id', 'labels.url', 'labels.color', 'labels.default', 'labels.description'], inplace = True)

closedpw_reduced_flat2.head()

closedpw_reduced_flat2["labels.name"].unique() # There are ignore labels

# Get list of ignore labels

filter_labels = []
for label in closedpw_reduced_flat2["labels.name"].unique():
    if 'ignore' in label.lower(): # put everything in lowerrcase to eliminate case-sensitivity
        print(label)
        filter_labels.append(label)

# Create a unique_id column to filter out unwanted issues

# First, replace null values with a string value
closedpw_reduced_flat2["assignee.login"] = closedpw_reduced_flat2["assignee.login"].map(lambda x: 'NIL' if pd.isna(x) == True else x)

closedpw_reduced_flat2["Unique_ID"] = closedpw_reduced_flat2["title"] + closedpw_reduced_flat2["created_at"] + closedpw_reduced_flat2["assignee.login"]

# Double check if column was created correctly
closedpw_reduced_flat2.iloc[1, 9]

# Get prework issues that have ignore or draft labels

filter_labels.append("Draft")
id_to_remove = list(closedpw_reduced_flat2[closedpw_reduced_flat2["labels.name"].isin(filter_labels)]["Unique_ID"])
id_to_remove

# Filter out issues above

pw_clean = closedpw_reduced_flat2[~closedpw_reduced_flat2["Unique_ID"].isin(id_to_remove)]

# Now use regular expression to filter for only developer issues
# Issues that start with "Pre-work Checklist: Developer:" or has the word 'developer' in it

pw_clean["remove"] = [0 if re.search('developer', title.lower()) else 1 for title in pw_clean['title']]
bad_items_index = list(pw_clean[pw_clean["title"] == "Pre-work Checklist: Developer: [replace brackets with your name]"].index) # prework template name - unchanged
for i in bad_items_index:
  pw_clean.iloc[i, 10] = 1

pw_clean2 = pw_clean[pw_clean["remove"] == 0]

# list(pw_clean2["title"].unique())

pw_clean2.head(20)

pw_clean2.reset_index(drop = True, inplace = True)

pw_clean2.head(20)

# Remove duplicate rows and labels.name column
# Ensure there are no duplicates

pw_finaldf = pw_clean2[pw_clean2["labels.name"] == "prework"]
pw_finaldf2 = pw_finaldf.drop(columns = ["labels.name"])
pw_finaldf2.duplicated().sum()

# Finally, remove all prework with no assignees

pw_finaldf3 = pw_finaldf2[pw_finaldf2["assignee.login"] != 'NIL']
pw_finaldf3.reset_index(drop = True, inplace = True)

"""Step 3) Add in cohort information for prework authors"""

pw_finaldf3["cohort_year"] = pw_finaldf3["created_at"].map(lambda x: pd.to_datetime(x).year)
pw_finaldf3["cohort_month"] = pw_finaldf3["created_at"].map(lambda x: pd.to_datetime(x).month)
pw_finaldf3["cohort_monthtxt"] = pw_finaldf3["created_at"].map(lambda x: pd.to_datetime(x).strftime("%b"))

pw_finaldf3.head()

# pw_finaldf3.cohort_year.value_counts()

# pw_finaldf3.cohort_month.value_counts()

pw_finaldf3["cohort"] = pw_finaldf3["cohort_year"].astype(str)+" "+pw_finaldf3["cohort_monthtxt"].astype(str)

# pw_finaldf3.head()

"""Step 4) From the above dataset, get list of prework authors to retrieve all issues created by these authors."""

pw_finaldf3 = pw_finaldf3.sort_values(by = "created_at", ascending = False)  
# there are some developers who create multiple preworks with the same title. To prepare for cleaning these out, assuming that the latest prework issue they create is the most complete.

pw_finaldf3.drop_duplicates(subset = ["title", "cohort"], keep = "first", inplace = True)

prework_authors = list(pw_finaldf3["assignee.login"].unique())

# len(prework_authors)

"""Step 5) Retrieve Issue Data for Closed Prework Authors"""

# cohorts = list(pw_finaldf3["cohort"].unique())

# Identify ahead of time prework authors who are part of two distinct cohort

pw_finaldf4 = pw_finaldf3[["assignee.login", "cohort_year", "cohort_month", "cohort"]]

pw_finaldf4["assignee_count"] = pw_finaldf4.groupby("assignee.login")["assignee.login"].transform("count")

pw_finaldf4.head()

# pw_finaldf4[pw_finaldf4["assignee_count"]>1]

author_num = []

for cohort in cohorts:
    prework_authors = list(pw_reduced3[pw_reduced3["cohort"] == cohort]["assignee.login"].unique())
    print(prework_authors)
    print(len(prework_authors))
    author_num.append(len(prework_authors))
    
print("\n", sum(author_num))  # KazushiR appears twice due to doing two preworks