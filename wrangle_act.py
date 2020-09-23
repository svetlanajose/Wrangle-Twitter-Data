#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')
import requests
import tweepy 
import time
import json


# # Data Wrangling
# 
# Data Wrangling consists of :
#     <li>Gathering Data</li>
#     <li>Assessing Data</li>
#     <li>Cleaning Data</li>
# After the Data Wrangling process, data is stored into new files if required, analyzed and visualized to deliver insignts. 
# 
# This project addresses only a subset of data wrangling issues: Eight quality issues and two tidiness issues at the minimum will be addressed. 
# 
# Following the analysis, reports are created which explains in detail the wrangling efforts and the insights delivered post analysis.

# ### Gathering Data
# 
# 1. Gather data from an existing file (twitter-archive-enhanced.csv) using pandas library.
# 2. Download a tsv file from the internet using requests. (https://d17h27t6h515a5.cloudfront.net/topher/2017/August/599fd2ad_image-predictions/image-predictions.tsv )
# 3. Querying a twitter API, gather selected JSON data of each individual tweet IDs using Tweepy into a dictionary, and convert it into a dataframe, which is then read using pandas library. 

# In[2]:


#read WeRateDogs twitter archive as pandas dataframe
tweet_archive = pd.read_csv('twitter-archive-enhanced.csv') 


# In[3]:


tweet_archive.head()


# In[4]:


tweet_archive.info()


# In[5]:


#Read tweet image predictions programatically by using requests library
#to download tsv file
url =  'https://d17h27t6h515a5.cloudfront.net/topher/2017/August/599fd2ad_image-predictions/image-predictions.tsv'
response = requests.get(url)

with open('image-predictions.tsv',mode='wb')as file:
    file.write(response.content)


# In[6]:


image_prediction = pd.read_csv('image-predictions.tsv',sep='\t')
image_prediction.head()


# In[7]:


image_prediction.info()


# In[8]:


#Query the Twitter API for each JSON data using Tweepy library 


# In[9]:


import tweepy

consumer_key = 'DmQmME8hlck95fTTOWPa96vve'
consumer_secret = 'aGOHMA4D21ZvwVtgCoQvdALvBBCBR0CQHLXvKq1U05AcMBwQRw'
access_token = '1286769360938262528-doxHdHFqlRcGoKh9FpcnpCtU7XoHip'
access_secret = 'q9E1BxS62nq4OWcN94LmuG53iz3Ey3fQIOsh237P7sI3B'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth, parser = tweepy.parsers.JSONParser(), 
                 wait_on_rate_limit = True, 
                 wait_on_rate_limit_notify = True)


# In[10]:


#Fetches the status/tweet for each tweet based on tweet_ID and adds it to the list
tweet_list=[]
#List of tweet IDs that could not be found
error_list = []
#Calculate execution time
start = time.time()

#https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object
for tweet_id in tweet_archive['tweet_id']:
    try:
        tweet = api.get_status(tweet_id,tweet_mode='extended')
        favorites = tweet['favorite_count'] #count of favorites of the tweet
        retweets = tweet['retweet_count'] #number of retweets
        user_followers = tweet['user']['followers_count'] #number of followers of the user
        user_favourites = tweet['user']['favourites_count']#number of favorites the user had
        date_time = tweet['created_at']#data and time of tweet creation
        
        
        tweet_list.append({'tweet_id':int(tweet_id),
                           'favorites':int(favorites),
                           'retweets':int(retweets),
                           'followers':int(user_followers),
                           'user_favourites':int(user_favourites),
                           'date_time':pd.to_datetime(date_time)
                          })
        
    except Exception as e:
        error_list.append(tweet_id)

print("The lenght of tweets" ,len(tweet_list))
print("The length of tweets not found" , len(error_list))

end = time.time()
print("time taken:", end-start)


# In[11]:


#Converting the dictionary of tweets (based on tweet ID) 
# of JSON data into a dataframe

tweet_df = pd.DataFrame(tweet_list,columns = ['tweet_id','favorites','retweets','followers',
                                             'user_favourites','date_time'])
tweet_df.to_csv('json_tweet.txt',encoding='utf-8',index=False)


# In[12]:


tweepy_df = pd.read_csv('json_tweet.txt',encoding='utf8')
tweepy_df.head()


# In[13]:


tweepy_df.info()


# ### Assessing Data

# In[14]:


tweet_archive.head()


# In[15]:


tweet_archive.info() #indicates lots of missing values


# In[16]:


tweet_archive['rating_numerator'].value_counts()


# In[17]:


tweet_archive['rating_denominator'].value_counts()


# In[18]:


tweet_archive.describe()


# In[19]:


tweet_archive.source.value_counts() #Sources are difficult to read


# In[20]:


#original tweets do not have retweeted status id. 
#2175 original tweets exist.
tweet_archive[tweet_archive['retweeted_status_id'].isnull()].count() 


# In[21]:


sum(tweet_archive['tweet_id'].duplicated())


# In[22]:


with pd.option_context('max_colwidth', 200):
    display(tweet_archive[tweet_archive['text'].str.contains(r"(\d+\.\d*\/\d+)")]
            [['tweet_id', 'text', 'rating_numerator', 'rating_denominator']])


# In[23]:


image_prediction.sample(10)


# In[24]:


image_prediction.info()


# In[25]:


image_prediction.shape


# In[26]:


sum(image_prediction['jpg_url'].duplicated())


# In[27]:


image_prediction['jpg_url'].value_counts()


# In[28]:


print(image_prediction.p1_dog.value_counts())
print(image_prediction.p2_dog.value_counts())
print(image_prediction.p3_dog.value_counts())


# In[29]:


tweepy_df.sample(10)


# In[30]:


tweepy_df.info()


# In[31]:


tweepy_df.query('retweets > 3000').shape #tweets with more than 3000 retweets


# ### Quality (content issues)
# 
# ##### tweet_archive
# 1. Remove columns with lots of missing values and are not required for analysis.
# 2. We only want original ratings (No retweets) that have images.
# 3. Sources are difficult to read.
# 4. Convert None to NaN to indicate null objects.
# 5. Denominators vary, making comparison harder. Need to set a common denominator 10 for all.
# 6. Correct numerators with decimals.
# 7. Ensure 'tweet_id' is of consistent datatype across all datasets.
# 
# ##### image_prediction
# 1. Drop duplicated jpg_url entries. 
# 2. Delete columns not required for analysis.
# 3. Create a single column for prediction and single column for confidence level.
# 
# 
# ### Tidiness (structural issues)
# 1. Merge all dog stages column (doggo, pupper, etc) into a single column.
# 1. Reduce number of datasets by combining datasets on common column "tweet_id"
# 
# 
# 
# 

# ### Cleaning Data

# #### tweet_archive

# In[32]:


tweet_archive_clean = tweet_archive.copy()


# In[33]:


# Remove columns with lots of missing values and are not required 
#for analysis.

tweet_archive_clean.info()


# In[34]:


#Eliminating retweets

tweet_archive_clean = tweet_archive_clean[tweet_archive_clean['retweeted_status_id'].isnull()]


# In[35]:


tweet_archive_clean.info()


# In[36]:


#testing for retweets
tweet_archive_clean['retweeted_status_id'].value_counts()


# In[37]:


#Remove columns with lots of missing values 
#and are not required for analysis.

tweet_archive_clean.columns


# In[38]:


tweet_archive_clean = tweet_archive_clean.drop(['in_reply_to_status_id',
        'in_reply_to_user_id', 'retweeted_status_id', 'retweeted_status_user_id',
        'retweeted_status_timestamp', 'expanded_urls','name'],1)


# In[39]:


print(tweet_archive_clean.columns)


# In[40]:


tweet_archive_clean.head()


# In[41]:


#Convert None to NaN to indicate null objects.

tweet_archive_clean.replace(to_replace="None", value=np.nan,inplace=True)


# In[42]:


#Clean the Sources column and make it easy to read
import re
tweet_archive_clean['source'] = tweet_archive_clean['source'].apply(lambda x: re.findall(r'>(.*)<', x)[0])


# In[43]:


tweet_archive_clean['source'].value_counts()


# In[44]:


#Make all denominators consistent.
#In this dataset, majority of the entries are with denominator 10.
#The number of entries with other values as denominators is very small
#compared to the size of the dataset and hence can be eliminated.

tweet_archive_clean['rating_denominator'].value_counts()


# In[45]:


tweet_archive_clean=tweet_archive_clean.query('rating_denominator == 10')


# In[46]:


tweet_archive_clean['rating_denominator'].value_counts()


# In[47]:


#Correct numerators with decimals.
#For this, we first need to convert the numerator and denominators 
#type into to float to allow decimals.
tweet_archive_clean[['rating_numerator', 'rating_denominator']] = tweet_archive_clean[['rating_numerator','rating_denominator']].astype(float)
tweet_archive_clean.info() #test


# In[48]:


with pd.option_context('max_colwidth', 200):
    display(tweet_archive_clean[tweet_archive_clean['text'].str.contains(r"(\d+\.\d*\/\d+)")]
            [['tweet_id', 'text', 'rating_numerator', 'rating_denominator']])


# In[49]:


#Update numerators of specific tweet_ids that have decimal ratings.
tweet_archive_clean.loc[(tweet_archive_clean.tweet_id == 883482846933004288), 'rating_numerator'] = 13.5
tweet_archive_clean.loc[(tweet_archive_clean.tweet_id == 786709082849828864), 'rating_numerator'] = 9.75
tweet_archive_clean.loc[(tweet_archive_clean.tweet_id == 778027034220126208), 'rating_numerator'] = 11.27
tweet_archive_clean.loc[(tweet_archive_clean.tweet_id == 681340665377193984), 'rating_numerator'] = 9.5
tweet_archive_clean.loc[(tweet_archive_clean.tweet_id == 680494726643068929), 'rating_numerator'] = 11.26


# In[50]:


#Testing
with pd.option_context('max_colwidth', 200):
    display(tweet_archive_clean[tweet_archive_clean['text'].str.contains(r"(\d+\.\d*\/\d+)")]
            [['tweet_id', 'text', 'rating_numerator', 'rating_denominator']])


# In[51]:


tweet_archive_clean.info()


# #### image_prediction

# In[52]:


image_prediction_clean = image_prediction.copy()


# In[53]:


image_prediction_clean.info()


# In[54]:


image_prediction_clean[image_prediction_clean['jpg_url'].duplicated()].shape
#66 duplicate entries to be removed


# In[55]:


# Deleting duplicate entries

del_df = image_prediction_clean[image_prediction_clean['jpg_url'].duplicated()]


# In[56]:


image_prediction_clean = image_prediction_clean.drop(del_df.index)


# In[57]:


image_prediction_clean[image_prediction_clean['jpg_url'].duplicated()].shape


# In[58]:


image_prediction_clean.info()


# In[59]:


#Create a single column for prediction and single column for 
#confidence level.


# In[60]:


#Creating two empty lists to capture the dog type and confidence level
dog_type = []
confidence_level = []


# In[61]:


#Creating a function to parse through the column with 'True' Boolean value
#prediction, and capture the associated dog type and confidence level

def prediction(image_prediction_clean):
    if image_prediction_clean['p1_dog'] == True:
        dog_type.append(image_prediction_clean['p1'])
        confidence_level.append(image_prediction_clean['p1_conf'])
    elif image_prediction_clean['p2_dog'] == True:
        dog_type.append(image_prediction_clean['p2'])
        confidence_level.append(image_prediction_clean['p2_conf'])
    elif image_prediction_clean['p3_dog'] == True:
        dog_type.append(image_prediction_clean['p3'])
        confidence_level.append(image_prediction_clean['p3_conf'])
    else:
        dog_type.append('Error')
        confidence_level.append('Error')
        
#Call the function

image_prediction_clean.apply(prediction, axis=1)

#Convert lists into dataframe columns

image_prediction_clean['dog_type']=dog_type
image_prediction_clean['confidence_level']=confidence_level


# In[62]:


#Entries with no true predictions and have errors are eliminated from 
#the df.
image_prediction_clean = image_prediction_clean[image_prediction_clean['dog_type']!= 'Error']


# In[63]:


image_prediction_clean.info()


# In[64]:


#drop columns not required for analysis.
image_prediction_clean=image_prediction_clean.drop(['img_num', 'p1', 
                                                      'p1_conf', 'p1_dog', 
                                                      'p2', 'p2_conf', 
                                                      'p2_dog', 'p3', 
                                                      'p3_conf', 
                                                      'p3_dog'],1)


# In[65]:


image_prediction_clean.info()


# ### Tidiness Issues
# #### Merge all dog stages column (doggo, pupper, etc) into a single column.

# In[66]:


tweet_archive_clean.head()


# In[67]:


#Concatenate columns to form a single column 'dog_stage'

cols = ['doggo', 'floofer', 'pupper','puppo']
tweet_archive_clean['dog_stage'] = tweet_archive_clean[cols].apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
tweet_archive_clean.head(100)


# In[68]:


#Replace irrelevant characters with null

tweet_archive_clean['dog_stage'] = tweet_archive_clean['dog_stage'].str.replace('nan', '')
tweet_archive_clean['dog_stage'] = tweet_archive_clean['dog_stage'].str.replace('_', '')


# In[69]:


tweet_archive_clean['dog_stage'].unique()


# In[70]:


tweet_archive_clean=tweet_archive_clean.drop(['doggo','floofer','pupper','puppo'],1)


# In[71]:


tweet_archive_clean.head(100)


# #### Merge dataframes into a single dataframe

# In[72]:


twitter_data1 = pd.merge(image_prediction_clean,tweepy_df,
                        how='left',on=['tweet_id'])


# In[73]:


twitter_data1.info()


# In[74]:


twitter_data = pd.merge(twitter_data1,tweet_archive_clean,
                       how='left',on=['tweet_id'])
twitter_data.info()


# In[75]:


twitter_data.dropna(axis=0,inplace=True)


# In[76]:


twitter_data.info()


# In[77]:


twitter_data.head()


# In[78]:


twitter_data.drop('timestamp',axis=1,inplace=True)


# In[79]:


twitter_data.info()


# ### Storing Data

# In[80]:


#Store the merged file into a new CSV file
twitter_data.to_csv('twitter_archive_master.csv',index=False,encoding='utf-8')


# In[81]:


twitter_df = pd.read_csv('twitter_archive_master.csv')


# ### Visualizing Data & Gathering Insights

# In[82]:


twitter_df.info()


# #### Defining Questions
# 
# 1. What are the 10 most common dog types? 
# 2. What dog types are the highest rated and lowest rated?
# 3. Tweets of what dog types get retweeted the most?
# 4. Tweets of what dog types get most favorited?
# 5. Relationship between favorites and retweets?
# 5. Is there a relationship between average rating and number of retweets?
# 6. Is there a relationship between average rating and number of ratings received?
# 7. Pattern of retweets and favorites over time? 
# 8. Is there a relationship between dog type and confidence level of prediction on an average? Which dog type predictions have a confidence level above 0.5 on an average?
# 9. What is the distribution of dog stage for the twitter dataset?
# 
# 
# 
# 

# In[83]:


twitter_df.head()


# In[84]:


common_dogs = twitter_df['dog_type'].value_counts().head(10).rename_axis('dog_type').reset_index(name='counts')
common_dogs


# In[85]:


#What are the 10 most common dog types? 


plt.pie(common_dogs['counts'], labels=common_dogs['dog_type'])
plt.title("Most Common Dog Types")
plt.show()
#plt.show()


# In[86]:


#What dog types are the highest rated and lowest rated on average?

twitter_df.groupby('dog_type',as_index=False)['rating_numerator'].mean().sort_values('rating_numerator',ascending=False).head()


# In[87]:


twitter_df.groupby('dog_type',as_index=False)['rating_numerator'].mean().sort_values('rating_numerator',ascending=False).tail()


# In[88]:


#Tweets of what dog types get retweeted the most?
df1 = twitter_df.groupby('dog_type',as_index=False)['retweets'
].mean().sort_values('retweets',ascending=False)
df1.head()


# In[89]:


#Tweets of what dog types get favorited the most?
df2=twitter_df.groupby('dog_type',as_index=False)['favorites'
].mean().sort_values('favorites',ascending=False)
df2.head()


# In[90]:


#Relationship between favorites and retweets?

retweet_favorites = pd.merge(df1,df2,how='left',on=['dog_type'])
retweet_favorites.head()


# In[91]:



retweet_favorites.plot(x='retweets', y='favorites', kind='scatter')
plt.xlabel('Average Number of Retweets')
plt.ylabel('Average Number of Favorites')
plt.title('Average Number of Retweets by Average Number of Favorites')


# In[92]:


#Is there a relationship between average rating and number of 
#retweets?

rating_retweets = twitter_df.groupby('dog_type',as_index=False)['rating_numerator'
,'retweets','favorites'].mean()
rating_retweets.head()


# In[93]:



x=rating_retweets['retweets']
y=rating_retweets['rating_numerator']
plt.scatter(x,y)

z = np.polyfit(x, y, 1)
p = np.poly1d(z)
plt.plot(x,p(x),"r--")

plt.xlabel('Average Number of Retweets')
plt.ylabel('Average Rating')
plt.title('Average Rating by Number of Retweets')

plt.show()


# In[94]:



#Is there a relationship between average rating and number of ratings received?
dog_type_rating = twitter_df.groupby('dog_type',as_index=False)['rating_numerator'
].mean()
dog_type_rating.head()


# In[95]:


dog_type_count = twitter_df.groupby('dog_type',as_index=False)['rating_numerator'
].count()
dog_type_count.head()


# In[96]:


x=dog_type_rating['rating_numerator']
y=dog_type_count['rating_numerator']
plt.scatter(x,y)

z = np.polyfit(x, y, 1)
p = np.poly1d(z)
plt.plot(x,p(x),"r--")

plt.xlabel('Average Rating')
plt.ylabel('Number of Ratings')
plt.title('Average Rating by Number of Ratings')

plt.show()


# In[97]:


twitter_df['date_time']=pd.to_datetime(twitter_df.date_time)


# In[98]:


twitter_df.set_index('date_time',inplace=True)


# In[99]:


twitter_df.sort_values('date_time',inplace=True)


# In[100]:


twitter_df.head(2)


# In[101]:


#Patter of Retweets and Favorites over time

twitter_df['retweets'].plot(color = 'red', label='Retweets')
twitter_df['favorites'].plot(color = 'blue', label='Favorites')

plt.style.use('seaborn-darkgrid')
plt.legend(loc='upper left')
plt.xlabel('Tweet timestamp')
plt.ylabel('Count')
plt.title('Retweets and favorites over time')
plt.show()


# In[102]:


#Is there a relationship between dog type and confidence level of prediction on an average? 


# In[103]:


confidence = twitter_df.groupby('dog_type',as_index=False)['confidence_level'].mean()
confidence.head()


# In[104]:


x=confidence['confidence_level']
y=dog_type_count['rating_numerator'] #number of ratings by dog type

plt.scatter(x,y)


plt.xlabel('Confidence Level')
plt.ylabel('Number of Ratings')
plt.title('Confidence level by Number of Ratings')


# In[105]:


#Which dog type predictions have a confidence level above 0.6 on an average?
confidence.query('confidence_level > 0.6')


# In[106]:


twitter_df['dog_stage'].value_counts()


# In[107]:


twitter_df['dog_stage'].value_counts().plot(kind='bar')
plt.xlabel('Dog_stage')
plt.ylabel('Count')
plt.title('Count of Dogs by Dog Stage')

