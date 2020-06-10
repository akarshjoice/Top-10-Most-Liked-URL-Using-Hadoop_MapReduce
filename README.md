# Top-10-Most-Liked-URL-Using-Hadoop_MapReduce

Facebook has a large number of APP servers, each dealing with a particular set of users. Assume that the set of all Facebook users is distributed randomly among these servers for load balancing. Whenever a user clicks a like, an entry is made into the log file in that server with these values: 

  Timestamp
	URL
	User’s age
	User’s location
	User’s sex.

Build a dashboard that shows the top 10 most liked URLs (updated hourly) by each user demographic. Information from the logs must be used for this purpose.

Set up a MapReduce implementation that emits the like count for each URL and user demographic.Use a custom generated dataset
