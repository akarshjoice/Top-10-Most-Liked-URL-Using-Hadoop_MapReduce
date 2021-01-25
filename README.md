# Top-10-Most-Liked-URL-Using-Hadoop_MapReduce

Facebook has a large number of APP servers, each dealing with a particular set of users. Assume that the set of all Facebook users is distributed randomly among these servers for load balancing. Whenever a user clicks a like, an entry is made into the log file in that server with these values: 

	Timestamp
	URL
	User’s age
	User’s location
	User’s sex.

Set up a MapReduce implementation that emits the like count for each URL based on user demographic. Use a custom generated dataset.The Reduce functions can run in a separate server(s). Map runs in each Facebook APP Server and simply emits the link count for each URL and user demographic. The Reduce aggregates the link counts on a continuous basis and this is used to populate the dashboard.
