# DataEngineerChallenge

This is an interview challenge for PayPay. Please feel free to fork. Pull Requests will be ignored.
The challenge is to make analytical observations about the data using the distributed tools below.

## Processing & Analytical goals:

1.Sessionize the web log by IP. Sessionize = aggregate all page hits by visitor/IP during a session.
    https://en.wikipedia.org/wiki/Session_(web_analytics)
> I've defined session with `${IP}:${PORT}__${SESSION_KEY}`
> 
> `SESSION_KEY` is integer value which starts with 0, 
> and update when same IP has been last accessed 30 minutes ago(defined on wiki) 

Result looks like:
```
+------------------------+-----+
|session                 |count|
+------------------------+-----+
|112.196.25.164:55986__0 |1946 |
|112.196.25.164:42792__0 |1752 |
|112.196.25.164:37516__0 |1429 |
|106.51.132.54:5048__0   |414  |
|106.51.132.54:4508__0   |303  |
|88.198.69.103:47828__0  |239  |
|78.46.60.71:58504__0    |237  |
|106.51.132.54:5049__0   |235  |
...
...
```

2.Determine the average session time
> I've defined `max(timestamp) - min(timestamp)` value for each session, and get average value of all.

Result looks like:
```
+-----------------+
|      avgDuration|
+-----------------+
|1.635632908837669|
+-----------------+
``` 

3.Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
> I've used url with query parsed to use it as identifier
> For example if raw url is `https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null`, I've converted with `https://paytm.com:443/shop/authresponse`.
>  
> Not sure about this, but thought it will be better for the concept of this question.

Result looks like:
```
+------------------------+----------+
|session                 |uniqueUrls|
+------------------------+----------+
|106.51.132.54:5048__0   |287       |
|88.198.69.103:47828__0  |239       |
|78.46.60.71:58504__0    |237       |
|213.239.204.204:35094__0|234       |
|106.51.132.54:5049__0   |233       |
|106.51.132.54:5037__0   |205       |
|106.51.132.54:4508__0   |178       |
...
...
``` 

4.Find the most engaged users, ie the IPs with the longest session times
> Like question number 2, I've defined I've defined `max(timestamp) - min(timestamp)` value for each session.

Result looks like:
```
+----------------------+--------+
|session               |duration|
+----------------------+--------+
|106.186.23.95:35626__0|66.511  |
|106.186.23.95:35632__0|66.511  |
|106.186.23.95:35629__0|66.511  |
|106.186.23.95:39639__0|66.5    |
|106.186.23.95:40598__0|66.5    |
|106.186.23.95:39646__0|66.5    |
|106.186.23.95:39172__0|66.5    |

```

### Tools:
- Scala 2.12
- Spark 2.4

### How to:
1. Run project
```
$ sbt package
...
[success] Total time:...
$ spark-submit target/scala-2.12/daastest_2.12-0.1.0-SNAPSHOT.jar
```
2. Check result files at `/result` directory.

### Additional notes:
- You are allowed to use whatever libraries/parsers/solutions you can find provided you can explain the functions you are implementing in detail.
- IP addresses do not guarantee distinct users, but this is the limitation of the data. As a bonus, consider what additional data would help make better analytical conclusions
- For this dataset, complete the sessionization by time window rather than navigation. Feel free to determine the best session window time on your own, or start with 15 minutes.
- The log file was taken from an AWS Elastic Load Balancer:
http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html#access-log-entry-format


## How to complete this challenge:

1. Fork this repo in github
2. Complete the processing and analytics as defined first to the best of your ability with the time provided.
3. Place notes in your code to help with clarity where appropriate. Make it readable enough to present to the PayPay interview team.
4. Include the test code and data in your solution. 
5. Complete your work in your own github repo and send the results to us and/or present them during your interview.

## What are we looking for? What does this prove?

We want to see how you handle:
- New technologies and frameworks
- Messy (ie real) data
- Understanding data transformation
This is not a pass or fail test, we want to hear about your challenges and your successes with this particular problem.
