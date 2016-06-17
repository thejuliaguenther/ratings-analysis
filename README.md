# Ratings Analytics 

Ratings Analytics uses Apache Spark to process a 20 million line movie ratings file. Designed for data geeks, Ratings Analytics provides an organic user experience for viewing insights about movie ratings.

## Technical Stack 
Apache Spark, Spark SQL, Redis, Python, Flask, JavaScript, D3.js, C3.js, jQuery, Ajax, HTML/CSS, Bootstrap

## Data
The data for this visualization comes from [grouplens.org](http://grouplens.org).
The original source of this data is the movie recommendation service [MovieLens](http://movielens.org). Here is the breakdown of the data:
- Total movie ratings: 20,000,263
- Number of movies rated: 27,278 
- Number of anonymized users who rated movies: 138,493 
- Time frame within which movies were rated: January 9, 1995 and March 31, 2015

The ratings range from 0.0-5.0 with increments of 0.5. The tags for each movie were submitted as free-text by the users.

The users represented in this data set are randomly selected users that have rated at least 20 movies and are identified only by their anonymous user id; no other user information is provided. 

The csv files containing the data are not contained in this repo due to their size.

## Features
### **Data Processing in Apache Spark with Spark SQL**
- This application uses [Apache Spark](http://spark.apache.org/) to process the 20 million line movie ratings file. 
- Unlike Hadoop, which uses processes data on the hard disk, Spark processes data in RAM; this allows spark to process data up to 100 times faster than Hadoop MapReduce. Thus, Spark is an excellent choice for processing large amounts of data quickly. 
- The data is organized into separate scemas for each CSV file using Spark SQL. Using Spark SQL not only facilitates the use of SQL queries to access the data but also provides information about the structure of the data that allows Spark to perform additional optimizations not included in the basic Spark RDD.
- The dataset is static and the source of the data processed in Spark is several CSV's (movies.csv, tags.csv, ratings.csv). Therefore the Spark computations were performed once to create the JSON files necessary to seed Redis and are not performed each time the application is run, greatly improving speed.
- Used [Spark CSV](https://github.com/databricks/spark-csv) to reduce engineering time spent parsing a CSV (side note: [click here](https://github.com/thejuliaguenther/csv-parser) to see a generic CSV-parser class written in Python).

### **Autocomplete Search Bar**
![autocomplete](https://cloud.githubusercontent.com/assets/13442273/15460656/220bc92a-2068-11e6-8fd0-87815a82e8b6.png)
- Implemented autocomplete using [jQuery-Autocomplete](https://github.com/devbridge/jQuery-Autocomplete) and an AJAX call to get the appropriate JSON 

### **Movies Arranged in Alphabetical Order**
![movies by title](https://cloud.githubusercontent.com/assets/13442273/15460584/61e946f4-2067-11e6-809f-ffe340b304f3.png)
- Used Redis to store all of the movie titles stored in alphabetical order, ignoring the articles "A", "An", and "The"; these movie titles link to the page showing analytics for that movie
- Implemented buttons to view the movies starting with a certain letter

### **D3.js Word Cloud and Pie Chart with Tooltips**
![pie chart and word cloud](https://cloud.githubusercontent.com/assets/13442273/15460561/1c5c276e-2067-11e6-93d2-143c6e5f7979.png)
- Word cloud shows the unique tags that users assigned to each movie; the tags are associated with the appropriate movie id in Apache Spark
- Pie Chart contains the breakdown of each movie rating for the movie; the chart contains tooltips that show the rating and the total count of that rating (i.e. Rating: 5.0, Count: 100)

### **C3.js Time-series Graph**
![response time series](https://cloud.githubusercontent.com/assets/13442273/15460488/61fea6c6-2066-11e6-8193-0e7460706139.png)
- Shows the trend in movie ratings per month          
- Tooltip shows the number of movie ratings per month 

### **Redis Storage**
- This application uses Redis as a NoSQL database as it allows for fast lookup times. 