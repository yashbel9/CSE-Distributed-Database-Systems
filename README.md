# Distributed-Database-SystemS
This repository includes all the assignments for the course Distributed Database Systems.

# Assignment 1

 - Implement a Python function Load Ratings() that takes a file system path that contains the rating.dat file as input. Load Ratings() then load the rating.dat content into a table (saved in PostgreSQL) named Ratings that has a fixed schema.
 - Implement a Python function Range Partition() that takes as input: (1) the Ratings table stored in PostgreSQL and (2) an integer value N; that represents the number of partitions. Range Partition() then generates N horizontal fragments of the Ratings table and store them in PostgreSQL. The algorithm should partition the ratings table based on N uniform ranges of the Rating attribute.
 - Implement a Python function RoundRobin Partition() that takes as input: (1) the Ratings table stored in PostgreSQL and (2) an integer value N; that represents the number of partitions. The function then generates N horizontal fragments of the Ratings table and stores them in PostgreSQL. The algorithm should partition the ratings table using the round robin partitioning approach.
 - Implement a Python function RoundRobin Insert() that takes as input: (1) Ratings table stored in PostgreSQL, (2) UserID, (3) ItemID, (4) Rating. RoundRobin Insert() then inserts a new tuple to the Ratings table and the right fragment based on the round robin approach.
 - Implement a Python function Range Insert() that takes as input: (1) Ratings table stored in Post- greSQL (2) UserID, (3) ItemID, (4) Rating. Range Insert() then inserts a new tuple to the Ratings table and the correct fragment (of the partitioned ratings table) based upon the Rating value.

# Assignment 2

 - Implement a Python function RangeQuery that takes as input: (1) Ratings table stored in PostgreSQL, (2) RatingMinValue (3) RatingMaxValue (4) openconnection
 - RangeQuery() then returns all tuples for which the rating value is larger than or equal to RatingMinValue and less than or equal toRatingMaxValue. 
 - Implement a Python function PointQuery that takes as input: (1) Ratings table stored in PostgreSQL, (2) RatingValue. (3)openconnection
 - PointQuery() then returns all tuples for which the rating value is equal to RatingValue.
 
# Assignment 3

 - Implement a Python function ParallelSort() that takes as input: (1) InputTable stored in a PostgreSQL database, (2) SortingColumnName the name of the column used to order
the tuples by. ParallelSort() then sorts all tuples (using five parallelized threads) and stores the sorted tuples for in a table named OutputTable (the output table name is passed to the function). The OutputTable contains all the tuple present in InputTable sorted in ascending order.
 - Implement a Python function ParallelJoin() that takes as input: (1) InputTable1 and InputTable2 table stored in a PostgreSQL database, (2) Table1JoinColumn and Table2JoinColumn that represent the join key in each input table respectively. ParallelJoin() then joins both InputTable1 and InputTable2 (using five parallelized threads) and stored the resulting joined tuples in a table named OutputTable (the output table name is passed to the function). The schema of OutputTable should be similar to the schema of both InputTable1 and InputTable2 combined.

# Assignment 4

- The code is implemented in Java and would take two inputs, one would be the HDFS location of the file on which the equijoin should be performed and other would be the HDFS location of the file, where the output should be stored.

# Assignment 5
- FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection)
This function searches the ‘collection’ given to find all the business present in the city provided in ‘cityToSearch’ and save it to ‘saveLocation1’. For each business you found, you should store name Full address, city, state of business in the following format.
- indBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection)
This function searches the ‘collection’ given to find name of all the business present in the ‘maxDistance’ from the given ‘myLocation’ that covers all the given categories (please use the distance algorithm given below) and save them to ‘saveLocation2’. Each
line of the output file will contain the name of the business only.
