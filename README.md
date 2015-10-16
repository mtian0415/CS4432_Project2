CS4432 Project 2
Yuchen Liu
Mi Tian

Task 1, 2, and 3 - Yuchen Liu



Task 4 and 5 (Smart Merge-Sort-Join) - Mi Tian

Steps to run this part:
1. Run "StartUp" class in src/simpledb.server to start the db server.
2. Run "CreateTable" class in studentClient/simpledb/smartmergesortjoin to create and insert data into the two tables.
3. Run "RunQuery" class in studentClient/simpledb/smartmergesortjoin to run the merge-sort-join query.
4. It may take several minutes. Wait until it finishes and see the print out.

The goal of task 4 and 5 is to increase the efficiency of the merge-sort-join query in simpledb. The original simpledb always sort two tables first, then merge and join them. Even if the tables have been sorted previously, it will still sort both of them, because the sorted tables are never written back to the original tables. In our project, we modified the related code to write the sort results back to the table, and mark the table as sorted, so that simpledb will not sort it again next time. 

Our test result shows the time is greatly reduced with our new smart merge-sort-join code. For two tables of 10000 records, merge-sort-join in the first time (unsorted tables) took 131.495 seconds. The same query took only 1.411 seconds in the second time. This is almost 100 times faster!

To implement this, I first modified the TableInfo class to add a "isSorted" variable, and wrote get and set functions to get that information. I then changed the open() function in SortPlan.java. Instead of directly split and sort the table, it will check if the table is already sorted by calling the get() function. It only sort the table if it's not sorted already.

The open() function returns a SortScanExtend. This is extended from the original SortScan. It has a tableInfo variable to point to the original table. In every function, it checks first if the table is already sorted. If yes, it will get from the original table directly. If not, it will call the super class (SortScan) to do the sort. The next() function is very important, because it also writes the sorted result back to the original table. It also tells the MetaDataManager class to update the table's information to mark it as sorted. If you read the TableMgr class, you will see that every table now has a isSorted field to store that information.

We also wrote a new ExploitSortQueryPlanner to test our new MergeJoinPlan. We changed the SimpleDB class and RemoteStatementImp class to use this planner instead of the original BasicQueryPlanner. In addition to that, we also wrote two simple client programs: CreateTable.java and RunQuery.java. The first one creates two tables of 10000 rows. You can change it to test more rows, but it will take a long time to run. The second one runs a simple merge-join query two times (first time is not sorted and second time is sorted). It also calculates the time spent to compare the two runs.

The test result is shown in Task4And5TestResult.txt.