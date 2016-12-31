#!/bin/bash

######################################################################
##            15-319/619 Cloud Computing Course                     ##
##           Answer Script for Project 3 Module 1                   ##
######################################################################

######################################################################
#                              Bash                                ###
######################################################################

# Answer question 1-6 by completing the bash commands inline.
# Standard output format will be given for each question, you must follow the format strictly.
# The colon ":" is a POSIX built-in basically equivalent to the "true" command, REPLACE it with your own command in each function.

# WARNING: Questions should be done using bash scripting only in this section. 
# Do NOT use other programming languages such as Java and Python, otherwise you will lose all the marks in this section.

# Question 1
# How many rows in million_songs_metadata.csv contain 'Aerosmith' (Case sensitive)?

# Note: You MUST use grep instead of awk for this question.
# Standard output format: <number>
answer_1() {
	grep 'Aerosmith' million_songs_metadata.csv | wc -l
}

# Question 2
# What is the total number of track_id(s) in million_songs_metadata.csv with
# artist_name containing "Bob Marley" (Case sensitive)?

# Note: You MUST use grep instead of awk for this question.
# It is quite challenging (but feasible) to just use grep without other bash commands
# You can also use cat, cut, wc, etc. piped with grep to make it easier
# Standard output format: <number>
answer_2() {
	cut -d ',' -f7 million_songs_metadata.csv | grep '.*Bob Marley.*' | wc -l
}

# Question 3
# How many rows in million_songs_metadata.csv contain 'The Beatles' (Case sensitive) on column 7?

# Note: q3 is the same as q2 in essence, so you SHOULD use awk this time.
# Standard output format: <number>
answer_3() {
	awk -v cnt=0 'BEGIN {FS=","}; $7~/.*The Beatles.*/ { print $7; };' million_songs_metadata.csv | wc -l
}

# Question 4
# Write commands to do the equivalent of the SQL query "SELECT AVG(duration) FROM songs;".

# Note: You can use either awk or grep for this question.
# Standard output format: <number>
answer_4() {
	 awk -v total=0 -v cnt=0 'BEGIN{FS=","}; {total+=$8; ++cnt;}; END{print total/cnt;}' million_songs_metadata.csv
}

# Question 5
# Merge the two original csv files into the file named `million_songs_metadata_and_sales.csv` in the current folder.

# Standard output format: No standard output needed
answer_5() {
	join -t ',' -j 1 -o '1.1,2.2,2.3,1.2,1.3,1.4,1.5,1.6,1.7,1.8,1.9,1.10,1.11' million_songs_metadata.csv million_songs_sales_data.csv >> million_songs_metadata_and_sales.csv
}

# Question 6
# Find the artist with maximum sales using the file million_songs_metadata_and_sales.csv.
# The output of your command(s) should be the artist name.

# Hint: Artists can have MANY different artist_names, but only ONE artist_id,
# which is unique to each artist. You should find the maximum sales based
# on artist_id, and return any of that artist_id’s valid artist_name as the result.
# Any correct artist_name is fine to get the score

# Standard output format: <artist_name>
answer_6() {
	sort -t ',' -k 7 million_songs_metadata_and_sales.csv | awk -v curMax=0 -v maxArId='' -v maxArNm='' -v curNum=0 -v curId='' -v curNm='' 'BEGIN{FS=","}; { if (curId!=$7) {if (curNum>curMax) { curMax=curNum; maxArNm=curNm; maxArId=curId; } curNm=$9; curId=$7; curNum=$3;} else { curNum+=$3 } }; END{if (curNum>curMax) { curMax=curNum; maxArNm=curNm; maxArId=curId; } print maxArNm}'
}

######################################################################
#                             MySQL                                ###
######################################################################

# Answer question 7-11 by completing the corresponding method(s) in MySQLTasks.java.

# Qustion 7
# Write a SQL query that returns the track_id of the song with the maximum duration.
# Complete the corresponding method(s) in MySQLTasks.java.
answer_7() {
    export id='qichenz'
    export pw='db15319root'
    java -jar MySQLTasks.jar q7
}

# Question 8
# A database index is a data structure that improves the speed of data retrieval.
# Identify the field that will improve the performance of query in question 7
# and create a database index on that field.
# Complete the corresponding method(s) in MySQLTasks.java.
INDEX_NAME="idx"
answer_8() {
    export id='qichenz'
    export pw='db15319root'
	java -jar MySQLTasks.jar q8 $INDEX_NAME
}

# Question 9
# Write a SQL query that returns the trackid of the song with the maximum duration
# This is the same query as Question 7. Do you see any difference in performance?
# Complete the corresponding method(s) in MySQLTasks.java.
answer_9() {
    export id='qichenz'
    export pw='db15319root'
	java -jar MySQLTasks.jar q9
}

# Question 10
# Write the SQL query that is equivalent to the command grep -P 'The Beatles' million_songs_metadata.csv | wc -l.
# Complete the corresponding method(s) in MySQLTasks.java.
answer_10() {
    export id='qichenz'
    export pw='db15319root'
	java -jar MySQLTasks.jar q10
}

# Question 11
# Which artist has the third-most number of rows in table songs?
# The output should be the name of the artist.
# Please use artist_id as the unique identifier of the artist. If the artist_id has several artist_names associated,
# return any one of them.
# Complete the corresponding method(s) in MySQLTasks.java.
answer_11() {
    export id='qichenz'
    export pw='db15319root'
	java -jar MySQLTasks.jar q11
}

######################################################################
#                            HBase                                 ###
######################################################################

# Answer question 12-16 by completing the corresponding method(s) in HBaseTasks.java.

# Question 12
# What was that song whose name started with "Total" and ended with "Water"?
# Write an HBase query that finds the track that the person is looking for.
# The title starts with "Total" and ends with "Water", both are case sensitive.
answer_12() {
    java HBaseTasks q12
}

# Question 13
# I don't remember the exact title, it was that song by "Kanye West", and the
# title started with either "Apologies" or "Confessions". Not sure which...
# Write an HBase query that finds the track that the person is looking for.
# The artist_name contains "Kanye West", and the title starts with either
# "Apologies" or "Confessions" (Case sensitive).
answer_13() {
    java HBaseTasks q13;
}

# Question 14
# There was that new track by "Bob Marley" that was really long. Do you know?
# Write an HBase query that finds the track the person is looking for.
# The artist_name has a prefix of "Bob Marley", duration greater than 400,
# and year 2000 and onwards (Case sensitive).
answer_14() {
    java HBaseTasks q14;
}

# Question 15
# I heard a really great song about "Family" by this really cute singer,
# I think his name was "Consequence" or something...
# Write an HBase query that finds the track the person is looking for.
# The track has an artist_hotttnesss of at least 1, and the artist_name
# contains "Consequence". Also, the title contains "Family" (Case sensitive).
answer_15() {
    java HBaseTasks q15;
}

# Question 16
# Hey what was that "Love" song that "Gwen Guthrie" came out with in 1990?
# No, no, it wasn't the sad one, nothing "Bitter" or "Never"...
# Write an HBase query that finds the track the person is looking for.
# The track has an artist_name prefix of "Gwen Guthrie", the title contains "Love"
# but does NOT contain "Bitter" or "Never".
answer_16() {
    java HBaseTasks q16;
}


##############################################
### DO NOT MODIFY ANYTHING BELOW THIS LINE ###
##############################################
echo "{"

if [ -z "$1" ] || [ "$1" == "files" ]
then

    echo -en ' '\"answer1\": \""$(answer_1)"\"
    echo ","

    echo -en ' '\"answer2\": \""$(answer_2)"\"
    echo ","

    echo -en ' '\"answer3\": \""$(answer_3)"\"
    echo ","

    echo -en ' '\"answer4\": \""$(answer_4)"\"
    echo ","

    answer_5 &> /dev/null
    if [ -f 'million_songs_metadata_and_sales.csv' ]
    then
        echo -en ' '\"answer5\": \"'million_songs_metadata_and_sales.csv' file created\"
        echo ","
    else
        echo -en ' '\"answer5\": \"'million_songs_metadata_and_sales.csv' file not created\"
        echo ","
    fi

    echo -en ' '\"answer6\": \""$(answer_6)"\"
    if [ -z "$1" ]
    then
        echo ","
    fi
fi

if [ -z "$1" ] || [ "$1" == "mysql" ]
then
    javac MySQLTasks.java > /dev/null

    # Disable caching
    mysql --skip-column-names --batch -pdb15319root song_db -e "SET GLOBAL query_cache_size = 0" &> /dev/null

    # Drop any existing index
    INDEX_FIELD=$(mysql --skip-column-names --batch -pdb15319root song_db -e "DESCRIBE songs" 2> /dev/null | grep MUL | cut -f1)
    if [ "$INDEX_FIELD" ]
    then
        mysql --skip-column-names --batch -pdb15319root song_db -e "DROP INDEX $INDEX_NAME ON songs" 2> /dev/null
    fi
    START_TIME=$(date +%s.%N)
    TID=$(answer_7 | tail -1)
    END_TIME=$(date +%s.%N)
    RUN_TIME=$(echo "$END_TIME - $START_TIME" | bc)
    echo -en ' '\"answer7\": \""$TID","$RUN_TIME"\"
    echo ","

    answer_8 > /dev/null
    INDEX_FIELD=$(mysql --skip-column-names --batch -pdb15319root song_db -e "DESCRIBE songs" 2> /dev/null | grep MUL | cut -f1)
    echo -en ' '\"answer8\": \""$INDEX_FIELD"\"
    echo ","

    START_TIME=$(date +%s.%N)
    TID=$(answer_9 | tail -1)
    END_TIME=$(date +%s.%N)
    RUN_TIME=$(echo "$END_TIME - $START_TIME" | bc)
    echo -en ' '\"answer9\": \""$TID","$RUN_TIME"\"
    echo ","

    echo -en ' '\"answer10\": \""$(answer_10)"\"
    echo ","

    echo -en ' '\"answer11\": \""$(answer_11)"\"
    if [ -z "$1" ]
    then
        echo ","
    fi
    rm MySQLTasks.class > /dev/null
fi

if [ -z "$1" ] || [ "$1" == "hbase" ]
then
    javac HBaseTasks.java > /dev/null
    echo -en ' '\"answer12\": \""$(answer_12)"\"
    echo ","

    echo -en ' '\"answer13\": \""$(answer_13)"\"
    echo ","

    echo -en ' '\"answer14\": \""$(answer_14)"\"
    echo ","

    echo -en ' '\"answer15\": \""$(answer_15)"\"
    echo ","

    echo -en ' '\"answer16\": \""$(answer_16)"\"
    rm HBaseTasks.class > /dev/null
fi
echo
echo  "}"
