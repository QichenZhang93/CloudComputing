#!/bin/bash
######################################################################
# Answer script for Project 1 module 2                             ###
# Fill  in the functions below for each question.                  ###
# You may use any other files/scripts/languages                    ###
# in these functions as long as they are in the submission folder. ###
######################################################################


# The filtered data should be put in a file named ‘output’


# How many lines emerged in your output files?
# Run your commands/code to process the output file and echo a
# single number to standard output
q1() {
	# Write a function to get the answer to Q1. Do not just echo the answer.
	wc -l ./output | cut -d' ' -f1
}

# What was the most popular article in the filtered output? How many total views
# did the most popular article get over the month?
# Run your commands/code to process the output and echo <total views>\t<article_name>
# to standard output.
q2() {
	# Write a function to get the answer to Q2. Do not just echo the answer.
	awk 'FNR==1{print $1,"\t",$2}' ./output
}

# What was the most popular article on May 18, 2016 from the filtered output?
# How many daily views did the most popular article get on May 18? (Col 19)
# Run your commands/code to process the output and echo <article_name>\t<daily views>
# to standard output
q3() {
    # Write a function to get the answer to Q3. Do not just echo the answer.
    javac q3.java
    java q3 ./output
}

# What is the most popular article of May 2016 with ZERO views on May 1, 2016? (col 2)
# Run your commands/code to process the output and echo the answer
q4() {
    # Write a function to get the answer to Q4. Do not just echo the answer.
    javac q4.java
    java q4 ./output
}


# For how many days over the month was the page titled "Facebook" more popular
# than the page titled "Google" ?
# Run your commands/code to process the dataset and echo a single number to standard output
# Do not hardcode the articles, as we will run your code with different articles as input
# For your convenience, "Facebook" is stored in the variable 'first', and "Google" in 'second'.
q5() {
	# do not change the following two lines
	first=$(head -n 1 q5) #Facebook
	second=$(cat q5 | sed -n 2p) #Google
	# Write a function to get the answer to Q5. Do not just echo the answer.
    javac q5.java
	java q5 ./output $first $second
}



# Rank the movie titles in the file q6 based on their maximum single-day wikipedia page views
# (In descending order of page views, with the highest one first):
# X-Men_(film),Warcraft_(film),Suicide_Squad_(film),Star_Wars_(film),Jason_Bourne_(film)
# Ensure that you print the answers comma separated (As shown in the above line) without spaces
# For your convenience, code to read the file q6 is given below. Feel free to modify.
q6() {
	# Write a function to get the answer to Q6. Do not just echo the answer.
	#while read line
	#do
	#    movie=$line
#		echo "$movie,"
	#done < q6
    
    ## file's format in project 1.1
    javac q6.java
    java q6 ./q6 ./output
}

# Rank the operating systems in the file q7 based on their total month views page views
# (In descending order of page views, with the highest one first. In descending alphabetical order by name
# if the pageviews are same;
# OS_X,Windows_10,Linux
# Ensure that you print the answers comma separated (As shown in the above line)
# For your convenience, code to read the file q7 is given below. Feel free to modify.
q7() {
	# Write a function to get the answer to Q7. Do not just echo the answer.
	#while read line
    #do
    #	os=$line
    #	echo "$os,"
    #done < q7
    javac q7.java
    java q7 ./q7 ./output
}

# When did the article "NBA_Finals" have the most number of page views?
# Input the answer in yyyymmdd format
# Run your commands/code to process the output and echo the answer
# in the above format to standard output
q8() {
	# Write a function to get the answer to Q8. Do not just echo the answer.
    javac q8.java
	java q8 ./output
}

# Find out the number of articles with longest number of strictly decreasing sequence of views
# Example: If 21 articles have strictly decreasing pageviews everyday for 5 days (which is the global maximum),
# then your script should find these articles from the output file and return 21.
# Run your commands/code to process the output and echo the answer
q9() {
	# Write a function to get the answer to Q8. Do not just echo the answer.
    javac q9.java
	java q9 ./output
}



######################################################################
##               DO NOT MODIFY ANYTHING BELOW                       ##
######################################################################
question_num=9
unset -v question_index

while getopts ":hs:" opt; do
    case $opt in
        h)
            echo "This program is used to execute your solution." >&2
            echo "Usage:" >&2
            echo "./runner.sh to run all the questions" >&2
            echo "./runner.sh -s <question_index> to run one single question" >&2
            echo "Example:" >&2
            echo "./runner.sh -s 1 to run q1" >&2
            exit
            ;;
        s)
            question_index=$OPTARG
            if [ "$question_index" -lt 1 ] || [ "$question_index" -gt "$question_num" ]
            then
                echo "Invalid question index"
                exit 2
            else
                answer=$(q"$question_index")
                echo -en "$answer"
                echo ""
            fi
            exit
            ;;
        \?)
            echo "Invalid option: -$OPTARG"  >&2
            exit 1
            ;;
    esac
done

if [ -z "$1" ]
then
    echo "The answers generated by executing your solution are: "
    echo "{"

    for question_index in $(seq 1 $question_num);
    do
        answer=$(q"$question_index")
        echo -en ' '\"answer"$question_index"\": \""$answer"\"
        if [ "$question_index" -eq "$question_num" ]
        then
            echo ""
        else
            echo ","
        fi
    done

    echo  "}"

    echo ""
    echo "If you feel these values are correct please run:"  >&2
    echo "./submitter -a <andrewId> -l <language>"  >&2
    echo "./submitter -a msakr -l java"
    echo "Alternatively, run ./submitter -h for a usage guide."  >&2
else
    echo "Invalid usage"
    exit 3
fi
