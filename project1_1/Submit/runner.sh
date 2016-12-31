#!/bin/bash

######################################################################
##            15-319/619 Cloud Computing Course                     ##
##           Answer Script for Project 1 Module 1                   ##
######################################################################

# Hint: 
# Fill in the functions below for each question. You may use any programming language(s) in any question.
# You may use other files or scripts in these functions as long as they are in the submission folder. All files must include source (do not just submit jar or pyc files)
# Standard output format will be given for each question, you must follow the format strictly.
# The colon ":" is a POSIX built-in basically equivalent to the "true" command, replace it with your own command in each function.

######################################################################
##                   Data Pre-processing                            ##
######################################################################

q0() {
    # Fill this function to filter the dataset and redirect the output to a file called "output".
    
    # Examples:
    # ./data_filter.sh
    # python data_filter.py
    # java DataFilter

    # Stardard output format: No standard output needed
    java -jar ./Datafilter.jar
}

######################################################################
##                     Original Dataset                             ##
######################################################################

# Hint: 
# You should write a function to get the answer to each question.
# Do not just echo/print the answer.

q1() {
    # How many lines were originally present in pagecounts-20160501-000000 before filtering?
    
    # Stardard output format: <number>
    wc -l ./pagecounts-20160501-000000 | cut -d' ' -f1
}

q2() {
    # Before filtering, what was the total number of requests made to all
    # of wikipedia (all subprojects, all elements, all languages, etc.) during
    # the hour stored in pagecounts-20160501-000000?
    
    # Hint: 
    # Add up all the <count_views> that appear in the original file.
    # Note that some lines don't have four columns. However, all the records have <count_views>. Try to handle all possible cases gracefully.       
    # Do not do any filtering.
    
    # Stardard output format: <number>
   java -jar Analyzer.jar 2 ./pagecounts-20160501-000000
}

######################################################################
##                     Filtered Output                              ##
######################################################################

q3() {
    # What was the most popular article in the filtered output?

    # Hint: 
    # You don't need to worry about breaking ties.
    
    # Stardard output format: <title>
    java -jar Analyzer.jar 3 ./output
}

q4() {
    # How many views are there in the filtered output for the most popular movie?
    
    # Hint: 
    # Entries for movies contain "film" as a substring in the article name (case sensitive).
    # You don't need to worry about breaking ties.
    
    # Stardard output format: <number>
    java -jar Analyzer.jar 4 ./output
}

# Hint: 
# Please remember that your output format should remain consistent, even on corner cases.
# If there is no entry that satisfies the filtering requirements in Q5-Q8, your code should return 0 rather than nothing.

q5() {
    # How many articles are there in the filtered output with titles that contain both "cloud" and "computing"?         
    # Both substrings are case insensitive.
    
    # Stardard output format: <number>
    java -jar Analyzer.jar 5 ./output
}

q6() {
    # How many views are there in the filtered output for all articles with titles
    # that start with a single digit (0-9) and then an English character (either uppercase or lowercase [a-zA-z])?
    
    # Valid example:        
    # 3D_animation  2
    
    # Invalid example: 
    # 88th_New_York_Infantry    2
    
    # Stardard output format: <number> 
    java -jar Analyzer.jar 6 ./output
}

q7() {
    # How many films are there in the filtered output that also have a corresponding TV series?
    # Films are named <article_name>_([year_]film)
    # TV_series are named <article_name>_([year_]TV_series)
    # 1. The <article_name> must be identical(case sensitive) in both the film and TV_series
    # 2. It is also valid if the "film" or "TV_series" have a 4-digit year and an underscore as prefix
    # 3. It is NOT valid if the "film" or "TV_series" have any other string as prefix, e.g. 1979_Animated_TV_film, fictional_film, U.S._TV_series
    
    # Hint: 
    # There might be duplicate names such as "Concussion_(2015_film)" and "Concussion_(film)".
    # If there is one film sharing the same name with multiple TV series, you should only count it once.
    # If there are two films sharing the same name with one TV series, you should count it twice.
    # If there are multiple films sharing the same name with multiple TV series, your count should be based on the number of films.
    
    # Examples of valid cases:
    # "Concussion_(2015_film)" is a match for "Concussion_(TV_series)"
    # "Scream_Queens_(2015_TV_series)" is a match for "Scream_Queens_(1929_film)"
    
    # Examples of invalid cases:
    # "Concussion_(2015_film_series)" is not a match for "Concussion_(TV_series)"
    # "Scream_Queens_(U.S._TV_series)" is not a match for "Scream_Queens_(1929_film)"
    
    # Stardard output format: <number>
    java -jar Analyzer.jar 7 ./output
}

q8() {
    # How many articles are there in the filtered dataset with titles that include
    # ** EXACTLY ** "cloud" as a word instead of a substring (case insensitive)?
    
    # Hint:
    # The word "cloud" can be concatenated with "-", "_", and even numbers.
    # Only [a-zA-Z] is not allowed to concatenate with "cloud"
    
    # Valid examples:       
    # Cloud_computing 122
    # Charlie_St._Cloud_(film)        86
    # Cumulonimbus_cloud      31
    
    # Invalid examples:         
    # SoundCloud    43
    # ICloud        29
    # A_Walk_in_the_Clouds  14
    # No_More_Cloudy_Days   1
    
    # Stardard output format: <number>      
    java -jar Analyzer.jar 8 ./output
}

######################################################################
##               DO NOT MODIFY ANYTHING BELOW                       ##
######################################################################
question_num=8
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
            if [ "$question_index" -lt 0 ] || [ "$question_index" -gt "$question_num" ] 
            then
                echo "Invalid question index"
                exit 2
            else
                if [ "$question_index" -eq 0 ]
                then
                    rm -f output && q0 &> /dev/null
                else                    
                    answer=$(q"$question_index")
                    echo -en "$answer"
                    echo ""
                fi
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

    rm -f output && q0 &> /dev/null
    if [ -f 'output' ]
    then
            echo -en ' '\"answer0\": \"'output' file created\"
    else
            echo -en ' '\"answer0\": \"No 'output' file created\"
    fi
    echo ","

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
    echo "./submitter -a <andrewId>"  >&2
    echo "Alternatively, run ./submitter -h for a usage guide."  >&2
else
    echo "Invalid usage"
    exit 3
fi
