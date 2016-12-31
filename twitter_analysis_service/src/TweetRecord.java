public class TweetRecord {
        String tweetID;
        long impactScore = 0;
        String createTime;
        String slimText;
        String censorText;
        
        public TweetRecord(String tweet, long impact, String time, String text, String censor) {
            tweetID = tweet;
            impactScore = impact;
            createTime = time;
            slimText = text;
            censorText = censor;
            if (tweet == null) tweet = "";
            if (createTime == null) createTime = "";
            if (slimText == null) slimText = "";
            if (censorText == null) censorText = "";
        }
}

class WordCount {
    int count1;
    int count2;
    public WordCount(int num1, int num2) {
        count1 = num1;
        count2 = num2;
    }
}
