package eu.unitn.disi.db.spleetter;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.api.java.record.operators.CoGroupOperator;
import eu.stratosphere.api.java.record.operators.JoinOperator;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.StringValue;
import eu.unitn.disi.db.spleetter.cogroup.EnglishDictionaryCoGroup;
import eu.unitn.disi.db.spleetter.cogroup.HashtagPolarityCoGroup;
import eu.unitn.disi.db.spleetter.join.DictionaryFilterJoin;
import eu.unitn.disi.db.spleetter.join.HashtagLifespanJoin;
import eu.unitn.disi.db.spleetter.join.HashtagPolarityJoin;
import eu.unitn.disi.db.spleetter.join.HashtagUserJoin;
import eu.unitn.disi.db.spleetter.join.TweetDateJoin;
import eu.unitn.disi.db.spleetter.join.TweetPolarityJoin;
import eu.unitn.disi.db.spleetter.map.CleanTextMap;
import eu.unitn.disi.db.spleetter.map.LoadDictionaryMap;
import eu.unitn.disi.db.spleetter.map.LoadHashtagMap;
import eu.unitn.disi.db.spleetter.map.LoadTweetDatesMap;
import eu.unitn.disi.db.spleetter.map.LoadTweetMap;
import eu.unitn.disi.db.spleetter.map.PolarityHashtagExtractMap;
import eu.unitn.disi.db.spleetter.map.SentimentAnalysisMap;
import eu.unitn.disi.db.spleetter.map.SpamFlagMap;
import eu.unitn.disi.db.spleetter.map.SplitSentenceMap;
import eu.unitn.disi.db.spleetter.map.UserExtractMap;
import eu.unitn.disi.db.spleetter.map.UserTweetExtractMap;
import eu.unitn.disi.db.spleetter.reduce.CountAllHashtagTweetsReduce;
import eu.unitn.disi.db.spleetter.reduce.CountEnglishWordsReduce;
import eu.unitn.disi.db.spleetter.reduce.CountHashtagTweetsReduce;
import eu.unitn.disi.db.spleetter.reduce.CountHashtagUsersReduce;
import eu.unitn.disi.db.spleetter.reduce.CountUserTweetsReduce;
import eu.unitn.disi.db.spleetter.reduce.CountWordsAppearancesReduce;
import eu.unitn.disi.db.spleetter.reduce.HashtagFirstAppearanceReduce;
import eu.unitn.disi.db.spleetter.reduce.HashtagLastAppearanceReduce;
import eu.unitn.disi.db.spleetter.reduce.HashtagLowsReduce;
import eu.unitn.disi.db.spleetter.reduce.HashtagPeeksReduce;
import eu.unitn.disi.db.spleetter.reduce.SumHashtagPolarityReduce;
import java.util.Arrays;
import java.util.HashSet;

/**
 * Perform cleansing phase of tweets.
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class TweetCleanse implements Program, ProgramDescription {

    public static final String WORDS_TRESHOLD = "parameter.WORDS_TRESHOLD";
    public static final String APPEARANCE_TRESHOLD = "parameter.APPEARANCE_TRESHOLD";
    public static final String SENTIMENT_PATH = "parameter.SENTIMENT_PATH";

    /*
     * Profiling variables
     */
    public static final boolean LoadDictionaryMapLog          = true;  // LDM
    public static final boolean LoadTweetMapLog               = true;  // LTM
    public static final boolean LoadTweetDatesMapLog          = true;  // LTD
    public static final boolean LoadHashtagMapLog             = true;  // LHM
    public static final boolean EnglishDictionaryCoGroupLog   = true;  // EDCG
    public static final boolean CountEnglishWordsReduceLog    = true;   // CEWR
    public static final boolean CleanTextMapLog               = true;   // CTM
    public static final boolean DictionaryFilterJoinLog      = true;   // DFM
    public static final boolean SplitSentenceMapLog           = true;
    public static final boolean SentimentAnalysisMapLog       = true;   // SAM
    public static final boolean TweetPolarityJoinLog         = true ;  // TPM
    public static final boolean TweetDateJoinLog             = true;
    public static final boolean HashtagPolarityCoGroupLog     = true;
    public static final boolean HashtagPolarityJoinLog       = true;
    public static final boolean PolarityHashtagExtractMapLog  = true;
    public static final boolean UserExtractMapLog             = true;
    public static final boolean UserTweetExtractMapLog        = true;
    public static final boolean HashtagLifespanJoinLog       = true;
    public static final boolean HashtagUserJoinLog           = true;
    public static final boolean CountAllHashtagTweetsReduceLog = true;
    public static final boolean CountHashtagTweetsReduceLog    = true;
    public static final boolean CountHashtagUsersReduceLog     = true;
    public static final boolean CountUserTweetsReduceLog       = true;
    public static final boolean HashtagFirstAppearanceReduceLog = true;
    public static final boolean HashtagLastAppearanceReduceLog = true;
    public static final boolean HashtagLowsReduceLog           = true;
    public static final boolean HashtagPeeksReduceLog          = true;
    public static final boolean SumHashtagPolarityReduceLog    = true;
    public static final boolean CountWordsAppearancesReduceLog = true;
    public static final boolean SpamFlagMapLog                 = true;





    @Override
    public Plan getPlan(String... args) {
        int noSubTasks          = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
        String dataInput        = (args.length > 1 ? args[1] : "");
        String datesInput       = (args.length > 2 ? args[2] : "");
        String dictionaryInput  = (args.length > 3 ? args[3] : "");
        String sentimentData    = (args.length > 4 ? args[4] : "");
        String wordTreshold     = (args.length > 5 ? args[5] : "0.2");
        String hashtagInput     = (args.length > 6 ? args[6] : "");
        String appearanceTreshold     = (args.length > 7 ? args[7] : "1");


        String outputCleanTweets        = (args.length > 8 ? args[8] : "file:///tmp/") +"/clean_tweets";
        String outputUsersTweetsCount   = (args.length > 8 ? args[8] : "file:///tmp/") +"/users_tweets";
        String outputHashtagUsersCount  = (args.length > 8 ? args[8] : "file:///tmp/") +"/hashtag_users";
        String outputHashtagSentiment   = (args.length > 8 ? args[8] : "file:///tmp/") +"/hashtag_sentiment";
        String outputHashtagTweetsCount = (args.length > 8 ? args[8] : "file:///tmp/") +"/hashtag_tweets";
        String outputHashtagCount       = (args.length > 8 ? args[8] : "file:///tmp/") +"/hashtag_count";
        String outputHashtagLows        = (args.length > 8 ? args[8] : "file:///tmp/") +"/hashtag_lows";
        String outputHashtagPeeks       = (args.length > 8 ? args[8] : "file:///tmp/") +"/hashtag_peeks";
        String outputHashtagLifespan    = (args.length > 8 ? args[8] : "file:///tmp/") +"/hashtag_lifespan";
        String outputWordAppearances    = (args.length > 8 ? args[8] : "file:///tmp/") +"/words_count";
        String outputSpamTweets         = (args.length > 8 ? args[8] : "file:///tmp/") +"/spam_tweets";
//        int outputFilesCount = 9;


        /*
         * Load Data
         */

        FileDataSource tweets = new FileDataSource(TextInputFormat.class, dataInput, "Input Lines");
        tweets.setParameter(TextInputFormat.CHARSET_NAME, TextInputFormat.DEFAULT_CHARSET_NAME);	// comment out this line for UTF-8 inputs

        MapOperator tokenizeMapper = MapOperator.builder(LoadTweetMap.class)
                .input(tweets)
                .name("Tokenize Lines")
                .build();
        //tokenizeMapper.getCompilerHints().setAvgBytesPerRecord(105);
        //tokenizeMapper.getCompilerHints().setUniqueField(new FieldSet(0));
        //tokenizeMapper.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(new int[]{0, 1}), 1);



        FileDataSource dates = new FileDataSource(TextInputFormat.class, datesInput, "Tweet dates");
        dates.setParameter(TextInputFormat.CHARSET_NAME, TextInputFormat.DEFAULT_CHARSET_NAME);		// comment out this line for UTF-8 inputs

        MapOperator datesMapper = MapOperator.builder(LoadTweetDatesMap.class)
                .input(dates)
                .name("Tokenize Dates")
                .build();
        //datesMapper.getCompilerHints().setAvgBytesPerRecord(50);
        //datesMapper.getCompilerHints().setUniqueField(new FieldSet(0));
        //datesMapper.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(new int[]{0, 1}), 1);


        FileDataSource dict = new FileDataSource(TextInputFormat.class, dictionaryInput, "English words");
        dict.setParameter(TextInputFormat.CHARSET_NAME, TextInputFormat.DEFAULT_CHARSET_NAME);		// comment out this line for UTF-8 inputs

        MapOperator dictionaryMap = MapOperator.builder(LoadDictionaryMap.class)
                .input(dict)
                .name("Load dictionary")
                .build();
        //dictionaryMap.getCompilerHints().setAvgBytesPerRecord(10);
        //dictionaryMap.getCompilerHints().setUniqueField(new FieldSet(0));
        //dictionaryMap.getCompilerHints().setUniqueField(new FieldSet(0));


        FileDataSource hashtags = new FileDataSource(TextInputFormat.class, hashtagInput, "Hashtags");
        hashtags.setParameter(TextInputFormat.CHARSET_NAME, TextInputFormat.DEFAULT_CHARSET_NAME);		// comment out this line for UTF-8 inputs

        MapOperator loadHashtags = MapOperator.builder(LoadHashtagMap.class)
                .input(hashtags)
                .name("Load Hashtags")
                .build();

        //loadHashtags.getCompilerHints().setAvgBytesPerRecord(35);
        //loadHashtags.getCompilerHints().setUniqueField(new FieldSet(0));
        //loadHashtags.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(new int[]{0, 1}), 1);



        /*
         * Flow
         */


        JoinOperator datedTweets = JoinOperator.builder(TweetDateJoin.class, LongValue.class, 0,0)
                .keyField(LongValue.class, 1, 1)
                .input1(tokenizeMapper)
                .input2(datesMapper)
                .name("Join tweets and dates")
                .build();
        //datedTweets.getCompilerHints().setAvgRecordsEmittedPerFunctionCall(1.0f);
        //datedTweets.getCompilerHints().setAvgBytesPerRecord(130);

        MapOperator cleanText = MapOperator.builder(CleanTextMap.class)
                .input(datedTweets)
                .name("Clean Tweets")
                .build();
        //cleanText.getCompilerHints().setAvgRecordsEmittedPerFunctionCall(1.0f);

        MapOperator sentimentAnalysis = MapOperator.builder(SentimentAnalysisMap.class)
                .input(datedTweets)
                .name("Sentiment Analysis")
                .build();
        sentimentAnalysis.setParameter(SENTIMENT_PATH, sentimentData);
        //sentimentAnalysis.getCompilerHints().setAvgRecordsEmittedPerFunctionCall(1.0f);

        MapOperator spamMatcher = MapOperator.builder(SpamFlagMap.class)
                .input(datedTweets)
                .name("Spam Flagging")
                .build();


        MapOperator splitSentence = MapOperator.builder(SplitSentenceMap.class)
                .input(cleanText)
                .name("Split to words")
                .build();

        ReduceOperator countWordAppearances = ReduceOperator.builder(CountWordsAppearancesReduce.class, StringValue.class, 0)
                .input(splitSentence)
                .name("Count word appearances")
                .build();
        countWordAppearances.setParameter(APPEARANCE_TRESHOLD, appearanceTreshold);


        CoGroupOperator englishGroup = CoGroupOperator.builder(EnglishDictionaryCoGroup.class, StringValue.class, 0, 0)
                .input1(splitSentence)
                .input2(dictionaryMap)
                .name("Group en-words")
                .build();

        ReduceOperator countEnglishWords = ReduceOperator.builder(CountEnglishWordsReduce.class, LongValue.class, 0)
                .input(englishGroup)
                .name("Count en-words")
                .build();

        JoinOperator dictionaryFilter = JoinOperator.builder(DictionaryFilterJoin.class, LongValue.class, 0, 0)
                .input1(countEnglishWords)
                .input2(cleanText)
                .name("Filter English Tweets")
                .build();
        dictionaryFilter.setParameter(WORDS_TRESHOLD, wordTreshold);
        //dictionaryFilter.getCompilerHints().setAvgRecordsEmittedPerFunctionCall(0.2f);

        JoinOperator tweetPolarityJoin = JoinOperator.builder(TweetPolarityJoin.class, LongValue.class, 0, 0)
                .input1(dictionaryFilter)
                .input2(sentimentAnalysis)
                .name("Tweet Polarity Join")
                .build();

        MapOperator userExtract = MapOperator.builder(UserExtractMap.class)
                .input(tweetPolarityJoin)
                .name("Extract User")
                .build();

        ReduceOperator countUserTweets = ReduceOperator.builder(CountUserTweetsReduce.class, LongValue.class, 0)
                .input(userExtract)
                .name("Count user tweets")
                .build();



        MapOperator timePolarity = MapOperator.builder(PolarityHashtagExtractMap.class)
                .input(tweetPolarityJoin)
                .name("Tweet Time & Polarity")
                .build();

        MapOperator userTweetExtract = MapOperator.builder(UserTweetExtractMap.class)
                .input(tweetPolarityJoin)
                .name("Extract User")
                .build();


        JoinOperator hashtagUserJoin = JoinOperator.builder(HashtagUserJoin.class, LongValue.class, 0, 0)
                .keyField(IntValue.class, 1, 1)
                .input1(userTweetExtract)
                .input2(loadHashtags)
                .name("Hashtag User Join")
                .build();

        ReduceOperator countHashtagUsers = ReduceOperator.builder(CountHashtagUsersReduce.class, StringValue.class, 0)
                .keyField(IntValue.class, 1)
                .input(hashtagUserJoin)
                .name("Count hastag distinct users")
                .build();


        JoinOperator hashtagPolarityJoin = JoinOperator.builder(HashtagPolarityJoin.class, LongValue.class, 0, 0)
                .keyField(IntValue.class, 1, 1)
                .input1(timePolarity)
                .input2(loadHashtags)
                .name("Hashtag Polarity Join")
                .build();

        ReduceOperator sumHashtagPolarity = ReduceOperator.builder(SumHashtagPolarityReduce.class, StringValue.class, 0)
                .keyField(IntValue.class, 1)
                .input(hashtagPolarityJoin)
                .name("Sum Hashtag polarities")
                .build();

        ReduceOperator countHashtagTweets = ReduceOperator.builder(CountHashtagTweetsReduce.class, StringValue.class, 0)
                .keyField(IntValue.class, 1)
                .input(hashtagPolarityJoin)
                .name("Count Hashtag Tweets")
                .build();

        //NB reduce on key 1
        ReduceOperator countAllHashtagTweets = ReduceOperator.builder(CountAllHashtagTweetsReduce.class, IntValue.class, 1)
                .input(countHashtagTweets)
                .name("Count Hashtag Tweets")
                .build();


        CoGroupOperator timestampPolarityGroup = CoGroupOperator.builder(HashtagPolarityCoGroup.class, StringValue.class, 0, 0)
                .keyField(IntValue.class, 1,1)
                .input1(countHashtagTweets)
                .input2(sumHashtagPolarity)
                .name("Compute mean Divergence")
                .build();

        //NB reduce on key 1
        ReduceOperator hashtagPeeks = ReduceOperator.builder(HashtagPeeksReduce.class)
                .keyField( IntValue.class, 1)
                .input(countHashtagTweets)
                .name("Find Hashtags Peeks")
                .build();

        //NB reduce on key 1
        ReduceOperator hashtagLows = ReduceOperator.builder(HashtagLowsReduce.class)
                .keyField(IntValue.class, 1)
                .input(countHashtagTweets)
                .name("Find Hashtags Low")
                .build();


        //NB reduce on key 1
        ReduceOperator hashtagFirstAppearance = ReduceOperator.builder(HashtagFirstAppearanceReduce.class)
                .keyField(IntValue.class, 1)
                .input(countHashtagTweets)
                .name("Find Hashtag first Appearance")
                .build();


        //NB reduce on key 1
        ReduceOperator hashtagLastAppearance = ReduceOperator.builder(HashtagLastAppearanceReduce.class)
                .keyField(IntValue.class, 1)
                .input(countHashtagTweets)
                .name("Find Hashtag last Appearance")
                .build();

        JoinOperator hastagLifespanJoin = JoinOperator.builder(HashtagLifespanJoin.class, IntValue.class, 0, 0)
                .input1(hashtagFirstAppearance)
                .input2(hashtagLastAppearance)
                .name("Compose Hashtag Time Window")
                .build();



        /*
         * Output
         */


        //FileDataSink[] outputs = new FileDataSink[outputFilesCount];
        FileDataSink[] outputs = new FileDataSink[11];
        int i = 0;

        outputs[i] = new FileDataSink(new CsvOutputFormat(), outputCleanTweets, tweetPolarityJoin, "Pruned tweets with polarities");
        CsvOutputFormat.configureRecordFormat(outputs[i])
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(LongValue.class, 0)
                .field(IntValue.class, 1)
                .field(StringValue.class, 2)
                .field(IntValue.class, 3)
                .field(StringValue.class, 4)
                .field(DoubleValue.class, 5)
                .field(DoubleValue.class, 6);

        i++;
        outputs[i] = new FileDataSink(new CsvOutputFormat(), outputSpamTweets, spamMatcher, "Tweets marked as spam");
        CsvOutputFormat.configureRecordFormat(outputs[i])
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(LongValue.class, 0)
                .field(IntValue.class, 1)
                .field(IntValue.class, 2);

        i++;
        outputs[i] = new FileDataSink(new CsvOutputFormat(), outputUsersTweetsCount, countUserTweets, "User tweets count");
        CsvOutputFormat.configureRecordFormat(outputs[i])
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(IntValue.class, 0)
                .field(IntValue.class, 1);

        i++;
        outputs[i] = new FileDataSink(new CsvOutputFormat(), outputHashtagUsersCount, countHashtagUsers, "Hahtag Users count");
        CsvOutputFormat.configureRecordFormat(outputs[i])
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(StringValue.class, 0)
                .field(IntValue.class, 1)
                .field(IntValue.class, 2);

        i++; //timestampPolarityGroup
        outputs[i] = new FileDataSink(new CsvOutputFormat(), outputHashtagSentiment, timestampPolarityGroup, "Hahtag Polarities ");
        CsvOutputFormat.configureRecordFormat(outputs[i])
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(StringValue.class, 0)
                .field(IntValue.class, 1)
                .field(DoubleValue.class, 2)
                .field(DoubleValue.class, 3)
                .field(DoubleValue.class, 4)
                .field(DoubleValue.class, 5)
                .field(DoubleValue.class, 6)
                .field(DoubleValue.class, 7)
                .field(IntValue.class, 8);
        i++;
        outputs[i] = new FileDataSink(new CsvOutputFormat(), outputHashtagTweetsCount, countHashtagTweets , "Hahtag Tweets Count");
        CsvOutputFormat.configureRecordFormat(outputs[i])
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(StringValue.class, 0)
                .field(IntValue.class, 1)
                .field(IntValue.class, 2);

        i++;
        outputs[i] = new FileDataSink(new CsvOutputFormat(), outputHashtagPeeks, hashtagPeeks , "Hahtag Peeks");
        CsvOutputFormat.configureRecordFormat(outputs[i])
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(IntValue.class, 0)
                .field(StringValue.class, 1)
                .field(IntValue.class, 2);

        i++;
        outputs[i] = new FileDataSink(new CsvOutputFormat(), outputHashtagLows, hashtagLows , "Hashtag Lows");
        CsvOutputFormat.configureRecordFormat(outputs[i])
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(IntValue.class, 0)
                .field(StringValue.class, 1)
                .field(IntValue.class, 2);

        i++;
        outputs[i] = new FileDataSink(new CsvOutputFormat(), outputHashtagLifespan, hastagLifespanJoin , "Hashtag Life Span");
        CsvOutputFormat.configureRecordFormat(outputs[i])
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(IntValue.class, 0)
                .field(StringValue.class, 1)
                .field(StringValue.class, 2);

        i++;
        outputs[i] = new FileDataSink(new CsvOutputFormat(), outputHashtagCount, countAllHashtagTweets , "Hashtag Total Tweets");
        CsvOutputFormat.configureRecordFormat(outputs[i])
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(IntValue.class, 0)
                .field(IntValue.class, 1);

        i++;
        outputs[i] = new FileDataSink(new CsvOutputFormat(), outputWordAppearances, countWordAppearances , "Word Total Appearances");
        CsvOutputFormat.configureRecordFormat(outputs[i])
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(StringValue.class, 0)
                .field(IntValue.class, 1);






        HashSet outputsSet = new HashSet<FileDataSink>();
        outputsSet.addAll(Arrays.asList(outputs));

        Plan plan = new Plan(outputsSet, "Tweet Statistics Process");

        plan.setDefaultParallelism(noSubTasks);
        return plan;
    }

    @Override
    public String getDescription() {
        return "Parameters: [No Tasks] [Tweets] [Dates] [DictionaryFile] [SentiDataFolder] [WordsTreshold] [Hashtags] [outputDir] [wordAppearanceTreshold]";
    }
}
