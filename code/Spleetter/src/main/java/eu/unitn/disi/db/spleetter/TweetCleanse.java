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
import eu.unitn.disi.db.spleetter.join.CountedNgramsJoin;
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
import eu.unitn.disi.db.spleetter.map.NgramTokenizerMap;
import eu.unitn.disi.db.spleetter.map.PolarityHashtagExtractMap;
import eu.unitn.disi.db.spleetter.map.SentimentAnalysisMap;
import eu.unitn.disi.db.spleetter.map.SpamFlagMap;
import eu.unitn.disi.db.spleetter.map.UserExtractMap;
import eu.unitn.disi.db.spleetter.map.UserTweetExtractMap;
import eu.unitn.disi.db.spleetter.map.WordTokenizerMap;
import eu.unitn.disi.db.spleetter.reduce.CountEnglishWordsReduce;
import eu.unitn.disi.db.spleetter.reduce.CountHashtagAppearancesReduce;
import eu.unitn.disi.db.spleetter.reduce.CountHashtagHourlyReduce;
import eu.unitn.disi.db.spleetter.reduce.CountHashtagUsersReduce;
import eu.unitn.disi.db.spleetter.reduce.CountNgramHourlyReduce;
import eu.unitn.disi.db.spleetter.reduce.CountNgramsAppearancesReduce;
import eu.unitn.disi.db.spleetter.reduce.CountTweetsHourlyReduce;
import eu.unitn.disi.db.spleetter.reduce.CountUserTweetsReduce;
import eu.unitn.disi.db.spleetter.reduce.HashtagFirstAppearanceReduce;
import eu.unitn.disi.db.spleetter.reduce.HashtagLastAppearanceReduce;
import eu.unitn.disi.db.spleetter.reduce.HashtagLowsReduce;
import eu.unitn.disi.db.spleetter.reduce.HashtagPeeksReduce;
import eu.unitn.disi.db.spleetter.reduce.SumHashtagPolarityReduce;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Perform cleansing phase of tweets.
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class TweetCleanse implements Program, ProgramDescription {

    public static final String WORDS_TRESHOLD = "parameter.WORDS_TRESHOLD";
    public static final String APPEARANCE_TRESHOLD = "parameter.APPEARANCE_TRESHOLD";
    public static final String SENTIMENT_PATH = "parameter.SENTIMENT_PATH";
    public static final String N_GRAM_LENGTH = "parameter.N_GRAM_LENGTH";

    /*
     * Profiling variables
     */
    public static final boolean LoadDictionaryMapLog               = true;  // LDM
    public static final boolean LoadTweetMapLog                    = true;  // LTM
    public static final boolean LoadTweetDatesMapLog               = true;  // LTD
    public static final boolean LoadHashtagMapLog                  = true;  // LHM
    public static final boolean EnglishDictionaryCoGroupLog        = true;  // EDCG
    public static final boolean CountEnglishWordsReduceLog         = true;  // CEWR
    public static final boolean CleanTextMapLog                    = true;  // CTM
    public static final boolean DictionaryFilterJoinLog            = true;  // DFM
    public static final boolean WordTokenizerMapLog                = true;
    public static final boolean NgramTokenizerMapLog               = true;
    public static final boolean SentimentAnalysisMapLog            = true;  // SAM
    public static final boolean TweetPolarityJoinLog               = true;  // TPM
    public static final boolean TweetDateJoinLog                   = true;
    public static final boolean HashtagPolarityCoGroupLog          = true;
    public static final boolean HashtagPolarityJoinLog             = true;
    public static final boolean PolarityHashtagExtractMapLog       = true;
    public static final boolean UserExtractMapLog                  = true;
    public static final boolean UserTweetExtractMapLog             = true;
    public static final boolean HashtagLifespanJoinLog             = true;
    public static final boolean HashtagUserJoinLog                 = true;
    public static final boolean CountedNgramsJoinLog               = true;
    public static final boolean CountTweetsHourlyReduceLog         = true;
    public static final boolean CountHashtagAppearancesReduceLog   = true;
    public static final boolean CountHashtagHourlyReduceLog        = true;
    public static final boolean CountNgramsHourlyReduceLog         = true;
    public static final boolean CountHashtagUsersReduceLog         = true;
    public static final boolean CountUserTweetsReduceLog           = true;
    public static final boolean HashtagFirstAppearanceReduceLog    = true;
    public static final boolean HashtagLastAppearanceReduceLog     = true;
    public static final boolean HashtagLowsReduceLog               = true;
    public static final boolean HashtagPeeksReduceLog              = true;
    public static final boolean SumHashtagPolarityReduceLog        = true;
    public static final boolean CountNgramsAppearancesReduceLog    = true;
    public static final boolean SpamFlagMapLog                     = true;





    @Override
    public Plan getPlan(String... args) {
        int noSubTasks          = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
        String tweetsInput      = (args.length > 1 ? args[1] : "");
        String datesInput       = (args.length > 2 ? args[2] : "");
        String hashtagInput     = (args.length > 3 ? args[3] : "");
        String dictionaryInput  = (args.length > 4 ? args[4] : "");
        String sentimentData    = (args.length > 5 ? args[5] : "");
        String wordTreshold     = (args.length > 6 ? args[6] : "0.2");
        String appearanceTreshold     = (args.length > 7 ? args[7] : "1");

        String outDir = (args.length > 8 ? args[8] : "file:///tmp/");
        String outputCleanTweets          = outDir +"/clean_tweets";
        String outputUsersTweetsCount     = outDir +"/users_tweets";
        String outputHashtagUsersCount    = outDir +"/hashtag_users";
        String outputHashtagSentiment     = outDir +"/hashtag_sentiment";
        String outputHashtagPerHourCount  = outDir +"/hashtag_hourly";
        String outputWordPerHourCount     = outDir +"/word_hourly";
        String outputBigramPerHourCount   = outDir +"/bigram_hourly";
        String outputTrigramPerHourCount  = outDir +"/trigram_hourly";
        String outputTweetsPerHourCount   = outDir +"/tweets_hourly";
        String outputHashtagCount         = outDir +"/hashtag_count";
        String outputHashtagLows          = outDir +"/hashtag_lows";
        String outputHashtagPeeks         = outDir +"/hashtag_highs";
        String outputHashtagLifespan      = outDir +"/hashtag_lifespan";
        String outputWordAppearances      = outDir +"/words_count";
        String outputBigramAppearances    = outDir +"/bigram_count";
        String outputTrigramAppearances   = outDir +"/trigram_count";
        String outputSpamTweets           = outDir +"/spam_tweets";

        /*
         * Load Data
         */

        FileDataSource tweets = new FileDataSource(TextInputFormat.class, tweetsInput, "Input Lines");
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


//        FileDataSource hashtags = new FileDataSource(TextInputFormat.class, hashtagInput, "Hashtags");
//        hashtags.setParameter(TextInputFormat.CHARSET_NAME, TextInputFormat.DEFAULT_CHARSET_NAME);		// comment out this line for UTF-8 inputs
//
//        MapOperator loadHashtags = MapOperator.builder(LoadHashtagMap.class)
//                .input(hashtags)
//                .name("Load Hashtags")
//                .build();

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



        MapOperator spamMatcher = MapOperator.builder(SpamFlagMap.class)
                .input(datedTweets)
                .name("Spam Flagging")
                .build();


        ReduceOperator countHourlyTweets = ReduceOperator.builder(CountTweetsHourlyReduce.class, StringValue.class, 4)
                .input(cleanText)
                .name("Count tweets per hour appearances")
                .build();

        MapOperator wordsTokenizer = MapOperator.builder(WordTokenizerMap.class)
                .input(cleanText)
                .name("Split to words")
                .build();

        MapOperator bigramsTokenizer = MapOperator.builder(NgramTokenizerMap.class) // 0 ngram - 1 tweet id - 2 user id - 3 tweet date [h]
                .input(cleanText)
                .name("Split to bigrams")
                .build();
        bigramsTokenizer.setParameter(N_GRAM_LENGTH, 2);


        MapOperator trigramsTokenizer = MapOperator.builder(NgramTokenizerMap.class)
                .input(cleanText)
                .name("Split to trigrams")
                .build();
        trigramsTokenizer.setParameter(N_GRAM_LENGTH, 3);


        ReduceOperator countWordAppearances = ReduceOperator.builder(CountNgramsAppearancesReduce.class, StringValue.class, 0)
                .input(wordsTokenizer)
                .name("Count word appearances")
                .build();
        countWordAppearances.setParameter(APPEARANCE_TRESHOLD, appearanceTreshold);

        ReduceOperator countBigramAppearances = ReduceOperator.builder(CountNgramsAppearancesReduce.class, StringValue.class, 0) // 0 word - 1 number of appearances
                .input(bigramsTokenizer)
                .name("Count bigram appearances")
                .build();
        countBigramAppearances.setParameter(APPEARANCE_TRESHOLD, appearanceTreshold);

        ReduceOperator countTrigramAppearances = ReduceOperator.builder(CountNgramsAppearancesReduce.class, StringValue.class, 0)
                .input(trigramsTokenizer)
                .name("Count trigram appearances")
                .build();
        countTrigramAppearances.setParameter(APPEARANCE_TRESHOLD, appearanceTreshold);



        JoinOperator countedWordsFilter = JoinOperator.builder(CountedNgramsJoin.class, StringValue.class, 0, 0)  // 0 timestamp [h] - 1 ngram
                .input1(countWordAppearances)
                .input2(wordsTokenizer)
                .name("Filter Words that are popular enough")
                .build();

        JoinOperator countedBigramsFilter = JoinOperator.builder(CountedNgramsJoin.class, StringValue.class, 0, 0)  // 0 timestamp [h] - 1 ngram
                .input1(countBigramAppearances)
                .input2(bigramsTokenizer)
                .name("Filter Bigrams that are popular enough")
                .build();

        JoinOperator countedTrigramsFilter = JoinOperator.builder(CountedNgramsJoin.class, StringValue.class, 0, 0)
                .input1(countTrigramAppearances)
                .input2(trigramsTokenizer)
                .name("Filter Trigrams that are popular enough")
                .build();


        ReduceOperator countWordPerHour = ReduceOperator.builder(CountNgramHourlyReduce.class, StringValue.class, 0)
                .keyField(StringValue.class, 1)
                .input(countedWordsFilter)
                .name("Count Word per Hour")
                .build();

        ReduceOperator countBigramPerHour = ReduceOperator.builder(CountNgramHourlyReduce.class, StringValue.class, 0)
                .keyField(StringValue.class, 1)
                .input(countedBigramsFilter)
                .name("Count Word per Hour")
                .build();

        ReduceOperator countTrigramPerHour = ReduceOperator.builder(CountNgramHourlyReduce.class, StringValue.class, 0)
                .keyField(StringValue.class, 1)
                .input(countedTrigramsFilter)
                .name("Count Word per Hour")
                .build();

        CoGroupOperator englishGroup = CoGroupOperator.builder(EnglishDictionaryCoGroup.class, StringValue.class, 0, 0)
                .input1(wordsTokenizer)
                .input2(dictionaryMap)
                .name("Group en-words")
                .build();

        ReduceOperator countEnglishWords = ReduceOperator.builder(CountEnglishWordsReduce.class, LongValue.class, 0)
                .keyField(LongValue.class, 1)
                .input(englishGroup)
                .name("Count en-words")
                .build();

        JoinOperator dictionaryFilter = JoinOperator.builder(DictionaryFilterJoin.class, LongValue.class, 0, 0)
                .keyField(LongValue.class, 1, 1)
                .input1(countEnglishWords)
                .input2(cleanText)
                .name("Filter English Tweets")
                .build();
        dictionaryFilter.setParameter(WORDS_TRESHOLD, wordTreshold);
//        //dictionaryFilter.getCompilerHints().setAvgRecordsEmittedPerFunctionCall(0.2f);


//        JoinOperator dictionaryFilter2 = JoinOperator.builder(DictionaryFilterJoin.class, LongValue.class, 0, 0)
//                .keyField(LongValue.class, 1, 1)
//                .input1(countEnglishWords)
//                .input2(datedTweets)
//                .name("Filter English Tweets Original Text")
//                .build();
//        dictionaryFilter.setParameter(WORDS_TRESHOLD, wordTreshold);


        MapOperator sentimentAnalysis = MapOperator.builder(SentimentAnalysisMap.class)
                //.input(dictionaryFilter2)
                .input(datedTweets)
                .name("Sentiment Analysis")
                .build();
        sentimentAnalysis.setParameter(SENTIMENT_PATH, sentimentData);
        //sentimentAnalysis.getCompilerHints().setAvgRecordsEmittedPerFunctionCall(1.0f);

        JoinOperator tweetPolarityJoin = JoinOperator.builder(TweetPolarityJoin.class, LongValue.class, 0, 0)
                .keyField(LongValue.class, 1, 1)
                //.input1(dictionaryFilter)
                .input1(dictionaryFilter)
                .input2(sentimentAnalysis)
                .name("Tweet Polarity Join")
                .build();

        MapOperator userExtract = MapOperator.builder(UserExtractMap.class)
                //.input(dictionaryFilter)
                .input(dictionaryFilter)
                .name("Extract User")
                .build();

        ReduceOperator countUserTweets = ReduceOperator.builder(CountUserTweetsReduce.class, LongValue.class, 0)
                .input(userExtract)
                .name("Count user tweets")
                .build();



//        MapOperator timePolarity = MapOperator.builder(PolarityHashtagExtractMap.class)
//                .input(tweetPolarityJoin)
//                .name("Tweet Time & Polarity")
//                .build();
//
//        MapOperator userTweetExtract = MapOperator.builder(UserTweetExtractMap.class)
//                .input(tweetPolarityJoin)
//                .name("Extract User")
//                .build();


//        JoinOperator hashtagUserJoin = JoinOperator.builder(HashtagUserJoin.class, LongValue.class, 0, 0)
//                .keyField(LongValue.class, 1, 1)
//                .input1(userTweetExtract) // tweet, user, timestamp
//                .input2(loadHashtags) // tweet, user, hashtag
//                .name("Hashtag User Join")
//                .build(); // out: timestamp, hashatag, user
//
//        ReduceOperator countHashtagUsers = ReduceOperator.builder(CountHashtagUsersReduce.class, StringValue.class, 0)
//                .keyField(IntValue.class, 1)
//                .input(hashtagUserJoin)
//                .name("Count hastag distinct users")
//                .build();
//
//
//        JoinOperator hashtagPolarityJoin = JoinOperator.builder(HashtagPolarityJoin.class, LongValue.class, 0, 0)
//                .keyField(IntValue.class, 1, 1)
//                .input1(timePolarity)
//                .input2(loadHashtags)
//                .name("Hashtag Polarity Join")
//                .build();
//
//        ReduceOperator sumHashtagPolarity = ReduceOperator.builder(SumHashtagPolarityReduce.class, StringValue.class, 0)
//                .keyField(IntValue.class, 1)
//                .input(hashtagPolarityJoin)
//                .name("Sum Hashtag polarities")
//                .build();
//
//        ReduceOperator countHashtagPerHour = ReduceOperator.builder(CountHashtagHourlyReduce.class, StringValue.class, 0)
//                .keyField(IntValue.class, 1)
//                .input(hashtagPolarityJoin)
//                .name("Count Hashtag Tweets")
//                .build();

//        //NB reduce on key 1
//        ReduceOperator countAllHashtagTweets = ReduceOperator.builder(CountHashtagAppearancesReduce.class, IntValue.class, 1)
//                .input(countHashtagPerHour)
//                .name("Count Hashtag Tweets")
//                .build();
//        countAllHashtagTweets.setParameter(APPEARANCE_TRESHOLD, appearanceTreshold);
//
//        CoGroupOperator timestampPolarityGroup = CoGroupOperator.builder(HashtagPolarityCoGroup.class, StringValue.class, 0, 0)
//                .keyField(IntValue.class, 1,1)
//                .input1(countHashtagPerHour)
//                .input2(sumHashtagPolarity)
//                .name("Compute mean Divergence")
//                .build();

//        //NB reduce on key 1
//        ReduceOperator hashtagPeeks = ReduceOperator.builder(HashtagPeeksReduce.class)
//                .keyField( IntValue.class, 1)
//                .input(countHashtagPerHour)
//                .name("Find Hashtags Peeks")
//                .build();
//
//        //NB reduce on key 1
//        ReduceOperator hashtagLows = ReduceOperator.builder(HashtagLowsReduce.class)
//                .keyField(IntValue.class, 1)
//                .input(countHashtagPerHour)
//                .name("Find Hashtags Low")
//                .build();
//
//
//        //NB reduce on key 1
//        ReduceOperator hashtagFirstAppearance = ReduceOperator.builder(HashtagFirstAppearanceReduce.class)
//                .keyField(IntValue.class, 1)
//                .input(countHashtagPerHour)
//                .name("Find Hashtag first Appearance")
//                .build();
//
//
//        //NB reduce on key 1
//        ReduceOperator hashtagLastAppearance = ReduceOperator.builder(HashtagLastAppearanceReduce.class)
//                .keyField(IntValue.class, 1)
//                .input(countHashtagPerHour)
//                .name("Find Hashtag last Appearance")
//                .build();
//
//        JoinOperator hastagLifespanJoin = JoinOperator.builder(HashtagLifespanJoin.class, IntValue.class, 0, 0)
//                .input1(hashtagFirstAppearance)
//                .input2(hashtagLastAppearance)
//                .name("Compose Hashtag Time Window")
//                .build();



        /*
         * Output
         */


        //FileDataSink[] outputs = new FileDataSink[outputFilesCount];
        List<FileDataSink> outputs = new ArrayList<FileDataSink>();
        int i = 0;

        outputs.add(new FileDataSink(new CsvOutputFormat(), outputCleanTweets, tweetPolarityJoin, "Pruned tweets with polarities"));
        CsvOutputFormat.configureRecordFormat(outputs.get(i))
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(LongValue.class, 0)
                .field(LongValue.class, 1)
                .field(StringValue.class, 2)
                .field(IntValue.class, 3)
                .field(StringValue.class, 4)
                .field(DoubleValue.class, 5)
                .field(DoubleValue.class, 6);

        i++;
        outputs.add(new FileDataSink(new CsvOutputFormat(), outputSpamTweets, spamMatcher, "Tweets marked as spam"));
        CsvOutputFormat.configureRecordFormat(outputs.get(i))
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(LongValue.class, 0)
                .field(LongValue.class, 1)
                .field(IntValue.class, 2);

        i++;
        outputs.add(new FileDataSink(new CsvOutputFormat(), outputUsersTweetsCount, countUserTweets, "User tweets count"));
        CsvOutputFormat.configureRecordFormat(outputs.get(i))
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(LongValue.class, 0)
                .field(IntValue.class, 1);

        i++;
        outputs.add(new FileDataSink(new CsvOutputFormat(), outputWordPerHourCount, countWordPerHour , "Word per Hour Count"));
        CsvOutputFormat.configureRecordFormat(outputs.get(i))
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(StringValue.class, 0)
                .field(StringValue.class, 1)
                .field(IntValue.class, 2);

        i++;
        outputs.add(new FileDataSink(new CsvOutputFormat(), outputBigramPerHourCount, countBigramPerHour , "Bigram per Hour Count"));
        CsvOutputFormat.configureRecordFormat(outputs.get(i))
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(StringValue.class, 0)
                .field(StringValue.class, 1)
                .field(IntValue.class, 2);

        i++;
        outputs.add(new FileDataSink(new CsvOutputFormat(), outputTrigramPerHourCount, countTrigramPerHour , "Trigram per Hour Count"));
        CsvOutputFormat.configureRecordFormat(outputs.get(i))
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(StringValue.class, 0)
                .field(StringValue.class, 1)
                .field(IntValue.class, 2);

        i++;
        outputs.add(new FileDataSink(new CsvOutputFormat(), outputTweetsPerHourCount, countHourlyTweets , "Tweets per Hour Count"));
        CsvOutputFormat.configureRecordFormat(outputs.get(i))
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(StringValue.class, 0)
                .field(IntValue.class, 1);


        i++;
        outputs.add(new FileDataSink(new CsvOutputFormat(), outputWordAppearances, countWordAppearances , "Word Total Appearances"));
        CsvOutputFormat.configureRecordFormat(outputs.get(i))
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(StringValue.class, 0)
                .field(IntValue.class, 1);

        i++;
        outputs.add(new FileDataSink(new CsvOutputFormat(), outputBigramAppearances, countBigramAppearances , "Bigram Total Appearances"));
        CsvOutputFormat.configureRecordFormat(outputs.get(i))
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(StringValue.class, 0)
                .field(IntValue.class, 1);

        i++;
        outputs.add(new FileDataSink(new CsvOutputFormat(), outputTrigramAppearances, countTrigramAppearances , "Trigram Total Appearances"));
        CsvOutputFormat.configureRecordFormat(outputs.get(i))
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(StringValue.class, 0)
                .field(IntValue.class, 1);

//        i++;
//        outputs.add(new FileDataSink(new CsvOutputFormat(), outputHashtagUsersCount, countHashtagUsers, "Hashtag Users count"));
//        CsvOutputFormat.configureRecordFormat(outputs.get(i))
//                .recordDelimiter('\n')
//                .fieldDelimiter('\t')
//                .lenient(true)
//                .field(StringValue.class, 0)
//                .field(IntValue.class, 1)
//                .field(IntValue.class, 2);
//
//        i++; //timestampPolarityGroup
//        outputs.add(new FileDataSink(new CsvOutputFormat(), outputHashtagSentiment, timestampPolarityGroup, "Hashtag Polarities "));
//        CsvOutputFormat.configureRecordFormat(outputs.get(i))
//                .recordDelimiter('\n')
//                .fieldDelimiter('\t')
//                .lenient(true)
//                .field(StringValue.class, 0)
//                .field(IntValue.class, 1)
//                .field(DoubleValue.class, 2)
//                .field(DoubleValue.class, 3)
//                .field(DoubleValue.class, 4)
//                .field(DoubleValue.class, 5)
//                .field(DoubleValue.class, 6)
//                .field(DoubleValue.class, 7)
//                .field(IntValue.class, 8);
//        i++;
//        outputs.add(new FileDataSink(new CsvOutputFormat(), outputHashtagPerHourCount, countHashtagPerHour , "Hashtag per Hour Count"));
//        CsvOutputFormat.configureRecordFormat(outputs.get(i))
//                .recordDelimiter('\n')
//                .fieldDelimiter('\t')
//                .lenient(true)
//                .field(StringValue.class, 0)
//                .field(IntValue.class, 1)
//                .field(IntValue.class, 2);
//
//        i++;
//        outputs.add(new FileDataSink(new CsvOutputFormat(), outputHashtagPeeks, hashtagPeeks , "Hahtag Peeks"));
//        CsvOutputFormat.configureRecordFormat(outputs.get(i))
//                .recordDelimiter('\n')
//                .fieldDelimiter('\t')
//                .lenient(true)
//                .field(IntValue.class, 0)
//                .field(StringValue.class, 1)
//                .field(IntValue.class, 2);
//
//        i++;
//        outputs.add(new FileDataSink(new CsvOutputFormat(), outputHashtagLows, hashtagLows , "Hashtag Lows"));
//        CsvOutputFormat.configureRecordFormat(outputs.get(i))
//                .recordDelimiter('\n')
//                .fieldDelimiter('\t')
//                .lenient(true)
//                .field(IntValue.class, 0)
//                .field(StringValue.class, 1)
//                .field(IntValue.class, 2);
//
//        i++;
//        outputs.add(new FileDataSink(new CsvOutputFormat(), outputHashtagLifespan, hastagLifespanJoin , "Hashtag Life Span"));
//        CsvOutputFormat.configureRecordFormat(outputs.get(i))
//                .recordDelimiter('\n')
//                .fieldDelimiter('\t')
//                .lenient(true)
//                .field(IntValue.class, 0)
//                .field(StringValue.class, 1)
//                .field(StringValue.class, 2);
//
//        i++;
//        outputs.add(new FileDataSink(new CsvOutputFormat(), outputHashtagCount, countAllHashtagTweets , "Hashtag Total Tweets"));
//        CsvOutputFormat.configureRecordFormat(outputs.get(i))
//                .recordDelimiter('\n')
//                .fieldDelimiter('\t')
//                .lenient(true)
//                .field(IntValue.class, 0)
//                .field(IntValue.class, 1);
//



        HashSet outputsSet = new HashSet<FileDataSink>();
        outputsSet.addAll(outputs);

        Plan plan = new Plan(outputsSet, "Tweet Statistics Process");

        plan.setDefaultParallelism(noSubTasks);
        return plan;
    }

    @Override
    public String getDescription() {
        return "Parameters: [No Tasks] [Tweets] [Dates] [Hashtags] [DictionaryFile] [SentiDataFolder] [EngWordsTreshold] [minCountAppearanceTreshold] [outputDir] ";
    }
}
