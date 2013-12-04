package eu.unitn.disi.db.spleetter;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.unitn.disi.db.spleetter.cogroup.EnglishDictionaryCoGroup;
import eu.unitn.disi.db.spleetter.cogroup.HashtagPolarityCoGroup;
import eu.unitn.disi.db.spleetter.map.CleanTextMap;
import eu.unitn.disi.db.spleetter.map.LoadDictionaryMap;
import eu.unitn.disi.db.spleetter.map.LoadHashtagMap;
import eu.unitn.disi.db.spleetter.map.LoadTweetDatesMap;
import eu.unitn.disi.db.spleetter.map.LoadTweetMap;
import eu.unitn.disi.db.spleetter.map.PolarityHashtagExtractMap;
import eu.unitn.disi.db.spleetter.map.SentimentAnalysisMap;
import eu.unitn.disi.db.spleetter.map.SplitSentenceMap;
import eu.unitn.disi.db.spleetter.map.UserExtractMap;
import eu.unitn.disi.db.spleetter.map.UserTweetExtractMap;
import eu.unitn.disi.db.spleetter.match.DictionaryFilterMatch;
import eu.unitn.disi.db.spleetter.match.HashtagLifespanMatch;
import eu.unitn.disi.db.spleetter.match.HashtagPolarityMatch;
import eu.unitn.disi.db.spleetter.match.HashtagUserMatch;
import eu.unitn.disi.db.spleetter.match.TweetDateMatch;
import eu.unitn.disi.db.spleetter.match.TweetPolarityMatch;
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
public class TweetCleanse implements PlanAssembler, PlanAssemblerDescription {

    public static final String WORDS_TRESHOLD = "parameter.WORDS_TRESHOLD";
    public static final String APPEARANCE_TRESHOLD = "parameter.APPEARANCE_TRESHOLD";

    /*
     * Profiling variables
     */
    public static final boolean LoadDictionaryMapLog          = false;  // LDM
    public static final boolean LoadTweetMapLog               = false;  // LTM
    public static final boolean LoadTweetDatesMapLog          = false;  // LTD
    public static final boolean LoadHashtagMapLog             = false;  // LHM
    public static final boolean EnglishDictionaryCoGroupLog   = false;  // EDCG
    public static final boolean CountEnglishWordsReduceLog    = false;   // CEWR
    public static final boolean CleanTextMapLog               = false;   // CTM
    public static final boolean DictionaryFilterMatchLog      = false;   // DFM
    public static final boolean SplitSentenceMapLog           = false;
    public static final boolean SentimentAnalysisMapLog       = false;   // SAM
    public static final boolean TweetPolarityMatchLog         = false ;  // TPM
    public static final boolean TweetDateMatchLog             = false;
    public static final boolean HashtagPolarityCoGroupLog     = false;
    public static final boolean HashtagPolarityMatchLog       = false;
    public static final boolean PolarityHashtagExtractMapLog  = false;
    public static final boolean UserExtractMapLog             = false;
    public static final boolean UserTweetExtractMapLog        = false;
    public static final boolean HashtagLifespanMatchLog       = false;
    public static final boolean HashtagUserMatchLog           = false;
    public static final boolean CountAllHashtagTweetsReduceLog = false;
    public static final boolean CountHashtagTweetsReduceLog   = false;
    public static final boolean CountHashtagUsersReduceLog    = false;
    public static final boolean CountUserTweetsReduceLog      = false;
    public static final boolean HashtagFirstAppearanceReduceLog = false;
    public static final boolean HashtagLastAppearanceReduceLog = false;
    public static final boolean HashtagLowsReduceLog          = false;
    public static final boolean HashtagPeeksReduceLog         = false;
    public static final boolean SumHashtagPolarityReduceLog   = false;
    public static final boolean CountWordsAppearancesReduceLog = false;





    @Override
    public Plan getPlan(String... args) {
        int noSubTasks          = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
        String dataInput        = (args.length > 1 ? args[1] : "");
        String datesInput       = (args.length > 2 ? args[2] : "");
        String dictionaryInput  = (args.length > 3 ? args[3] : "");
        String wordTreshold     = (args.length > 4 ? args[4] : "0.2");
        String hashtagInput     = (args.length > 5 ? args[5] : "");
        String appearanceTreshold     = (args.length > 6 ? args[6] : "1");

        String outputCleanTweets        = (args.length > 7 ? args[7] : "file:///tmp/") +"/clean_tweets";
        String outputUsersTweetsCount   = (args.length > 7 ? args[7] : "file:///tmp/") +"/users_tweets";
        String outputHashtagUsersCount  = (args.length > 7 ? args[7] : "file:///tmp/") +"/hashtag_users";
        String outputHashtagSentiment   = (args.length > 7 ? args[7] : "file:///tmp/") +"/hashtag_sentiment";
        String outputHashtagTweetsCount = (args.length > 7 ? args[7] : "file:///tmp/") +"/hashtag_tweets";
        String outputHashtagCount       = (args.length > 7 ? args[7] : "file:///tmp/") +"/hashtag_count";
        String outputHashtagLows        = (args.length > 7 ? args[7] : "file:///tmp/") +"/hashtag_lows";
        String outputHashtagPeeks       = (args.length > 7 ? args[7] : "file:///tmp/") +"/hashtag_peeks";
        String outputHashtagLifespan    = (args.length > 7 ? args[7] : "file:///tmp/") +"/hashtag_lifespan";
        String outputWordAppearances    = (args.length > 7 ? args[7] : "file:///tmp/") +"/words_count";


//        int outputFilesCount = 9;

        /*
         * Load Data
         */

        FileDataSource tweets = new FileDataSource(TextInputFormat.class, dataInput, "Input Lines");
        tweets.setParameter(TextInputFormat.CHARSET_NAME, TextInputFormat.DEFAULT_CHARSET_NAME);	// comment out this line for UTF-8 inputs

        MapContract tokenizeMapper = MapContract.builder(LoadTweetMap.class)
                .input(tweets)
                .name("Tokenize Lines")
                .build();
        //tokenizeMapper.getCompilerHints().setAvgBytesPerRecord(105);
        //tokenizeMapper.getCompilerHints().setUniqueField(new FieldSet(0));
        //tokenizeMapper.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(new int[]{0, 1}), 1);



        FileDataSource dates = new FileDataSource(TextInputFormat.class, datesInput, "Tweet dates");
        dates.setParameter(TextInputFormat.CHARSET_NAME, TextInputFormat.DEFAULT_CHARSET_NAME);		// comment out this line for UTF-8 inputs

        MapContract datesMapper = MapContract.builder(LoadTweetDatesMap.class)
                .input(dates)
                .name("Tokenize Dates")
                .build();
        //datesMapper.getCompilerHints().setAvgBytesPerRecord(50);
        //datesMapper.getCompilerHints().setUniqueField(new FieldSet(0));
        //datesMapper.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(new int[]{0, 1}), 1);


        FileDataSource dict = new FileDataSource(TextInputFormat.class, dictionaryInput, "English words");
        dict.setParameter(TextInputFormat.CHARSET_NAME, TextInputFormat.DEFAULT_CHARSET_NAME);		// comment out this line for UTF-8 inputs

        MapContract dictionaryMap = MapContract.builder(LoadDictionaryMap.class)
                .input(dict)
                .name("Load dictionary")
                .build();
        //dictionaryMap.getCompilerHints().setAvgBytesPerRecord(10);
        //dictionaryMap.getCompilerHints().setUniqueField(new FieldSet(0));
        //dictionaryMap.getCompilerHints().setUniqueField(new FieldSet(0));


        FileDataSource hashtags = new FileDataSource(TextInputFormat.class, hashtagInput, "Hashtags");
        hashtags.setParameter(TextInputFormat.CHARSET_NAME, TextInputFormat.DEFAULT_CHARSET_NAME);		// comment out this line for UTF-8 inputs

        MapContract loadHashtags = MapContract.builder(LoadHashtagMap.class)
                .input(hashtags)
                .name("Load Hashtags")
                .build();

//        loadHashtags.getCompilerHints().setAvgBytesPerRecord(35);
//        loadHashtags.getCompilerHints().setUniqueField(new FieldSet(0));
//        loadHashtags.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(new int[]{0, 1}), 1);



        /*
         * Flow
         */


        MatchContract datedTweets = MatchContract.builder(TweetDateMatch.class, PactLong.class, 0,0)
                .keyField(PactLong.class, 1, 1)
                .input1(tokenizeMapper)
                .input2(datesMapper)
                .name("Join tweets and dates")
                .build();
        //datedTweets.getCompilerHints().setAvgRecordsEmittedPerStubCall(1.0f);
        //datedTweets.getCompilerHints().setAvgBytesPerRecord(130);

        MapContract cleanText = MapContract.builder(CleanTextMap.class)
                .input(datedTweets)
                .name("Clean Tweets")
                .build();
        //cleanText.getCompilerHints().setAvgRecordsEmittedPerStubCall(1.0f);

        MapContract sentimentAnalysis = MapContract.builder(SentimentAnalysisMap.class)
                .input(datedTweets)
                .name("Sentiment Analysis")
                .build();
        //sentimentAnalysis.getCompilerHints().setAvgRecordsEmittedPerStubCall(1.0f);

        MapContract splitSentence = MapContract.builder(SplitSentenceMap.class)
                .input(cleanText)
                .name("Split to words")
                .build();

        ReduceContract countWordAppearances = ReduceContract.builder(CountWordsAppearancesReduce.class, PactString.class, 0)
                .input(splitSentence)
                .name("Count word appearances")
                .build();
        countWordAppearances.setParameter(APPEARANCE_TRESHOLD, appearanceTreshold);


        CoGroupContract englishGroup = CoGroupContract.builder(EnglishDictionaryCoGroup.class, PactString.class, 0, 0)
                .input1(splitSentence)
                .input2(dictionaryMap)
                .name("Group en-words")
                .build();

        ReduceContract countEnglishWords = ReduceContract.builder(CountEnglishWordsReduce.class, PactLong.class, 0)
                .input(englishGroup)
                .name("Count en-words")
                .build();

        MatchContract dictionaryFilter = MatchContract.builder(DictionaryFilterMatch.class, PactLong.class, 0, 0)
                .input1(countEnglishWords)
                .input2(cleanText)
                .name("Filter English Tweets")
                .build();
        dictionaryFilter.setParameter(WORDS_TRESHOLD, wordTreshold);
        //dictionaryFilter.getCompilerHints().setAvgRecordsEmittedPerStubCall(0.2f);

        MatchContract tweetPolarityMatch = MatchContract.builder(TweetPolarityMatch.class, PactLong.class, 0, 0)
                .input1(dictionaryFilter)
                .input2(sentimentAnalysis)
                .name("Tweet Polarity Match")
                .build();

        MapContract userExtract = MapContract.builder(UserExtractMap.class)
                .input(tweetPolarityMatch)
                .name("Extract User")
                .build();

        ReduceContract countUserTweets = ReduceContract.builder(CountUserTweetsReduce.class, PactLong.class, 0)
                .input(userExtract)
                .name("Count user tweets")
                .build();



        MapContract timePolarity = MapContract.builder(PolarityHashtagExtractMap.class)
                .input(tweetPolarityMatch)
                .name("Tweet Time & Polarity")
                .build();

        MapContract userTweetExtract = MapContract.builder(UserTweetExtractMap.class)
                .input(tweetPolarityMatch)
                .name("Extract User")
                .build();


        MatchContract hashtagUserMatch = MatchContract.builder(HashtagUserMatch.class, PactLong.class, 0, 0)
                .keyField(PactInteger.class, 1, 1)
                .input1(userTweetExtract)
                .input2(loadHashtags)
                .name("Hashtag User Match")
                .build();

        ReduceContract countHashtagUsers = ReduceContract.builder(CountHashtagUsersReduce.class, PactString.class, 0)
                .keyField(PactInteger.class, 1)
                .input(hashtagUserMatch)
                .name("Count hastag users")
                .build();


        MatchContract hashtagPolarityMatch = MatchContract.builder(HashtagPolarityMatch.class, PactLong.class, 0, 0)
                .keyField(PactInteger.class, 1, 1)
                .input1(timePolarity)
                .input2(loadHashtags)
                .name("Hashtag Polarity Match")
                .build();

        ReduceContract sumHashtagPolarity = ReduceContract.builder(SumHashtagPolarityReduce.class, PactString.class, 0)
                .keyField(PactInteger.class, 1)
                .input(hashtagPolarityMatch)
                .name("Sum Hashtag polarities")
                .build();

        ReduceContract countHashtagTweets = ReduceContract.builder(CountHashtagTweetsReduce.class, PactString.class, 0)
                .keyField(PactInteger.class, 1)
                .input(hashtagPolarityMatch)
                .name("Count Hashtag Tweets")
                .build();

        //NB reduce on key 1
        ReduceContract countAllHashtagTweets = ReduceContract.builder(CountAllHashtagTweetsReduce.class, PactInteger.class, 1)
                .input(countHashtagTweets)
                .name("Count Hashtag Tweets")
                .build();


        CoGroupContract timestampPolarityGroup = CoGroupContract.builder(HashtagPolarityCoGroup.class, PactString.class, 0, 0)
                .keyField(PactInteger.class, 1,1)
                .input1(countHashtagTweets)
                .input2(sumHashtagPolarity)
                .name("Compute mean Divergence")
                .build();

        //NB reduce on key 1
        ReduceContract hashtagPeeks = ReduceContract.builder(HashtagPeeksReduce.class)
                .keyField( PactInteger.class, 1)
                .input(countHashtagTweets)
                .name("Find Hashtags Peeks")
                .build();

        //NB reduce on key 1
        ReduceContract hashtagLows = ReduceContract.builder(HashtagLowsReduce.class)
                .keyField(PactInteger.class, 1)
                .input(countHashtagTweets)
                .name("Find Hashtags Low")
                .build();


        //NB reduce on key 1
        ReduceContract hashtagFirstAppearance = ReduceContract.builder(HashtagFirstAppearanceReduce.class)
                .keyField(PactInteger.class, 1)
                .input(countHashtagTweets)
                .name("Find Hashtag first Appearance")
                .build();


        //NB reduce on key 1
        ReduceContract hashtagLastAppearance = ReduceContract.builder(HashtagLastAppearanceReduce.class)
                .keyField(PactInteger.class, 1)
                .input(countHashtagTweets)
                .name("Find Hashtag last Appearance")
                .build();

        MatchContract hastagLifespanMatch = MatchContract.builder(HashtagLifespanMatch.class, PactInteger.class, 0, 0)
                .input1(hashtagFirstAppearance)
                .input2(hashtagLastAppearance)
                .name("Compose Hashtag Time Window")
                .build();



        /*
         * Output
         */


        //FileDataSink[] outputs = new FileDataSink[outputFilesCount];
        FileDataSink[] outputs = new FileDataSink[10];
        int i = 0;

        outputs[i] = new FileDataSink(RecordOutputFormat.class, outputCleanTweets, tweetPolarityMatch, "Pruned tweets with polarities");
        RecordOutputFormat.configureRecordFormat(outputs[i])
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(PactLong.class, 0)
                .field(PactInteger.class, 1)
                .field(PactString.class, 2)
                .field(PactInteger.class, 3)
                .field(PactString.class, 4)
                .field(PactDouble.class, 5)
                .field(PactDouble.class, 6);

        i++;
        outputs[i] = new FileDataSink(RecordOutputFormat.class, outputUsersTweetsCount, countUserTweets, "User tweets count");
        RecordOutputFormat.configureRecordFormat(outputs[i])
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(PactInteger.class, 0)
                .field(PactInteger.class, 1);

        i++;
        outputs[i] = new FileDataSink(RecordOutputFormat.class, outputHashtagUsersCount, countHashtagUsers, "Hahtag Users count");
        RecordOutputFormat.configureRecordFormat(outputs[i])
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(PactString.class, 0)
                .field(PactInteger.class, 1)
                .field(PactInteger.class, 2);

        i++; //timestampPolarityGroup
        outputs[i] = new FileDataSink(RecordOutputFormat.class, outputHashtagSentiment, timestampPolarityGroup, "Hahtag Polarities ");
        RecordOutputFormat.configureRecordFormat(outputs[i])
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(PactString.class, 0)
                .field(PactInteger.class, 1)
                .field(PactDouble.class, 2)
                .field(PactDouble.class, 3)
                .field(PactDouble.class, 4)
                .field(PactDouble.class, 5)
                .field(PactDouble.class, 6)
                .field(PactDouble.class, 7)
                .field(PactInteger.class, 8);
        i++;
        outputs[i] = new FileDataSink(RecordOutputFormat.class, outputHashtagTweetsCount, countHashtagTweets , "Hahtag Tweets Count");
        RecordOutputFormat.configureRecordFormat(outputs[i])
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(PactString.class, 0)
                .field(PactInteger.class, 1)
                .field(PactInteger.class, 2);

        i++;
        outputs[i] = new FileDataSink(RecordOutputFormat.class, outputHashtagPeeks, hashtagPeeks , "Hahtag Peeks");
        RecordOutputFormat.configureRecordFormat(outputs[i])
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(PactInteger.class, 0)
                .field(PactString.class, 1)
                .field(PactInteger.class, 2);

        i++;
        outputs[i] = new FileDataSink(RecordOutputFormat.class, outputHashtagLows, hashtagLows , "Hashtag Lows");
        RecordOutputFormat.configureRecordFormat(outputs[i])
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(PactInteger.class, 0)
                .field(PactString.class, 1)
                .field(PactInteger.class, 2);

        i++;
        outputs[i] = new FileDataSink(RecordOutputFormat.class, outputHashtagLifespan, hastagLifespanMatch , "Hashtag Life Ssan");
        RecordOutputFormat.configureRecordFormat(outputs[i])
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(PactInteger.class, 0)
                .field(PactString.class, 1)
                .field(PactString.class, 2);

        i++;
        outputs[i] = new FileDataSink(RecordOutputFormat.class, outputHashtagCount, countAllHashtagTweets , "Hashtag Total Tweets");
        RecordOutputFormat.configureRecordFormat(outputs[i])
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(PactInteger.class, 0)
                .field(PactInteger.class, 1);

        i++;
        outputs[i] = new FileDataSink(RecordOutputFormat.class, outputWordAppearances, countWordAppearances , "Word Total Appearances");
        RecordOutputFormat.configureRecordFormat(outputs[i])
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(PactString.class, 0)
                .field(PactInteger.class, 1);






        HashSet outputsSet = new HashSet<FileDataSink>();
        outputsSet.addAll(Arrays.asList(outputs));

        Plan plan = new Plan(outputsSet, "Tweet Statistics Process");

        plan.setDefaultParallelism(noSubTasks);
        return plan;
    }

    @Override
    public String getDescription() {
        return "Parameters: [No Tasks] [Tweets] [Dates] [DictionaryFile] [WordsTreshold] [Hashtags] [outputDir] [wordAppearanceTreshold]";
    }
}
