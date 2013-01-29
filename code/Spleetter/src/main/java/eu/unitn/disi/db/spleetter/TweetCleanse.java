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
import eu.unitn.disi.db.spleetter.map.LoadTweetMap;
import eu.unitn.disi.db.spleetter.map.SentimentAnalysisMap;
import eu.unitn.disi.db.spleetter.map.SplitSentenceMap;
import eu.unitn.disi.db.spleetter.map.TimePolarityMap;
import eu.unitn.disi.db.spleetter.map.UserExtractMap;
import eu.unitn.disi.db.spleetter.map.UserTweetExtractMap;
import eu.unitn.disi.db.spleetter.match.DictionaryFilterMatch;
import eu.unitn.disi.db.spleetter.match.HashtagLifespanMatch;
import eu.unitn.disi.db.spleetter.match.HashtagPolarityMatch;
import eu.unitn.disi.db.spleetter.match.HashtagUserMatch;
import eu.unitn.disi.db.spleetter.match.TweetPolarityMatch;
import eu.unitn.disi.db.spleetter.reduce.CountEnglishWordsReduce;
import eu.unitn.disi.db.spleetter.reduce.CountHashtagTweetsReduce;
import eu.unitn.disi.db.spleetter.reduce.CountHashtagUsersReduce;
import eu.unitn.disi.db.spleetter.reduce.CountUserTweetsReduce;
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

    @Override
    public Plan getPlan(String... args) {
        final int noSubTasks          = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
        final String dataInput        = (args.length > 1 ? args[1] : "");
        final String dictionaryInput  = (args.length > 2 ? args[2] : "");
        final String wordTreshold     = (args.length > 3 ? args[3] : "0.2");
        final String hashtagInput     = (args.length > 4 ? args[4] : "");

        final String outputCleanTweets        = (args.length > 5 ? args[5]+"/clean_tweets.txt" : "");
        final String outputUsersTweetsCount   = (args.length > 5 ? args[5]+"/users_tweets.txt" : "");
        final String outputHashtagUsersCount  = (args.length > 5 ? args[5]+"/hashtag_users.txt" : "");
        final String outputHashtagSentiment   = (args.length > 5 ? args[5]+"/hashtag_sentiment.txt" : "");
        final String outputHashtagTweetsCount = (args.length > 5 ? args[5]+"/hashtag_tweets.txt" : "");
        final String outputHashtagLows        = (args.length > 5 ? args[5]+"/hashtag_lows.txt" : "");
        final String outputHashtagPeeks       = (args.length > 5 ? args[5]+"/hashtag_peeks.txt" : "");
        final String outputHashtagLifespan    = (args.length > 5 ? args[5]+"/hashtag_lifespan.txt" : "");


        /*
         * Load Data
         */

        FileDataSource source = new FileDataSource(TextInputFormat.class, dataInput, "Input Lines");
        source.setParameter(TextInputFormat.CHARSET_NAME, TextInputFormat.DEFAULT_CHARSET_NAME);		// comment out this line for UTF-8 inputs

        MapContract tokenizeMapper = MapContract.builder(LoadTweetMap.class)
                .input(source)
                .name("Tokenize Lines")
                .build();

        FileDataSource dict = new FileDataSource(TextInputFormat.class, dictionaryInput, "English words");
        dict.setParameter(TextInputFormat.CHARSET_NAME, TextInputFormat.DEFAULT_CHARSET_NAME);		// comment out this line for UTF-8 inputs

        MapContract dictionaryMap = MapContract.builder(LoadDictionaryMap.class)
                .input(dict)
                .name("Load dictionary")
                .build();


        FileDataSource hashtags = new FileDataSource(TextInputFormat.class, hashtagInput, "Hashtags");
        hashtags.setParameter(TextInputFormat.CHARSET_NAME, TextInputFormat.DEFAULT_CHARSET_NAME);		// comment out this line for UTF-8 inputs

        MapContract loadHashtags = MapContract.builder(LoadHashtagMap.class)
                .input(hashtags)
                .name("Load Hashtags")
                .build();



        /*
         * Flow
         */


        MapContract cleanText = MapContract.builder(CleanTextMap.class)
                .input(tokenizeMapper)
                .name("Clean Tweets")
                .build();

        MapContract sentimentAnalysis = MapContract.builder(SentimentAnalysisMap.class)
                .input(tokenizeMapper)
                .name("Sentiment Analysis")
                .build();

        MapContract splitSentence = MapContract.builder(SplitSentenceMap.class)
                .input(cleanText)
                .name("Split to words")
                .build();
        CoGroupContract englishGroup = CoGroupContract.builder(EnglishDictionaryCoGroup.class, PactString.class, 0, 0)
                .input1(splitSentence)
                .input2(dictionaryMap)
                .name("Group en-words")
                .build();
        ReduceContract countEnglishWords = new ReduceContract.Builder(CountEnglishWordsReduce.class, PactLong.class, 0)
                .input(englishGroup)
                .name("Count en-words")
                .build();
        MatchContract dictionaryFilter = MatchContract.builder(DictionaryFilterMatch.class, PactLong.class, 0, 0)
                .input1(countEnglishWords)
                .input2(cleanText)
                .name("Filter English Tweets")
                .build();
        dictionaryFilter.setParameter(WORDS_TRESHOLD, wordTreshold);

        MatchContract tweetPolarityMatch = MatchContract.builder(TweetPolarityMatch.class, PactLong.class, 0, 0)
                .input1(dictionaryFilter)
                .input2(sentimentAnalysis)
                .name("Tweet Polairty Match")
                .build();

        MapContract userExtract = MapContract.builder(UserExtractMap.class)
                .input(tweetPolarityMatch)
                .name("Extract User")
                .build();

        ReduceContract countUserTweets = new ReduceContract.Builder(CountUserTweetsReduce.class, PactInteger.class, 0)
                .input(userExtract)
                .name("Count user tweets")
                .build();



        MapContract timePolarity = MapContract.builder(TimePolarityMap.class)
                .input(tweetPolarityMatch)
                .name("Tweet Time & Polarity")
                .build();

        MapContract userTweetExtract = MapContract.builder(UserTweetExtractMap.class)
                .input(tweetPolarityMatch)
                .name("Extract User")
                .build();


        MatchContract hashtagUserMatch = MatchContract.builder(HashtagUserMatch.class, PactLong.class, 0, 0)
                .input1(userTweetExtract)
                .input2(loadHashtags)
                .name("Hashtag User Match")
                .build();

        ReduceContract countHashtagUsers = new ReduceContract.Builder(CountHashtagUsersReduce.class, PactString.class, 0)
                .input(hashtagUserMatch)
                .name("Count user tweets")
                .build();

        MatchContract hashtagPolarityMatch = MatchContract.builder(HashtagPolarityMatch.class, PactLong.class, 0, 0)
                .input1(timePolarity)
                .input2(loadHashtags)
                .name("Hashtag Polarity Match")
                .build();



        ReduceContract sumHashtagPolarity = new ReduceContract.Builder(SumHashtagPolarityReduce.class, PactString.class, 0)
                .input(hashtagPolarityMatch)
                .name("Sum Hashtag polarities")
                .build();

        ReduceContract countHashtagTweets = new ReduceContract.Builder(CountHashtagTweetsReduce.class, PactString.class, 0)
                .input(hashtagPolarityMatch)
                .name("Count Hashtag Tweets")
                .build();

        CoGroupContract timestampPolarityGroup = CoGroupContract.builder(HashtagPolarityCoGroup.class, PactString.class, 0, 0)
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


        FileDataSink[] outputs = new FileDataSink[8];
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

        i++;
        outputs[i] = new FileDataSink(RecordOutputFormat.class, outputHashtagSentiment, timestampPolarityGroup, "Hahtag Polairties ");
        RecordOutputFormat.configureRecordFormat(outputs[i])
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(PactString.class, 0)
                .field(PactInteger.class, 1)
                .field(PactDouble.class, 2)
                .field(PactDouble.class, 3)
                .field(PactDouble.class, 4);


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


        HashSet outputsSet = new HashSet<FileDataSink>();
        outputsSet.addAll(Arrays.asList(outputs));

        Plan plan = new Plan(outputsSet, "Tweet Statistics Process");

        plan.setDefaultParallelism(noSubTasks);
        return plan;
    }

    @Override
    public String getDescription() {
        return "Parameters: [noSubStasks] [dataInput] [dictionaryFile] [wordsTreshold] [hashtagInput] [outputDir]";
    }
}