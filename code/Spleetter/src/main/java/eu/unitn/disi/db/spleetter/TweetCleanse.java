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
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.unitn.disi.db.spleetter.cogroup.EnglishDictionaryCoGroup;
import eu.unitn.disi.db.spleetter.map.CleanTextMap;
import eu.unitn.disi.db.spleetter.map.LoadDictionaryMap;
import eu.unitn.disi.db.spleetter.map.LoadHashtagMap;
import eu.unitn.disi.db.spleetter.map.SplitSentenceMap;
import eu.unitn.disi.db.spleetter.map.TokenizeLineMap;
import eu.unitn.disi.db.spleetter.map.UserExtractMap;
import eu.unitn.disi.db.spleetter.match.DictionaryFilterMatch;
import eu.unitn.disi.db.spleetter.reduce.CountEnglishWordsReduce;
import eu.unitn.disi.db.spleetter.reduce.CountUserTweetsReduce;
import java.util.HashSet;

/**
 * Perform cleansing phase of tweets. 
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class TweetCleanse implements PlanAssembler, PlanAssemblerDescription {

        
    @Override
    public Plan getPlan(String... args) {
        int noSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
        String dataInput = (args.length > 1 ? args[1] : "");
        String dictionaryInput = (args.length > 2 ? args[2] : "");
        String hashtagInput = (args.length > 3 ? args[3] : "");
        String output1    = (args.length > 4 ? args[4]+"/tweets" : "");
        String output2    = (args.length > 4 ? args[4]+"/users" : "");
        
        
        FileDataSource source = new FileDataSource(TextInputFormat.class, dataInput, "Input Lines");
        source.setParameter(TextInputFormat.CHARSET_NAME, TextInputFormat.DEFAULT_CHARSET_NAME);		// comment out this line for UTF-8 inputs
        MapContract tokenizeMapper = MapContract.builder(TokenizeLineMap.class)
                .input(source)
                .name("Tokenize Lines")
                .build();
        FileDataSource dict = new FileDataSource(TextInputFormat.class, dictionaryInput, "English words");
        dict.setParameter(TextInputFormat.CHARSET_NAME, TextInputFormat.DEFAULT_CHARSET_NAME);		// comment out this line for UTF-8 inputs
        MapContract dictionaryMap = MapContract.builder(LoadDictionaryMap.class)
                .input(dict)
                .name("Load dictionary")
                .build();
        MapContract cleanText = MapContract.builder(CleanTextMap.class)
                .input(tokenizeMapper)
                .name("Clean Tweets")
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
                .name("Dictionary Filter")
                .build();
        
        FileDataSource hashtags = new FileDataSource(TextInputFormat.class, hashtagInput, "Hashtags");
        dict.setParameter(TextInputFormat.CHARSET_NAME, TextInputFormat.DEFAULT_CHARSET_NAME);		// comment out this line for UTF-8 inputs

        MapContract loadHashtags = MapContract.builder(LoadHashtagMap.class)
                .input(dictionaryFilter)
                .name("Load Hashtags")
                .build();

                
        MapContract userExtract = MapContract.builder(UserExtractMap.class)
                .input(dictionaryFilter)
                .name("Extract User")
                .build();
        ReduceContract countUserTweets = new ReduceContract.Builder(CountUserTweetsReduce.class, PactInteger.class, 0)
                .input(userExtract)
                .name("Count user tweets")
                .build();
        
        
        
        
        FileDataSink out1 = new FileDataSink(RecordOutputFormat.class, output1, dictionaryFilter, "Pruned tweets");
        RecordOutputFormat.configureRecordFormat(out1)
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(PactLong.class, 0)
                .field(PactInteger.class, 1)
                .field(PactString.class, 2)
                .field(PactInteger.class, 3);


        FileDataSink out2 = new FileDataSink(RecordOutputFormat.class, output2, countUserTweets, "User tweets count");
        RecordOutputFormat.configureRecordFormat(out2)
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(PactInteger.class, 0)
                .field(PactInteger.class, 1);

        HashSet outputs = new HashSet<FileDataSink>();
        outputs.add(out1);
        outputs.add(out2);
        Plan plan = new Plan(outputs, "Tweet Cleaning Example");
        
        plan.setDefaultParallelism(noSubTasks);
        return plan;
    }

    @Override
    public String getDescription() {
        return "Parameters: [noSubStasks] [input] [dictionaryFile] [output]";
    }
}
