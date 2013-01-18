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
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsFirst;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsSecond;
import eu.stratosphere.pact.common.stubs.StubAnnotation.OutCardBounds;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Perform cleansing phase of tweets. 
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class TweetCleanse implements PlanAssembler, PlanAssemblerDescription {

    /**
     * Converts a PactRecord containing one string in to multiple string/integer pairs.
     * The string is tokenized by whitespaces. For each token a new record is emitted,
     * where the token is the first field and an Integer(1) is the second field.
     */
    @ConstantFields(fields = {})
    @OutCardBounds(lowerBound = 0, upperBound = OutCardBounds.UNBOUNDED)
    public static class TokenizeLine extends MapStub {
        // initialize reusable mutable objects
        private final PactRecord outputRecord = new PactRecord();
        private final PactString line = new PactString(); 
        private final PactLong tid = new PactLong();
        private final PactInteger uid = new PactInteger();
        private final PactString tweet = new PactString();
        private final PactInteger numWords = new PactInteger();
        
        @Override
        public void map(PactRecord record, Collector<PactRecord> collector) {
            line.setValue(record.getField(0, PactString.class));
            String[] splittedLine = line.toString().split(",");
            
            tid.setValue(Long.valueOf(splittedLine[0]));
            uid.setValue(Integer.parseInt(splittedLine[1]));
            tweet.setValue(splittedLine[2]);
            numWords.setValue(StringUtils.numWords(splittedLine[2]));
            
            this.outputRecord.setField(0, tid);
            this.outputRecord.setField(1, uid);
            this.outputRecord.setField(2, tweet);
            this.outputRecord.setField(3, numWords);
            collector.collect(this.outputRecord);
        }
    }

    /**
     * Sums up the counts for a certain given key. The counts are assumed to be at position <code>1</code>
     * in the record. The other fields are not modified.
     */
    @ConstantFields(fields = {0})
    @OutCardBounds(lowerBound = 0, upperBound = 1)
    public static class CleanText extends MapStub {
        private PactString tweet = new PactString();
        private Pattern pAt = Pattern.compile("(@[a-zA-Z0-9]+)");
        private Pattern pUrl = Pattern.compile("(((ht|f)tp(s?)\\://)\\S+)");
        private Pattern pHash = Pattern.compile("#");
        
        @Override
        public void map(PactRecord pr, Collector<PactRecord> records) throws Exception {
            tweet = pr.getField(2, PactString.class);
            String text = tweet.getValue();
            
            Matcher matchAt  = null;
            Matcher matchUrl  = null;
            Matcher matchHash  = null;

            matchAt = pAt.matcher(text);
            text = matchAt.replaceAll("");
            matchUrl = pUrl.matcher(text);
            text = matchUrl.replaceAll("");
            matchHash = pHash.matcher(text);
            text = matchHash.replaceAll("");
            text = StringUtils.removeStopwords(text);
            if (text != null) {
                tweet.setValue(text);
                pr.setField(2, tweet);
                records.collect(pr);
            }
        }
    }
    
    @ConstantFields(fields = {})
    @OutCardBounds(lowerBound = 1, upperBound = OutCardBounds.UNBOUNDED)
    public static class SplitSentence extends MapStub {
        private PactString line;
        private PactLong tid; 
        private PactString word = new PactString();
        private StringUtils.WhitespaceTokenizer tokenizer = new StringUtils.WhitespaceTokenizer();
        private PactRecord output = new PactRecord();
        
        @Override
        public void map(PactRecord pr, Collector<PactRecord> records) throws Exception {
            tid = pr.getField(0, PactLong.class);
            line = pr.getField(2, PactString.class);
            tokenizer.setStringToTokenize(line);
            while (tokenizer.next(word)) {
                output.setField(0, word);
                output.setField(1, tid);
                records.collect(output);
            }
        }
    }    
    
    
    
    @ConstantFields(fields = {})
    @OutCardBounds(lowerBound = 0, upperBound = OutCardBounds.UNBOUNDED)
    public static class LoadDictionary extends MapStub {
        private PactString word = new PactString();

        @Override
        public void map(PactRecord pr, Collector<PactRecord> records) throws Exception {
            word = pr.getField(0, PactString.class);
            StringUtils.toLowerCase(word);
            pr.setField(0, word);
            records.collect(pr);
        }
    }
    
    
    /**
     * Sums up the counts for a certain given key. The counts are assumed to be at position <code>1</code>
     * in the record. The other fields are not modified.
     */
    @ConstantFieldsFirst(fields = {})
    @ConstantFieldsSecond(fields = {})
    @OutCardBounds(lowerBound = 0, upperBound = 1)
    public static class EnglishDictionaryGroup extends CoGroupStub {        
        private PactInteger one = new PactInteger(1);
        private PactRecord outputRecord  = new PactRecord();
        private PactLong tid;
        
        @Override
        public void coGroup(Iterator<PactRecord> wordMatch, Iterator<PactRecord> dictMatch, Collector<PactRecord> records) {
            PactRecord pr;
            if (dictMatch.hasNext()) {
                while(wordMatch.hasNext()) {
                    pr = wordMatch.next();
                    
                    tid = pr.getField(1, PactLong.class);
//                    System.out.printf("tid %d matches %s\n", tid.getValue(), pr.getField(0, PactString.class));
                    outputRecord.setField(0, tid);
                    outputRecord.setField(1, one);
                    records.collect(outputRecord);
                }
            }             
        }
    }
    
    @ConstantFields(fields = {0})
    @OutCardBounds(lowerBound = 1, upperBound = 1)
    public static class CountEnglighWords extends ReduceStub {
        PactInteger numWords = new PactInteger();
        
        @Override
        public void reduce(Iterator<PactRecord> matches, Collector<PactRecord> records) throws Exception {
            PactRecord pr = null;
            int sum = 0;
            
            while (matches.hasNext()) {
                pr = matches.next();
                sum += pr.getField(1, PactInteger.class).getValue();
            }
            numWords.setValue(sum);
            //System.out.printf("Tweet: %d, english words: %d\n", pr.getField(0, PactLong.class).getValue(), sum);
            pr.setField(1, numWords);
            records.collect(pr);
        }
    }    
    
    
    @ConstantFieldsSecond(fields = {0})
    @OutCardBounds(lowerBound = 0, upperBound = 1)
    public static class DictionaryMatch extends MatchStub {

        @Override
        public void match(PactRecord english, PactRecord sentence, Collector<PactRecord> records) throws Exception {
            int englishWords = english.getField(1, PactInteger.class).getValue();
            int totalWords = sentence.getField(3, PactInteger.class).getValue();

            //            System.out.printf("match, enW %d, tW %d, ratio %f\n", englishWords, totalWords, englishWords/(double)totalWords);
            if (englishWords/(double)totalWords > .2) 
                records.collect(sentence);
        }
        
    }
    
    
//    @ConstantFields(fields = {0})
//    @OutCardBounds(lowerBound = 0, upperBound = OutCardBounds.UNBOUNDED)
//    public static class FilterUsers extends CoGroupStub {
//        private PactRecord outputRecord = new PactRecord();
//        
//        @Override
//        public void coGroup(Iterator<PactRecord> itrtr, Iterator<PactRecord> itrtr1, Collector<PactRecord> clctr) {
//            int validTweets; 
//
//            if (itrtr1.hasNext()) {
//                
//                
//                clctr.collect(null);
//            }
//        }
//    }
    
    @Override
    public Plan getPlan(String... args) {
        int noSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
        String dataInput = (args.length > 1 ? args[1] : "");
        String dictionaryInput = (args.length > 2 ? args[2] : "");
        String output    = (args.length > 3 ? args[3] : "");
        
        FileDataSource source = new FileDataSource(TextInputFormat.class, dataInput, "Input Lines");
        source.setParameter(TextInputFormat.CHARSET_NAME, TextInputFormat.DEFAULT_CHARSET_NAME);		// comment out this line for UTF-8 inputs
        MapContract tokenizeMapper = MapContract.builder(TokenizeLine.class)
                .input(source)
                .name("Tokenize Lines")
                .build();
        FileDataSource dict = new FileDataSource(TextInputFormat.class, dictionaryInput, "English words");
        dict.setParameter(TextInputFormat.CHARSET_NAME, TextInputFormat.DEFAULT_CHARSET_NAME);		// comment out this line for UTF-8 inputs
        MapContract dictionaryMap = MapContract.builder(LoadDictionary.class)
                .input(dict)
                .name("Load dictionary")
                .build();
        MapContract cleanText = MapContract.builder(CleanText.class)
                .input(tokenizeMapper)
                .name("Clean Tweets")
                .build();
        MapContract splitSentence = MapContract.builder(SplitSentence.class)
                .input(cleanText)
                .name("Split to words")
                .build();
        CoGroupContract englishGroup = CoGroupContract.builder(EnglishDictionaryGroup.class, PactString.class, 0, 0)
                .input1(splitSentence)
                .input2(dictionaryMap)
                .name("Group en-words")
                .build();
        ReduceContract countEnglishWords = new ReduceContract.Builder(CountEnglighWords.class, PactLong.class, 0)
                .input(englishGroup)
                .name("Count en-words")
                .build();
        MatchContract dictionaryFilter = MatchContract.builder(DictionaryMatch.class, PactLong.class, 0, 0)
                .input1(countEnglishWords)
                .input2(cleanText)
                .name("Dictionary Filter")
                .build();

        FileDataSink out = new FileDataSink(RecordOutputFormat.class, output, dictionaryFilter, "Pruned tweets");
        RecordOutputFormat.configureRecordFormat(out)
                .recordDelimiter('\n')
                .fieldDelimiter('\t')
                .lenient(true)
                .field(PactLong.class, 0)
                .field(PactString.class, 2)
                .field(PactInteger.class, 3);

    //        CoGroupContract countValidTweets = CoGroupContract.builder(FilterUsers.class, PactLong.class, 0, 0)
    //                .input1(englishPruning)
    //                .input2(tokenizeMapper)
    //                .name("Group users per number of valid tweets")
    //                .build();
    //        
    //        FileDataSink out = new FileDataSink(RecordOutputFormat.class, output, countValidTweets, "Word Counts");
    //        RecordOutputFormat.configureRecordFormat(out)
    //                .recordDelimiter('\n')
    //                .fieldDelimiter('\t')
    //                .lenient(true)
    //                .field(PactInteger.class, 0)
    //                .field(PactInteger.class, 1);

        Plan plan = new Plan(out, "Tweet Cleaning Example");
        plan.setDefaultParallelism(noSubTasks);
        return plan;
    }

    @Override
    public String getDescription() {
        return "Parameters: [noSubStasks] [input] [dictionaryFile] [output]";
    }
}
