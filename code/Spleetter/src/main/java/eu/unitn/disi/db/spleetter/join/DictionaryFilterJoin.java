package eu.unitn.disi.db.spleetter.join;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Join tweets with tweets containing english words
 * filtering them depending on the ration
 * between the number of english words present in them
 *
 * 0 - tweet id<br />
 * 1 - user id<br />
 * 2 - text<br />
 * 3 - num words<br />
 * 4 - timestamp [h]<br />
 *
 */
@FunctionAnnotation.ConstantFieldsSecond({0,1,2,3,4})
public class DictionaryFilterJoin extends JoinFunction{
    private static final Log LOG = LogFactory.getLog(DictionaryFilterJoin.class);
    private long counter = 0;
    private double wordsThreshold; //	minimum ratio of english words/ totla words



    /**
    * Reads the filter literals from the configuration.
    *
    * @see eu.stratosphere.pact.common.stubs.Function#open(eu.stratosphere.nephele.configuration.Configuration)
    */
   @Override
   public void open(Configuration parameters) {
           this.wordsThreshold = Double.parseDouble(parameters.getString(TweetCleanse.WORDS_TRESHOLD, "0.2"));
   }

    @Override
    public void join(Record english, Record sentence, Collector<Record> records) throws Exception {
        int englishWords = english.getField(1, IntValue.class).getValue();
        int totalWords = sentence.getField(3, IntValue.class).getValue();
        if (englishWords/(double)totalWords > this.wordsThreshold) {
            records.collect(sentence);
            if(TweetCleanse.DictionaryFilterJoinLog){
                //System.out.printf("DFM out\n");
                this.counter++;
            }
        }
    }

    @Override
    public void close() throws Exception {
        if(TweetCleanse.DictionaryFilterJoinLog){
            LOG.fatal(counter);
        }
    	super.close();
    }

}
