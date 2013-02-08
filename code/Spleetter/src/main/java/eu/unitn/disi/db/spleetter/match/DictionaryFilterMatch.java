package eu.unitn.disi.db.spleetter.match;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.unitn.disi.db.spleetter.TweetCleanse;

/**
 * Match tweets with tweets containing english words
 * filtering them depending on the ration
 * between the number of english words present in them
 *
 * 0 - tweet id
 * 1 - user id
 * 2 - text
 * 3 - num words
 * 4 - timestamp [h]
 *
 */
@StubAnnotation.ConstantFieldsSecond(fields = {0,1,2,3,4})
@StubAnnotation.OutCardBounds(lowerBound = 0, upperBound = 1)
public class DictionaryFilterMatch extends MatchStub {

    private double wordsThreshold; //	minimum ratio of english words/ totla words

    /**
    * Reads the filter literals from the configuration.
    *
    * @see eu.stratosphere.pact.common.stubs.Stub#open(eu.stratosphere.nephele.configuration.Configuration)
    */
   @Override
   public void open(Configuration parameters) {
           this.wordsThreshold = Double.parseDouble(parameters.getString(TweetCleanse.WORDS_TRESHOLD, "0.2"));
   }

    @Override
    public void match(PactRecord english, PactRecord sentence, Collector<PactRecord> records) throws Exception {
        int englishWords = english.getField(1, PactInteger.class).getValue();
        int totalWords = sentence.getField(3, PactInteger.class).getValue();
        if (englishWords/(double)totalWords > this.wordsThreshold) {
            if(TweetCleanse.DictionaryFilterMatchLog){
                System.out.printf("DFM out\n");
            }
            records.collect(sentence);
        }
    }

}
