package eu.unitn.disi.db.spleetter.match;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.unitn.disi.db.spleetter.TweetCleanse;

/**
 * Joins the cleaned english tweets with their correspondent polarities
 * appending them to the end of the record
 *
 * 0 - tweet id
 * 1 - user id
 * 2 - tweet text
 * 3 - num of words in the original tweet
 * 4 - timestamp [h]
 * 5 - negative polarity
 * 6 - positive polarity
 */
@StubAnnotation.ConstantFieldsFirst(fields = {0,2,3,4})
@StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = 1)
public class TweetPolarityMatch extends MatchStub {
    private PactRecord output = new PactRecord(7);
    @Override
    public void match(PactRecord englishTweet, PactRecord polarityValue, Collector<PactRecord> records) throws Exception {

        output.setField(0, polarityValue.getField(0, PactLong.class));
        output.setField(1, polarityValue.getField(1, PactInteger.class));
        output.setField(2, englishTweet.getField(2, PactString.class));
        output.setField(3, englishTweet.getField(3, PactInteger.class));
        output.setField(4, englishTweet.getField(4, PactString.class));
        output.setField(5, polarityValue.getField(2, PactDouble.class));
        output.setField(6, polarityValue.getField(3, PactDouble.class));
        if(TweetCleanse.TweetPolarityMatchLog){
            System.out.printf("TPM out\n");
        }

        records.collect(output);
    }

}
