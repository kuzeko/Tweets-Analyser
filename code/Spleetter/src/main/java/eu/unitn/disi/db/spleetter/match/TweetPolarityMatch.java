package eu.unitn.disi.db.spleetter.match;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;

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
@StubAnnotation.ConstantFieldsFirst(fields = {0,1,2,3,4})
@StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = 1)
public class TweetPolarityMatch extends MatchStub {
    private PactDouble negPolarity = new PactDouble();
    private PactDouble posPolarity = new PactDouble();

    @Override
    public void match(PactRecord englishTweet, PactRecord polarityValue, Collector<PactRecord> records) throws Exception {

        negPolarity = polarityValue.getField(2, PactDouble.class);
        posPolarity = polarityValue.getField(3, PactDouble.class);
        englishTweet.setField(5, negPolarity);
        englishTweet.setField(6, posPolarity);
        records.collect(englishTweet);
        System.out.printf("PM %s\n", englishTweet.getField(0, PactLong.class));
    }

}
