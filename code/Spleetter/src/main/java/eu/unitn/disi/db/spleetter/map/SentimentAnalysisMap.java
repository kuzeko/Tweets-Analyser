package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import eu.unitn.disi.db.spleetter.utils.SentiStrengthWrapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Analyze the polairties from the tweet text,
 * appends negative and positive polarities to
 * the end of the record
 *
 * 0 - tweet id<br />
 * 1 - user id<br />
 * 2 - negative polarity<br />
 * 3 - positive polarity<br />
 */
@StubAnnotation.ConstantFields(fields = {0, 1})
@StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = 1)
public class SentimentAnalysisMap extends MapStub {

	private long counter = 0;

    private PactString tweet = new PactString();
    private PactDouble negPolarity = new PactDouble();
    private PactDouble posPolarity = new PactDouble();
    private PactRecord pr2 = new PactRecord(4);

    private static final Log LOG = LogFactory.getLog(SentimentAnalysisMap.class);

    @Override
    public void map(PactRecord pr, Collector<PactRecord> records) throws Exception {
        tweet = pr.getField(2, PactString.class);
        String text = tweet.getValue();

        SentiStrengthWrapper analyzer = SentiStrengthWrapper.getInstance();
        double[] polarities = analyzer.analyze(text);
        negPolarity.setValue(polarities[0]);
        posPolarity.setValue(polarities[1]);
//        negPolarity.setValue(-Math.random()*5);
//        posPolarity.setValue(Math.random()*5);

        if (text != null) {
            pr2.setField(0, pr.getField(0, PactLong.class));
            pr2.setField(1, pr.getField(1, PactInteger.class));
            pr2.setField(2, negPolarity);
            pr2.setField(3, posPolarity);
            records.collect(pr2);
            if(TweetCleanse.SentimentAnalysisMapLog){
              //System.out.printf("SAM out %d \n", pr2.getField(0, PactLong.class).getValue() );
              this.counter++;
            }


        }
    }

    @Override
    public void close() throws Exception {
        if(TweetCleanse.SentimentAnalysisMapLog){
            LOG.fatal(counter);
        }
    	super.close();
    }
}
