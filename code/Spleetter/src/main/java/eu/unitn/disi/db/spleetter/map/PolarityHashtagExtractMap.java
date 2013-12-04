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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Filters the tweet record keeping only the tweet id, user id, timestamp and polarity
 * 0 - tweet id
 * 1 - user id
 * 2 - timestamp [h]
 * 3 - neg polarity
 * 4 - pos polarity
 */

@StubAnnotation.ConstantFields({0,1})
public class PolarityHashtagExtractMap extends MapStub {
    private static final Log LOG = LogFactory.getLog(PolarityHashtagExtractMap.class);
    private long counter = 0;
    private PactRecord pr2 = new PactRecord(5);

    @Override
    public void map(PactRecord pr, Collector<PactRecord> records) throws Exception {
        pr2.setField(0, pr.getField(0, PactLong.class ));
        pr2.setField(1, pr.getField(1, PactInteger.class ));
        pr2.setField(2, pr.getField(4, PactString.class ));
        pr2.setField(3, pr.getField(5, PactDouble.class ));
        pr2.setField(4, pr.getField(6, PactDouble.class ));
        records.collect(pr2);
        if(TweetCleanse.PolarityHashtagExtractMapLog){
          this.counter++;
        }

    }

    @Override
    public void close() throws Exception {
        if(TweetCleanse.PolarityHashtagExtractMapLog){
            LOG.fatal(counter);
        }
    	super.close();
    }

}
