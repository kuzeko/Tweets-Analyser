package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Converts a PactRecord containing one string in to multiple integer pairs.
 * 0 - tweet id
 * 1 - user id
 * 2 - hashtag id
 */
@StubAnnotation.ConstantFields({})
public class LoadHashtagMap extends MapStub {

    private static final Log LOG = LogFactory.getLog(LoadHashtagMap.class);
    private long counter = 0;


    private PactRecord outputRecord = new PactRecord();
    private PactLong tid = new PactLong();
    private PactLong uid = new PactLong();
    private PactInteger hid = new PactInteger();
    private final PactString line = new PactString();

    @Override
    public void map(PactRecord pr, Collector<PactRecord> records) throws Exception {
        line.setValue(pr.getField(0, PactString.class));
        String[] splittedLine = line.toString().split(",");

        tid.setValue(Long.parseLong(splittedLine[0]));
        uid.setValue(Long.parseLong(splittedLine[1]));
        hid.setValue(Integer.parseInt(splittedLine[2]));

        outputRecord.setField(0, tid);
        outputRecord.setField(1, uid);
        outputRecord.setField(2, hid);

        records.collect(outputRecord);
        if (TweetCleanse.LoadHashtagMapLog) {
            //System.out.printf("CEWR out %d \n", pr.getField(0, PactLong.class).getValue() );
            this.counter++;
        }
    }

    @Override
    public void close() throws Exception {
        if (TweetCleanse.LoadHashtagMapLog) {
            LOG.fatal(counter);
        }
        super.close();
    }
}
