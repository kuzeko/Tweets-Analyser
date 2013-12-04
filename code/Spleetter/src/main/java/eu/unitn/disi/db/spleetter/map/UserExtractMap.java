/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Filters the tweet record keeping only the user id
 * 0 - user id
 */
@StubAnnotation.ConstantFields({})
public class UserExtractMap extends MapStub {
    private static final Log LOG = LogFactory.getLog(UserExtractMap.class);
    private long counter = 0;
    private PactRecord pr2 = new PactRecord(1);


    @Override
    public void map(PactRecord pr, Collector<PactRecord> records) throws Exception {
        pr2.setField(0, pr.getField(1, PactLong.class ));
        records.collect(pr2);
        if(TweetCleanse.UserExtractMapLog){
          this.counter++;
        }
    }


    @Override
    public void close() throws Exception {
        if(TweetCleanse.UserExtractMapLog){
            LOG.fatal(counter);
        }
    	super.close();
    }


}
