/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 * Filters the tweet record keeping only the user id
 * 0 - user id
 */
@StubAnnotation.ConstantFields(fields = {})
@StubAnnotation.OutCardBounds(lowerBound = 1, upperBound = 1)
public class UserExtractMap extends MapStub {
    private PactRecord pr2 = new PactRecord(1);
    @Override
    public void map(PactRecord pr, Collector<PactRecord> records) throws Exception {        
        pr2.setField(0, pr.getField(1, PactInteger.class ));
        records.collect(pr2);
    }
}
