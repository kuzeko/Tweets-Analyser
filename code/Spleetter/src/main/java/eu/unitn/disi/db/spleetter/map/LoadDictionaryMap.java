package eu.unitn.disi.db.spleetter.map;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.spleetter.TweetCleanse;
import eu.unitn.disi.db.spleetter.utils.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Converts a Record containing one string in to a string tuple
 * a tuple is a English word from the dictionary
 *
 * 0 - word
 */
@FunctionAnnotation.ConstantFields({})
public class LoadDictionaryMap extends MapFunction{

	private static final Log LOG = LogFactory.getLog(LoadDictionaryMap.class);
	private long counter = 0;

	private StringValue word = new StringValue();

    @Override
    public void map(Record pr, Collector<Record> records) throws Exception {
        word = pr.getField(0, StringValue.class);
        StringUtils.toLowerCase(word);
        pr.setField(0, word);
        records.collect(pr);
        if(TweetCleanse.LoadDictionaryMapLog){
            //System.out.printf("LDM out\n");
            this.counter++;
        }
    }

    @Override
    public void close() throws Exception {
        if(TweetCleanse.LoadDictionaryMapLog){
            LOG.fatal(counter);
        }
    	super.close();
    }
}
