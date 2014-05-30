package eu.unitn.disi.db.eventer.reduce;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import eu.unitn.disi.db.eventer.TweetPeaks;
import eu.unitn.disi.db.eventer.utils.Peaks;
import eu.unitn.disi.db.eventer.utils.Peaks.Peak;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * For each ngram timeseries produces a list of peaks computed
 * taking into account the frequence of the ngram proportional
 * to the number of tweets per hour
 *
 * 0 - word
 * 1 - peak start
 * 2 - peak top
 * 3 - peak end
 * 4 - day of peak
 * 5 - value at peak
 */
@FunctionAnnotation.ConstantFields({})
public class DetectProportionalPeaksReduce extends ReduceFunction {

    private static final Log LOG = LogFactory.getLog(DetectProportionalPeaksReduce.class);
    private long counter = 0;
    private double minAmplitude;
    private double minSlope;
    private int minSamples;
    private double smoothing;
    private Record prOut = new Record(6);
    private StringValue ngram = new StringValue();
    private StringValue start = new StringValue();
    private StringValue top = new StringValue();
    private StringValue end = new StringValue();
    private StringValue peakDate = new StringValue();
    private DoubleValue peakValue = new DoubleValue();

    /**
     * Reads the filter literals from the configuration.
     *
     * @see
     * eu.stratosphere.pact.common.stubs.Function#open(eu.stratosphere.nephele.configuration.Configuration)
     */
    @Override
    public void open(Configuration parameters) {
        this.minAmplitude = Double.parseDouble(parameters.getString(TweetPeaks.MIN_AMPLITUDE, "0.2"));
        this.minSlope = Double.parseDouble(parameters.getString(TweetPeaks.MIN_SLOPE, "0.2"));
        this.minSamples = Integer.parseInt(parameters.getString(TweetPeaks.MIN_SAMPLES, "1"));
        this.smoothing = Double.parseDouble(parameters.getString(TweetPeaks.SMOOTHING, "0.2"));
        //LOG.fatal("Peaks parameters:" + " minAmp " + this.minAmplitude + " minSlope " + this.minSlope + " minSamples " + this.minSamples + " smoothing " + this.smoothing);
    }

    @Override
    public void reduce(Iterator<Record> ngramRecords, Collector<Record> records) throws Exception {
        Record pr = null;
        List<Double> t = new ArrayList<Double>();
        List<Double> v = new ArrayList<Double>();
        List<String> dates = new ArrayList<String>();

        double nextInstant = 0;
        double lastCount = 0;
        double lastTotalCount = 0;
        double overallAvg = 0;
        double actualAvg = 0;
        int isFake = 0;

        t.clear();
        v.clear();
        dates.clear();
        int countFake = 0;
        while (ngramRecords.hasNext()) {
            pr = ngramRecords.next();

            lastCount = pr.getField(2, IntValue.class).getValue();
            lastTotalCount = pr.getField(4, IntValue.class).getValue();
            // Compute percentage over total count with magnifying parameter
            lastCount = (100*1000*lastCount)/lastTotalCount;
            isFake = pr.getField(3, IntValue.class).getValue();
            overallAvg += lastCount;
            countFake += isFake ;

            if(isFake==0){
                actualAvg+= lastCount;
            }

            t.add(nextInstant);
            v.add(lastCount);

            dates.add(pr.getField(0, StringValue.class).getValue());
            nextInstant++;
        }

        ngram = pr.getField(1, StringValue.class);

        if (v.size() > 2) {

            actualAvg  /= (v.size()- countFake) ;
            overallAvg /= v.size();

            if(actualAvg > this.minSamples) {
                double[] plainV = getPlainArray(v);

                if (TweetPeaks.DetectPeaksReduceLog) {
                    DecimalFormat df = new DecimalFormat("#####.###");
                    LOG.fatal("AVG for # " + ngram + " " + df.format(overallAvg) + " and " + df.format(actualAvg) +  " over "+ plainV.length + " Data points of which fake "+ countFake);
                }


                List<Peak> peaks = Peaks.extractPeaks(getPlainArray(t), getPlainArray(v), minAmplitude, minSlope, minSamples, smoothing, 1);


                for (Peak peak : peaks) {

                    start.setValue(dates.get(peak.getPeakBegin()));
                    top.setValue(dates.get(peak.getPeakTop()));
                    end.setValue(dates.get(peak.getPeakEnd()));
                    peakDate.setValue(dates.get(peak.getPeakTop()).substring(0, 10));
                    peakValue.setValue(peak.getPeakValue());

                    prOut.setField(0, ngram);
                    prOut.setField(1, start);
                    prOut.setField(2, top);
                    prOut.setField(3, end);
                    prOut.setField(4, peakDate);
                    prOut.setField(5, peakValue);
                    records.collect(prOut);

                    if (TweetPeaks.DetectProportioanlPeaksReduceLog) {
                        this.counter++;
                    }

                }
            }
        }

        t.clear();
        v.clear();


    }

    private static double[] getPlainArray(List<Double> toConvert) {
        double[] converted = new double[toConvert.size()];
        int i = 0;
        for (Double d : toConvert) {
            converted[i++] = d.doubleValue();
        }
        return converted;
    }

    @Override
    public void close() throws Exception {
        if (TweetPeaks.DetectProportioanlPeaksReduceLog) {
            LOG.fatal(counter);
        }
        super.close();
    }
}
