package eu.unitn.disi.db.eventer.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import umontreal.iro.lecuyer.functionfit.SmoothingCubicSpline;

/**
 *
 * @author Tsytsarau Mikalai
 * @author Kuzeko
 */
public class Peaks implements Serializable {

    /**
     *
     * @param t
     * @param v
     * @param minAmplitude
     * @param minSlope
     * @param minSamples
     * @param smoothing
     * @param window
     */
    public static List<Peak> extractPeaks(double[] t, double[] v, double minAmplitude, double minSlope, int minSamples, double smoothing, int window) {
        SmoothingCubicSpline cs = new SmoothingCubicSpline(t, v, smoothing);
        List<Peak> peaks = new ArrayList<Peak>();

        boolean valid = false;
        double avg = 0;
        //float m1 = (float) avg;
        float n = 1;

        int peak_begin = 0;
        int peak_end = 0;
        int peak_top = 0;
        int size = v.length - 1;

        for (int i = 0; i < v.length; i++) {
            avg += v[i];
        }
        avg /= v.length;
        for (int i = 1; i < size; i++) {
            if (v[i] > minAmplitude * avg && ((cs.derivative(t[i-1]) > 0 && cs.derivative(t[i+1]) < 0) || (v[i-1] < v[i] && v[i+1] < v[i]))) {
                peak_top = i;
                if (v[i] < v[i-1]) {
                    peak_top = i-1;
                }
                if (v[i] < v[i+1]) {
                    peak_top = i+1;
                }

                peak_end = Math.min(peak_top + 1, size);
                peak_begin = Math.max(peak_top - 1, 0);

                // descend down the left slope
                while (peak_begin > 0 && cs.derivative(t[peak_begin] - window / 2) > 0) {
                    peak_begin--;
                }

                // descend down the right slope
                while (peak_end < size && cs.derivative(t[peak_end] + window / 2) < 0) {
                    peak_end++;
                }

                //now skip small values and derivatives (if overshoot)
                //we set minimum derivative to be 10 times smaller than minSlope
                while (peak_begin < peak_top && (v[peak_begin] <= 0 || 10 * cs.derivative(t[peak_begin] + window / 2) * window < avg * minSlope)) {
                    peak_begin++;
                }

                //if (peak_begin > 0 && v[peak_begin-1] > 0 && v[peak_begin-1] < v[peak_begin]) peak_begin--;
                while (peak_end > peak_top && (v[peak_end] <= 0 || 10 * cs.derivative(t[peak_end] - window / 2) * window > - avg * minSlope)) {
                    peak_end--;
                }

                //if (peak_end < size && v[peak_end+1] > 0 && v[peak_end+1] < v[peak_end]) peak_end++;
                i = Math.max(i, peak_end); //move only forward
                if (peak_end - peak_begin + 1 < minSamples) {
                    continue;
                }

                valid = false;
                for (int j = peak_begin; j < peak_end; j++) {
                   if (Math.abs(cs.derivative(t[j] + window / 2)) * window >= avg * minSlope) {
                        valid = true;
                    }
                }
                if (!valid) {
                    continue;
                }


                peaks.add(new Peak(peak_begin, peak_top, peak_end, v[peak_top]));
            }
        }

        return peaks;
    }

    public static final class Peak implements Serializable{
        private int peakBegin;
        private int peakTop;
        private int peakEnd;
        private double peakValue;

        private Peak(int peakBegin, int peakTop, int peakEnd, double avg) {
            this.peakBegin = peakBegin;
            this.peakTop = peakTop;
            this.peakEnd = peakEnd;
            this.peakValue = avg;
        }

        public int getPeakBegin() {
            return peakBegin;
        }

        public int getPeakTop() {
            return peakTop;
        }

        public int getPeakEnd() {
            return peakEnd;
        }

        public double getPeakValue() {
            return peakValue;
        }

    };
}
