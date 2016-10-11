// Copyright (C) 2011-2016 CRS4.
//
// This file is part of Seal.
//
// Seal is free software: you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation, either version 3 of the License, or (at your option)
// any later version.
//
// Seal is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
// or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
// for more details.
//
// You should have received a copy of the GNU General Public License along
// with Seal.  If not, see <http://www.gnu.org/licenses/>.

package it.crs4.seal.recab;

import it.crs4.seal.common.IMRContext;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

public class RecabTableReducer
{
	public static final String CONF_SMOOTHING = "seal.recab.smoothing";
	public static final float CONF_SMOOTHING_DEFAULT = 0.0f;

	public static final String CONF_MAX_QSCORE = "seal.recab.max-qscore";
	public static final int CONF_MAX_QSCORE_DEFAULT = 40;

	public static final double PERROR_EPS = 0.0001;

	protected double smoothing = CONF_SMOOTHING_DEFAULT;
	protected int maxQscore = CONF_MAX_QSCORE_DEFAULT;
	protected ObservationCount sum = new ObservationCount();
	protected StringBuilder sbuilder = new StringBuilder(100);
	protected Text outputValue = new Text();

	public void setup(Configuration conf)
	{
		smoothing = conf.getFloat(CONF_SMOOTHING, CONF_SMOOTHING_DEFAULT);
		if (smoothing < 0.0)
			throw new IllegalArgumentException(CONF_SMOOTHING + " can't be less than 0");

		maxQscore = conf.getInt(CONF_MAX_QSCORE, CONF_MAX_QSCORE_DEFAULT);
		if (maxQscore <= 0)
			throw new IllegalArgumentException(CONF_MAX_QSCORE + " must be greater than 0");
	}

	public void reduce(Text key, Iterable<ObservationCount> values, IMRContext<Text, Text> context) throws IOException, InterruptedException
	{
		sum.set(0,0);
		sbuilder.delete(0, sbuilder.length());

		for (ObservationCount counts: values)
			sum.addToThis(counts);

		if (sum.getObservations() > 0)
		{
			outputValue.set(key);
			sbuilder.
				append(sum.getObservations()).append(RecabTable.TableDelim).
				append(sum.getMismatches()).append(RecabTable.TableDelim).
				append(empiricalQuality(sum));

			String tmp = sbuilder.toString();
			outputValue.append(tmp.getBytes(RecabTable.ASCII), 0, tmp.length());

			context.write(null, outputValue);
		}
	}

	protected int empiricalQuality(ObservationCount observations)
	{
		double perror = (observations.getMismatches() + smoothing) / (observations.getObservations() + smoothing) + PERROR_EPS;
		int score = (int)Math.rint(-10.0*Math.log10(perror));
		// bound score.  GATK bounds quality between 1 and MAX_REASONABLE_Q_SCORE
		if (score > maxQscore)
			score = maxQscore;
		else if (score <= 0)
			score = 1;

		return score;
	}
}
