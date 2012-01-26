// Copyright (C) 2011-2012 CRS4.
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

import org.apache.hadoop.conf.Configuration;

/**
 * Covariate that returns the read's RG value for each base.
 */
public class ReadGroupCovariate implements Covariate
{
	public static final String CONF_RG_COVARIATE_DEFAULT_RG = "seal.recab.rg-covariate.default-rg";

	protected String defaultRg = null;
	protected String currentRg = null;

	public ReadGroupCovariate()
	{
	}

	public ReadGroupCovariate(Configuration conf)
	{
		if (conf != null)
			defaultRg = conf.get(CONF_RG_COVARIATE_DEFAULT_RG);
	}

	public void applyToMapping(AbstractSamMapping m)
	{
		try {
			currentRg = m.getTag("RG");
		}
		catch (NoSuchFieldException e) 
		{
			if (defaultRg == null)
			{
				throw new RuntimeException("Read doesn't have a read group tag. If you'd like to set a default read group set the configuration property " + CONF_RG_COVARIATE_DEFAULT_RG + ".\nRecord: " + m);
			}
			else
				currentRg = defaultRg;
		}
	}

	public String getValue(int pos)
	{
		return currentRg;
	}
}
