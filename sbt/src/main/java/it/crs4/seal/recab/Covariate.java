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

import it.crs4.seal.common.AbstractTaggedMapping;

public interface Covariate
{
	/**
	 * Apply this covariate to the mapping m.
	 */
	public void applyToMapping(AbstractTaggedMapping m);

	/**
	 * Get the serialized covariate value for the loaded mapping at position pos (0-based).
	 */
	public String getValue(int pos);
}
