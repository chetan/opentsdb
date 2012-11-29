//
// DataPointImpl.java
//
// Copyright 2012, NextPage Inc. All rights reserved.
//

package net.opentsdb.core;

public class DataPointImpl implements DataPoint
{
	private long timestamp;
	private boolean isLong;
	private long longValue;
	private double doubleValue;

	public DataPointImpl(long timestamp, long longValue)
	{
		this.timestamp = timestamp;
		isLong = true;
		this.longValue = longValue;
	}

	public DataPointImpl(long timestamp, double doubleValue)
	{
		this.timestamp = timestamp;
		isLong = false;
		this.doubleValue = doubleValue;
	}

	@Override
	public long timestamp()
	{
		return timestamp;
	}

	@Override
	public boolean isInteger()
	{
		return isLong;
	}

	@Override
	public long longValue()
	{
		return longValue;
	}

	@Override
	public double doubleValue()
	{
		return doubleValue;
	}

	@Override
	public double toDouble()
	{
		if (isLong)
			return (double)longValue;
		else
			return doubleValue;
	}
}
