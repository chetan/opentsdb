//
// TestJsonHandler.java
//
// Copyright 2012, NextPage Inc. All rights reserved.
//

package net.opentsdb.tsd;

import com.google.common.collect.HashMultimap;
import net.opentsdb.core.DataPointImpl;
import net.opentsdb.core.DataPoints;
import org.json.JSONException;
import org.junit.Test;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestJsonHandler
{
	private List<DataPoints> dataPoints = new ArrayList<DataPoints>();

	private StringWriter outputWriter = new StringWriter();
	private long startTime = 5;
	private long endTime = 23;

	@Test
	public void testEmptyDataPoints() throws JSONException
	{
		JsonHandler.buildJsonResponse(outputWriter, dataPoints, startTime, endTime);

		String results = "{\"results\":[]}";

		assertEquals(results, outputWriter.toString());
	}

	@Test
	public void testNoTagsNoValues() throws JSONException
	{
		FakeDataPoints fdp = new FakeDataPoints("test.name.one", HashMultimap.<String, String>create());
		dataPoints.add(fdp);
		JsonHandler.buildJsonResponse(outputWriter, dataPoints, startTime, endTime);

		String results = "{\"results\":[{\"name\":\"test.name.one\",\"tags\":[],\"values\":[]}]}";

		assertEquals(results, outputWriter.toString());
	}

	@Test
	public void testNoTagsSingleMetric() throws JSONException
	{
		FakeDataPoints fdp = new FakeDataPoints("test.name.one", HashMultimap.<String, String>create());

		fdp.add(new DataPointImpl(6, 999));
		fdp.add(new DataPointImpl(7, 500));

		dataPoints.add(fdp);
		JsonHandler.buildJsonResponse(outputWriter, dataPoints, startTime, endTime);

		String results = "{\"results\":[{\"name\":\"test.name.one\",\"tags\":[],\"values\":[[6,999],[7,500]]}]}";

		assertEquals(results, outputWriter.toString());
	}

	@Test
	public void testNoTagsMultipleMetrics() throws JSONException
	{
		FakeDataPoints fdp1 = new FakeDataPoints("test.name.one", HashMultimap.<String, String>create());

		fdp1.add(new DataPointImpl(6, 999));
		fdp1.add(new DataPointImpl(7, 500));

		dataPoints.add(fdp1);

		FakeDataPoints fdp2 = new FakeDataPoints("test.name.two", HashMultimap.<String, String>create());

		fdp2.add(new DataPointImpl(6, 12.5));
		fdp2.add(new DataPointImpl(7, 14.5));

		dataPoints.add(fdp2);

		JsonHandler.buildJsonResponse(outputWriter, dataPoints, startTime, endTime);

		String results = "{\"results\":[{\"name\":\"test.name.one\",\"tags\":[],\"values\":[[6,999],[7,500]]},{\"name\":\"test.name.two\",\"tags\":[],\"values\":[[6,12.5],[7,14.5]]}]}";

		assertEquals(results, outputWriter.toString());
	}

	@Test
	public void testTagsSingleMetric() throws JSONException
	{
		HashMultimap<String, String> tags = HashMultimap.create();

		tags.put("host", "A");
		tags.put("host", "B");
		tags.put("client", "foo");

		FakeDataPoints fdp = new FakeDataPoints("test.name.one", tags);

		fdp.add(new DataPointImpl(6, 999));
		fdp.add(new DataPointImpl(7, 500));

		dataPoints.add(fdp);
		JsonHandler.buildJsonResponse(outputWriter, dataPoints, startTime, endTime);

		String results = "{\"results\":[{\"name\":\"test.name.one\",\"tags\":[{\"host\":[\"A\",\"B\"]},{\"client\":[\"foo\"]}],\"values\":[[6,999],[7,500]]}]}";

		assertEquals(results, outputWriter.toString());
	}
}
