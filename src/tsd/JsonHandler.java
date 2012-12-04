// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tsd;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.HashMultimap;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.json.JSONException;
import org.json.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.Aggregator;
import net.opentsdb.core.Aggregators;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.stats.Histogram;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.uid.NoSuchUniqueName;

/**
 Stateless handler of HTTP graph requests (the {@code /q} endpoint).
 */
final class JsonHandler implements HttpRpc
{

	private static final Logger LOG =
			LoggerFactory.getLogger(JsonHandler.class);

	/**
	 Number of times we had to do all the work up to running Gnuplot.
	 */
	private static final AtomicInteger graphs_generated
			= new AtomicInteger();
	/**
	 Number of times a graph request was served from disk, no work needed.
	 */
	private static final AtomicInteger graphs_diskcache_hit
			= new AtomicInteger();

	/**
	 Keep track of the latency of graphing requests.
	 */
	private static final Histogram graphlatency =
			new Histogram(16000, (short) 2, 100);

	/**
	 Keep track of the latency (in ms) introduced by running Gnuplot.
	 */
	private static final Histogram gnuplotlatency =
			new Histogram(16000, (short) 2, 100);


	/**
	 Directory where to cache query results.
	 */
	private final String cachedir;

	/**
	 Constructor.
	 */
	public JsonHandler()
	{
		// ArrayBlockingQueue does not scale as much as LinkedBlockingQueue in terms
		// of throughput but we don't need high throughput here.  We use ABQ instead
		// of LBQ because it creates far fewer references.
		cachedir = RpcHandler.getDirectoryFromSystemProp("tsd.http.cachedir");
	}

	public void execute(final TSDB tsdb, final HttpQuery query)
	{
		try
		{
			doQuery(tsdb, query);
		}
		catch (IOException e)
		{
			sendInternalError(query, e);
		}
		catch (IllegalArgumentException e)
		{
			sendBadRequestError(query, e);
		}
		catch (BadRequestException e)
		{
			sendBadRequestError(query, e);
		}
	}

	private void doQuery(final TSDB tsdb, final HttpQuery query)
			throws IOException
	{
		final String basepath = getGnuplotBasePath(query);
		final long start_time = getQueryStringDate(query, "start");
		if (start_time == -1)
		{
			throw BadRequestException.missingParameter("start");
		}
		long end_time = getQueryStringDate(query, "end");
		final long now = System.currentTimeMillis() / 1000;
		if (end_time == -1)
		{
			end_time = now;
		}
		/*final int max_age = computeMaxAge(query, start_time, end_time, now);
		if (!nocache && isDiskCacheHit(query, end_time, max_age, basepath))
		{
			return;
		}*/
		Query[] tsdbqueries;
		List<String> options;
		tsdbqueries = parseQuery(tsdb, query);
		options = query.getQueryStringParams("o");
		if (options == null)
		{
			options = new ArrayList<String>(tsdbqueries.length);
			for (int i = 0; i < tsdbqueries.length; i++)
			{
				options.add("");
			}
		}
		else if (options.size() != tsdbqueries.length)
		{
			throw new BadRequestException(options.size() + " `o' parameters, but "
					+ tsdbqueries.length + " `m' parameters.");
		}
		for (final Query tsdbquery : tsdbqueries)
		{
			try
			{
				tsdbquery.setStartTime(start_time);
			}
			catch (IllegalArgumentException e)
			{
				throw new BadRequestException("start time: " + e.getMessage());
			}
			try
			{
				tsdbquery.setEndTime(end_time);
			}
			catch (IllegalArgumentException e)
			{
				throw new BadRequestException("end time: " + e.getMessage());
			}
		}
		List<DataPoints> dataPointsList = new ArrayList<DataPoints>();

		final int nqueries = tsdbqueries.length;
		@SuppressWarnings("unchecked")
		int npoints = 0;
		for (int i = 0; i < nqueries; i++)
		{
			try
			{  // execute the TSDB query!
				// XXX This is slow and will block Netty.  TODO(tsuna): Don't block.
				// TODO(tsuna): Optimization: run each query in parallel.
				final DataPoints[] series = tsdbqueries[i].run();
				for (final DataPoints datapoints : series)
				{
					dataPointsList.add(datapoints);
					npoints += datapoints.aggregatedSize();
				}
			}
			catch (RuntimeException e)
			{
				logInfo(query, "Query failed (stack trace coming): "
						+ tsdbqueries[i]);
				throw e;
			}
			tsdbqueries[i] = null;  // free()
		}

		respondJson(query, basepath, dataPointsList, start_time, end_time);
	}


	/**
	 Collects the stats and metrics tracked by this instance.

	 @param collector The collector to use.
	 */
	public static void collectStats(final StatsCollector collector)
	{
		collector.record("http.latency", graphlatency, "type=graph");
		collector.record("http.latency", gnuplotlatency, "type=gnuplot");
		collector.record("http.graph.requests", graphs_diskcache_hit, "cache=disk");
		collector.record("http.graph.requests", graphs_generated, "cache=miss");
	}

	/**
	 Returns the base path to use for the Gnuplot files.
	 */
	private String getGnuplotBasePath(final HttpQuery query)
	{
		final Map<String, List<String>> q = query.getQueryString();
		q.remove("ignore");
		// Super cheap caching mechanism: hash the query string.
		final HashMap<String, List<String>> qs =
				new HashMap<String, List<String>>(q);
		// But first remove the parameters that don't influence the output.
		qs.remove("png");
		qs.remove("json");
		qs.remove("ascii");
		return cachedir + Integer.toHexString(qs.hashCode());
	}




	/*package*/ static void buildJsonResponse(Writer outputWriter,
			List<DataPoints> dataPointsList,
			long start_time,
			long end_time) throws JSONException
	{
		JSONWriter writer = new JSONWriter(outputWriter);

		writer.object().key("results").array();

		for (final DataPoints dp : dataPointsList)
		{
			final String metric = dp.metricName();

			writer.object();
			writer.key("name").value(metric);

			writer.key("tags").object();
			HashMultimap<String,String> inclusiveTags = dp.getInclusiveTags();
			for (String tagName : inclusiveTags.keySet())
			{
				writer.key(tagName).array();

				for (String tagValue : inclusiveTags.get(tagName))
				{
					writer.value(tagValue);
				}
				writer.endArray();
			}
			writer.endObject();

			writer.key("values").array();
			for (final DataPoint d : dp)
			{
				//Trim unwanted data points
				if (d.timestamp() < start_time || d.timestamp() > end_time)
					continue;

				writer.array().value(d.timestamp());
				if (d.isInteger())
				{
					writer.value(d.longValue());
				}
				else
				{
					final double value = d.doubleValue();
					if (value != value || Double.isInfinite(value))
					{
						throw new IllegalStateException("NaN or Infinity:" + value
								+ " d=" + d );
					}
					writer.value(value);
				}
				writer.endArray();
			}
			writer.endArray();
			writer.endObject();
		}
		writer.endArray().endObject();

	}

	private static void sendBadRequestError(HttpQuery query, Exception e)
	{
		logError(query, "Bad request", e);

		String msg = buildErrorResponse(e, null);

		query.sendReply(HttpResponseStatus.BAD_REQUEST, msg);
	}

	private static void sendInternalError(HttpQuery query, Exception e)
	{
		sendInternalError(query, e, null);
	}


	private static void sendInternalError(HttpQuery query, Exception e, String message)
	{
		logError(query, message, e);

		String msg = buildErrorResponse(e, message);

		query.sendReply(HttpResponseStatus.INTERNAL_SERVER_ERROR, msg);
	}

	private static String buildErrorResponse(Exception e, String message)
	{
		StringWriter sw = new StringWriter();
		JSONWriter jw = new JSONWriter(sw);

		try
		{
			String errorMessage = "";

			if (message != null)
				errorMessage = message+ ": ";

			errorMessage += e.getMessage();

			jw.object().key("errors").array();
			jw.value(errorMessage);

			jw.endArray();
			jw.endObject();
		}
		catch (JSONException e1)
		{
			LOG.error("Description of what failed:", e1);
			sw = new StringWriter();
			sw.write("{\"errors\":[\"Internal Error\"]}");
		}

		return sw.toString();
	}


	/**
	 Respond to a query that wants the output in ASCII.
	 <p/>
	 When a query specifies the "ascii" query string parameter, we send the
	 data points back to the client in plain text instead of sending a PNG.

	 @param query    The query we're currently serving.
	 cache the result in case of a cache hit.
	 @param basepath The base path used for the cached json file
	 @param dataPointsList list of data points to return
	 */
	private static void respondJson(final HttpQuery query,
			final String basepath,
			final List<DataPoints> dataPointsList,
			final long start_time,
			final long end_time)
	{


		final String path = basepath + ".txt";

		BufferedWriter fileWriter = null;
		try
		{
			fileWriter = new BufferedWriter(new FileWriter(path));

			buildJsonResponse(fileWriter, dataPointsList, start_time, end_time);

			fileWriter.flush();
		}
		catch (IOException e)
		{
			sendInternalError(query, e);
			return;
		}
		catch (JSONException e)
		{
			sendInternalError(query, e);
			return;
		}
		finally
		{
			try
			{
				if (fileWriter != null)
					fileWriter.close();
			}
			catch (IOException e)
			{
				sendInternalError(query, e);
				return;
			}
		}

		try
		{
			query.sendFile(path, 0);
		}
		catch (IOException e)
		{
			sendInternalError(query, e);
		}
	}

	/**
	 Parses the {@code /q} query in a list of {@link Query} objects.

	 @param tsdb  The TSDB to use.
	 @param query The HTTP query for {@code /q}.
	 @return The corresponding {@link Query} objects.
	 @throws BadRequestException      if the query was malformed.
	 @throws IllegalArgumentException if the metric or tags were malformed.
	 */
	private static Query[] parseQuery(final TSDB tsdb, final HttpQuery query)
	{
		final List<String> ms = query.getQueryStringParams("m");
		if (ms == null)
		{
			throw BadRequestException.missingParameter("m");
		}
		final Query[] tsdbqueries = new Query[ms.size()];
		int nqueries = 0;
		for (final String m : ms)
		{
			// m is of the following forms:
			//   agg:[interval-agg:][rate:]metric[{tag=value,...}]
			// Where the parts in square brackets `[' .. `]' are optional.
			final String[] parts = Tags.splitString(m, ':');
			int i = parts.length;
			if (i < 2 || i > 4)
			{
				throw new BadRequestException("Invalid parameter m=" + m + " ("
						+ (i < 2 ? "not enough" : "too many") + " :-separated parts)");
			}
			final Aggregator agg = getAggregator(parts[0]);
			i--;  // Move to the last part (the metric name).
			final HashMap<String, String> parsedtags = new HashMap<String, String>();
			final String metric = Tags.parseWithMetric(parts[i], parsedtags);
			final boolean rate = "rate".equals(parts[--i]);
			if (rate)
			{
				i--;  // Move to the next part.
			}
			final Query tsdbquery = tsdb.newQuery();
			try
			{
				tsdbquery.setTimeSeries(metric, parsedtags, agg, rate);
			}
			catch (NoSuchUniqueName e)
			{
				throw new BadRequestException(e.getMessage());
			}
			// downsampling function & interval.
			if (i > 0)
			{
				final int dash = parts[1].indexOf('-', 1);  // 1st char can't be `-'.
				if (dash < 0)
				{
					throw new BadRequestException("Invalid downsampling specifier '"
							+ parts[1] + "' in m=" + m);
				}
				Aggregator downsampler;
				try
				{
					downsampler = Aggregators.get(parts[1].substring(dash + 1));
				}
				catch (NoSuchElementException e)
				{
					throw new BadRequestException("No such downsampling function: "
							+ parts[1].substring(dash + 1));
				}
				final int interval = parseDuration(parts[1].substring(0, dash));
				tsdbquery.downsample(interval, downsampler);
			}
			tsdbqueries[nqueries++] = tsdbquery;
		}
		return tsdbqueries;
	}

	/**
	 Returns the aggregator with the given name.

	 @param name Name of the aggregator to get.
	 @throws BadRequestException if there's no aggregator with this name.
	 */
	private static Aggregator getAggregator(final String name)
	{
		try
		{
			return Aggregators.get(name);
		}
		catch (NoSuchElementException e)
		{
			throw new BadRequestException("No such aggregation function: " + name);
		}
	}

	/**
	 Parses a human-readable duration (e.g, "10m", "3h", "14d") into seconds.
	 <p/>
	 Formats supported: {@code s}: seconds, {@code m}: minutes,
	 {@code h}: hours, {@code d}: days, {@code w}: weeks, {@code y}: years.

	 @param duration The human-readable duration to parse.
	 @return A strictly positive number of seconds.
	 @throws BadRequestException if the interval was malformed.
	 */
	private static int parseDuration(final String duration)
	{
		int interval;
		final int lastchar = duration.length() - 1;
		try
		{
			interval = Integer.parseInt(duration.substring(0, lastchar));
		}
		catch (NumberFormatException e)
		{
			throw new BadRequestException("Invalid duration (number): " + duration);
		}
		if (interval <= 0)
		{
			throw new BadRequestException("Zero or negative duration: " + duration);
		}
		switch (duration.charAt(lastchar))
		{
			case 's':
				return interval;                    // seconds
			case 'm':
				return interval * 60;               // minutes
			case 'h':
				return interval * 3600;             // hours
			case 'd':
				return interval * 3600 * 24;        // days
			case 'w':
				return interval * 3600 * 24 * 7;    // weeks
			case 'y':
				return interval * 3600 * 24 * 365;  // years (screw leap years)
		}
		throw new BadRequestException("Invalid duration (suffix): " + duration);
	}


	/**
	 Returns a timestamp from a date specified in a query string parameter.
	 Formats accepted are:
	 - Relative: "5m-ago", "1h-ago", etc.  See {@link #parseDuration}.
	 - Absolute human readable date: "yyyy/MM/dd-HH:mm:ss".
	 - UNIX timestamp (seconds since Epoch): "1234567890".

	 @param query     The HTTP query from which to get the query string parameter.
	 @param paramname The name of the query string parameter.
	 @return A UNIX timestamp in seconds (strictly positive 32-bit "unsigned")
	 or -1 if there was no query string parameter named {@code paramname}.
	 @throws BadRequestException if the date is invalid.
	 */
	private static long getQueryStringDate(final HttpQuery query,
			final String paramname)
	{
		final String date = query.getQueryStringParam(paramname);
		if (date == null)
		{
			return -1;
		}
		else if (date.endsWith("-ago"))
		{
			return (System.currentTimeMillis() / 1000
					- parseDuration(date.substring(0, date.length() - 4)));
		}
		long timestamp;
		if (date.length() < 5 || date.charAt(4) != '/')
		{  // Already a timestamp?
			try
			{
				timestamp = Tags.parseLong(date);              // => Looks like it.
			}
			catch (NumberFormatException e)
			{
				throw new BadRequestException("Invalid " + paramname + " time: " + date
						+ ". " + e.getMessage());
			}
		}
		else
		{  // => Nope, there is a slash, so parse a date then.
			try
			{
				final SimpleDateFormat fmt = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss");
				setTimeZone(fmt, query.getQueryStringParam("tz"));
				timestamp = fmt.parse(date).getTime() / 1000;
			}
			catch (ParseException e)
			{
				throw new BadRequestException("Invalid " + paramname + " date: " + date
						+ ". " + e.getMessage());
			}
		}
		if (timestamp < 0)
		{
			throw new BadRequestException("Bad " + paramname + " date: " + date);
		}
		return timestamp;
	}

	/**
	 Immutable cache mapping a timezone name to its object.
	 We do this because the JDK's TimeZone class was implemented by retards,
	 and it's synchronized, going through a huge pile of code, and allocating
	 new objects all the time.  And to make things even better, if you ask for
	 a TimeZone that doesn't exist, it returns GMT!  It is thus impractical to
	 tell if the timezone name was valid or not.  JDK_brain_damage++;
	 Note: caching everything wastes a few KB on RAM (34KB on my system with
	 611 timezones -- each instance is 56 bytes with the Sun JDK).
	 */
	private static final HashMap<String, TimeZone> timezones;

	static
	{
		final String[] tzs = TimeZone.getAvailableIDs();
		timezones = new HashMap<String, TimeZone>(tzs.length);
		for (final String tz : tzs)
		{
			timezones.put(tz, TimeZone.getTimeZone(tz));
		}
	}

	/**
	 Applies the given timezone to the given date format.

	 @param fmt    Date format to apply the timezone to.
	 @param tzname Name of the timezone, or {@code null} in which case this
	 function is a no-op.
	 @throws BadRequestException if tzname isn't a valid timezone name.
	 */
	private static void setTimeZone(final SimpleDateFormat fmt,
			final String tzname)
	{
		if (tzname == null)
		{
			return;  // Use the default timezone.
		}
		final TimeZone tz = timezones.get(tzname);
		if (tz != null)
		{
			fmt.setTimeZone(tz);
		}
		else
		{
			throw new BadRequestException("Invalid timezone name: " + tzname);
		}
	}


	// ---------------- //
	// Logging helpers. //
	// ---------------- //

	static void logInfo(final HttpQuery query, final String msg)
	{
		LOG.info(query.channel().toString() + ' ' + msg);
	}

	static void logWarn(final HttpQuery query, final String msg)
	{
		LOG.warn(query.channel().toString() + ' ' + msg);
	}

	static void logError(final HttpQuery query, final String msg)
	{
		LOG.error(query.channel().toString() + ' ' + msg);
	}

	static void logError(final HttpQuery query, final String msg,
			final Throwable e)
	{
		LOG.error(query.channel().toString() + ' ' + msg, e);
	}

}
