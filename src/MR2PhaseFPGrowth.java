import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import ca.pfv.spmf.algorithms.frequentpatterns.fpgrowth.AlgoFPGrowth2;
import ca.pfv.spmf.patterns.itemset_array_integers_with_count.Itemset;
import ca.pfv.spmf.patterns.itemset_array_integers_with_count.Itemsets;

public class MR2PhaseFPGrowth {

	// hadoop params, for uploading params
	private static final String MINSUP_CONFIG = Commons.PROGNAME + ".minsup";
	private static final String TOTALROW_CONFIG = Commons.PROGNAME
			+ ".totalrow";
	private static final String PHASE1OUTDIR_CONFIG = Commons.PROGNAME
			+ ".phase1.outdir";
	private static long totalrows;

	private static enum RowCounter {
		TOTALROW
	}

	// solution0
	private static final double DISABLECACHE = 200;
	private static final String PHASE1MINSUP_CONFIG = Commons.PROGNAME
			+ ".phase1.minsup";
	// solution1
	private static final String SOLUTION1_CONFIG = Commons.PROGNAME
			+ ".solution1";
	private static final String SOLUTION1PARAM1_CONFIG = Commons.PROGNAME
			+ ".solution1param1";
	private static final String SOLUTION1PARAM2_CONFIG = Commons.PROGNAME
			+ ".solution1param2";
	private static final String SOLUTION1PARAM3_CONFIG = Commons.PROGNAME
			+ ".solution1param3";

	// solution0, solution1
	private static String cachefilesuffix = "-cachecount.file";

	public static void run_on_hadoop_phase1() throws IOException,
			ClassNotFoundException, InterruptedException {
		// upload the params, because this is distributed program
		Configuration conf = new Configuration();
		conf.set(MINSUP_CONFIG, Double.toString(minsup));
		conf.set(PHASE1OUTDIR_CONFIG, outputpath1stphase);
		// solution0
		conf.set(PHASE1MINSUP_CONFIG, Double.toString(phase1minsup));
		// solution1
		conf.set(SOLUTION1_CONFIG, Boolean.toString(solution1));
		conf.set(SOLUTION1PARAM1_CONFIG, Double.toString(solution1param1));
		conf.set(SOLUTION1PARAM2_CONFIG, Integer.toString(solution1param2));
		conf.set(SOLUTION1PARAM3_CONFIG, Double.toString(solution1param3));

		String jobname = Commons.PROGNAME + " Phase 1";
		Job job = new Job(conf, jobname);
		job.setJarByClass(MR2PhaseFPGrowth.class);
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(Phase1Mapper.class);
		job.setReducerClass(Phase1Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath1stphase));

		int retval = job.waitForCompletion(true) ? 0 : 1;
		if (retval != 0) {
			System.err.println(Commons.PREFIX + "Phase 1 Error, exit");
			System.exit(retval);
		}

		// phase1 mr job is piggybacked with another task:
		// count the total rows. here we get the result
		Counters counters = job.getCounters();
		Counter counter = counters.findCounter(RowCounter.TOTALROW);
		totalrows = counter.getValue();

		System.err.println(Commons.PREFIX + "Total rows: " + totalrows);
	}

	// this is a fake key, only for counting rows purpose
	private static final String SPLIT_NUM_ROWS = "split_num_rows.key";

	public static class Phase1Mapper extends
			Mapper<NullWritable, BytesWritable, Text, IntWritable> {
		@Override
		public void map(NullWritable key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			// byteswritable -> ArrayList<String>
			String realstr = new String(value.getBytes());
			String[] rows = realstr.split("\\n");
			ArrayList<String> dataset = new ArrayList<String>();
			String thisrow = null;
			for (int i = 0; i < rows.length; i++) {
				thisrow = rows[i].trim();
				if (thisrow.length() != 0) {
					dataset.add(thisrow);
				}
			}
			int numrows = dataset.size();

			// for local alg time
			long t2, t1;

			if (!solution1Enabled(solution1)) {
				// this is a big "if"
				// when solution1 is not enabled, we use solution0

				// start of solution0
				// use fp-growth alg
				AlgoFPGrowth2 localalgm = new AlgoFPGrowth2();
				// the vanilla version
				// Itemsets itemsets = localalgm.runAlgorithm2(dataset, null,
				// minsup / 100.0);

				// use cache version algorithm
				// this includes the vanilla version as a special case

				t1 = System.currentTimeMillis();
				localalgm.runAlgorithm_solution0(dataset, minsup / 100.0,
						phase1minsup / 100.0);
				t2 = System.currentTimeMillis();

				// get two range itemsets
				Itemsets higher = localalgm.getHigher();
				Itemsets lower = localalgm.getLower();

				System.err.println("higher size: " + higher.getItemsetsCount());
				System.err.println("lower size: " + lower.getItemsetsCount());

				int[] items = null;
				StringBuilder sb = new StringBuilder();
				if (phase1minsup <= minsup) {
					// minsup --- phase1minsup
					// output intermediate data to reduce phase
					// higher includes all global candidates
					for (List<Itemset> itemsetlist : higher.getLevels()) {
						for (Itemset itemset : itemsetlist) {
							items = itemset.getItems();
							sb.setLength(0);
							for (int i = 0; i < items.length; i++) {
								if (i == 0) {
									sb.append(items[i]);
								} else {
									sb.append(Commons.SEPARATOR);
									sb.append(items[i]);
								}
							}
							context.write(
									new Text(sb.toString()),
									new IntWritable(itemset
											.getAbsoluteSupport()));
						}
					}

					if (solution0Enabled(phase1minsup)) {
						// output cache to hdfs
						// higher + lower is the cache
						for (List<Itemset> itemsetlist : higher.getLevels()) {
							for (Itemset itemset : itemsetlist) {
								items = itemset.getItems();
								sb.setLength(0);
								for (int i = 0; i < items.length; i++) {
									if (i == 0) {
										sb.append(items[i]);
									} else {
										sb.append(Commons.SEPARATOR);
										sb.append(items[i]);
									}
								}
								bw.write(sb.toString() + " "
										+ itemset.getAbsoluteSupport() + "\n");
							}
						}
						for (List<Itemset> itemsetlist : lower.getLevels()) {
							for (Itemset itemset : itemsetlist) {
								items = itemset.getItems();
								sb.setLength(0);
								for (int i = 0; i < items.length; i++) {
									if (i == 0) {
										sb.append(items[i]);
									} else {
										sb.append(Commons.SEPARATOR);
										sb.append(items[i]);
									}
								}
								bw.write(sb.toString() + " "
										+ itemset.getAbsoluteSupport() + "\n");
							}
						} // end of for
					} // end of if

				} else {
					// phase1minsup --- minsup (including cachedisabled)
					// output intermediate data to reduce phase
					// higher + lower is the global candidate
					for (List<Itemset> itemsetlist : higher.getLevels()) {
						for (Itemset itemset : itemsetlist) {
							items = itemset.getItems();
							sb.setLength(0);
							for (int i = 0; i < items.length; i++) {
								if (i == 0) {
									sb.append(items[i]);
								} else {
									sb.append(Commons.SEPARATOR);
									sb.append(items[i]);
								}
							}
							context.write(
									new Text(sb.toString()),
									new IntWritable(itemset
											.getAbsoluteSupport()));
						} // end of inner for
					} // end of outer for
					for (List<Itemset> itemsetlist : lower.getLevels()) {
						for (Itemset itemset : itemsetlist) {
							items = itemset.getItems();
							sb.setLength(0);
							for (int i = 0; i < items.length; i++) {
								if (i == 0) {
									sb.append(items[i]);
								} else {
									sb.append(Commons.SEPARATOR);
									sb.append(items[i]);
								}
							}
							context.write(
									new Text(sb.toString()),
									new IntWritable(itemset
											.getAbsoluteSupport()));
						} // end of inner for
					} // end of outer for

					if (solution0Enabled(phase1minsup)) {
						// output cache to hdfs
						// higher is the cache
						for (List<Itemset> itemsetlist : higher.getLevels()) {
							for (Itemset itemset : itemsetlist) {
								items = itemset.getItems();
								sb.setLength(0);
								for (int i = 0; i < items.length; i++) {
									if (i == 0) {
										sb.append(items[i]);
									} else {
										sb.append(Commons.SEPARATOR);
										sb.append(items[i]);
									}
								}
								bw.write(sb.toString() + " "
										+ itemset.getAbsoluteSupport() + "\n");
							}
						} // end of for
					} // end of if
				} // end of else

				// the end for solution0
			} else {
				// start of solution1

				AlgoFPGrowth2 localalg = new AlgoFPGrowth2();

				t1 = System.currentTimeMillis();
				localalg.runAlgorithm_solution1(dataset, minsup / 100.0,
						solution1, solution1param1, solution1param2,
						solution1param3);
				t2 = System.currentTimeMillis();

				int[] items = null;
				StringBuilder sb = new StringBuilder();

				// write things into global candidate
				Itemsets retItemsets = localalg.getRetItemsets();
				for (List<Itemset> itemsetlist : retItemsets.getLevels()) {
					for (Itemset itemset : itemsetlist) {
						items = itemset.getItems();
						sb.setLength(0);
						for (int i = 0; i < items.length; i++) {
							if (i == 0) {
								sb.append(items[i]);
							} else {
								sb.append(Commons.SEPARATOR);
								sb.append(items[i]);
							}
						}
						context.write(new Text(sb.toString()), new IntWritable(
								itemset.getAbsoluteSupport()));
					}
				}

				// write things into cache file
				ArrayList<Itemsets> cacheItemsets = localalg.getCacheItemsets();
				for (Itemsets itemsets : cacheItemsets) {
					for (List<Itemset> itemsetlist : itemsets.getLevels()) {
						for (Itemset itemset : itemsetlist) {
							items = itemset.getItems();
							sb.setLength(0);
							for (int i = 0; i < items.length; i++) {
								if (i == 0) {
									sb.append(items[i]);
								} else {
									sb.append(Commons.SEPARATOR);
									sb.append(items[i]);
								}
							}
							bw.write(sb.toString() + " "
									+ itemset.getAbsoluteSupport() + "\n");
						}
					}
				}

				System.err.println(Commons.PREFIX + "(1/2) " + taskid
						+ " Map Task solution1 phase1minsup: "
						+ localalg.getSolution1phase1minsup());

				// end of solution1
			}

			System.err.println(Commons.PREFIX + "(1/2) " + taskid
					+ " Local algorithm run time: " + (t2 - t1));

			// for any solution, we have to calculate the #rows
			context.write(new Text(SPLIT_NUM_ROWS), new IntWritable(numrows));
		} // end of map function

		// download param
		private double minsup;
		private String output1stphasedir;
		// solution0
		private double phase1minsup;
		// solution1
		private boolean solution1;
		private double solution1param1;
		private int solution1param2;
		private double solution1param3;

		// bookkeeping
		private long starttime, endtime;
		private int taskid;

		// cache
		private BufferedWriter bw;

		@Override
		public void setup(Context context) throws IOException {
			// my id
			taskid = context.getTaskAttemptID().getTaskID().getId();

			// download
			minsup = context.getConfiguration().getDouble(MINSUP_CONFIG, 100);
			output1stphasedir = context.getConfiguration().get(
					PHASE1OUTDIR_CONFIG);
			// solution0
			phase1minsup = context.getConfiguration().getDouble(
					PHASE1MINSUP_CONFIG, 100);
			// solution1
			solution1 = context.getConfiguration().getBoolean(SOLUTION1_CONFIG,
					false);
			solution1param1 = context.getConfiguration().getDouble(
					SOLUTION1PARAM1_CONFIG, solution1param1default);
			solution1param2 = context.getConfiguration().getInt(
					SOLUTION1PARAM2_CONFIG, solution1param2default);
			solution1param3 = context.getConfiguration().getDouble(
					SOLUTION1PARAM3_CONFIG, solution1param3default);

			// show params
			System.err.println(Commons.PREFIX + "minsup: " + minsup);
			// solution0
			System.err
					.println(Commons.PREFIX + "phase1minsup: " + phase1minsup);
			System.err.println(Commons.PREFIX + "solution0Enabled: "
					+ solution0Enabled(phase1minsup));
			// solution1
			System.err.println(Commons.PREFIX + "solution1: " + solution1);
			System.err.println(Commons.PREFIX + "solution1param1: "
					+ solution1param1);
			System.err.println(Commons.PREFIX + "solution1param2: "
					+ solution1param2);
			System.err.println(Commons.PREFIX + "solution1param3: "
					+ solution1param3);
			System.err.println(Commons.PREFIX + "solution1Enabled: "
					+ solution1Enabled(solution1));

			// bookkeeping
			starttime = System.currentTimeMillis();
			System.err.println(Commons.PREFIX + "(1/2) " + taskid
					+ " Map Task start time: " + starttime);

			// open cache file for write
			if (solution0Enabled(phase1minsup) || solution1Enabled(solution1)) {
				FileSystem fs = FileSystem.get(context.getConfiguration());
				String splitname = ((FileSplit) context.getInputSplit())
						.getPath().getName();
				FSDataOutputStream out = fs.create(new Path(output1stphasedir
						+ "/_" + splitname + cachefilesuffix));
				bw = new BufferedWriter(new OutputStreamWriter(out));
			}
		}

		@Override
		public void cleanup(Context context) throws IOException {
			// bookkeeping
			endtime = System.currentTimeMillis();
			System.err.println(Commons.PREFIX + "(1/2) " + taskid
					+ " Map Task end time: " + endtime);
			System.err.println(Commons.PREFIX + "(1/2) " + taskid
					+ " Map Task execution time: " + (endtime - starttime));

			// close cache stream
			if (solution0Enabled(phase1minsup) || solution1Enabled(solution1)) {
				if (bw != null) {
					bw.close();
				}
			}
		}
	}

	public static class Phase1Reducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			if (key.toString().equals(SPLIT_NUM_ROWS)) {
				// compute input total rows
				int totalrows = 0;
				for (IntWritable val : values) {
					totalrows += val.get();
				}
				context.getCounter(RowCounter.TOTALROW).increment(totalrows);
			} else {
				context.write(key, one);
			}
		}

		// bookkeeping
		private long reducestart, reduceend;
		private int taskid;

		// save time
		private IntWritable one = new IntWritable(1);

		@Override
		public void setup(Context context) {
			// my id
			taskid = context.getTaskAttemptID().getTaskID().getId();

			// bookkeeping
			reducestart = System.currentTimeMillis();
			System.err.println(Commons.PREFIX + "(1/2) " + taskid
					+ " Reduce Task start time: " + reducestart);
		}

		@Override
		public void cleanup(Context context) {
			// bookkeeping
			reduceend = System.currentTimeMillis();
			System.err.println(Commons.PREFIX + "(1/2) " + taskid
					+ " Reduce Task end time: " + reduceend);
			System.err.println(Commons.PREFIX + "(1/2) " + taskid
					+ " Reduce Task execution time: "
					+ (reduceend - reducestart));
		}
	}

	public static void run_on_hadoop_phase2() throws IOException,
			URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set(MINSUP_CONFIG, Double.toString(minsup));
		conf.set(TOTALROW_CONFIG, Long.toString(totalrows));
		conf.set(PHASE1OUTDIR_CONFIG, outputpath1stphase);
		// solution0
		conf.set(PHASE1MINSUP_CONFIG, Double.toString(phase1minsup));
		// solution1
		conf.set(SOLUTION1_CONFIG, Boolean.toString(solution1));
		conf.set(SOLUTION1PARAM1_CONFIG, Double.toString(solution1param1));
		// since phase2 only needs to know whether solution1 is enabled or not
		// I don't need to upload solution1param2, solution1param3 for now

		String jobname = Commons.PROGNAME + " Phase 2";
		Job job = new Job(conf, jobname);
		job.setJarByClass(MR2PhaseFPGrowth.class);
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(Phase2Mapper.class);
		job.setReducerClass(Phase2Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));

		// add the global candidates into cache
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fss = fs.listStatus(new Path(outputpath1stphase));
		int filenum = 0;
		for (FileStatus status : fss) {
			Path path = status.getPath();
			if (path.getName().startsWith("_")) {
				continue;
			}
			filenum++;
			job.addCacheFile(new URI(path.toString()));
		}

		System.err.println(Commons.PREFIX + "Just added " + filenum
				+ " files into distributed cache.");
		int retval = job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Phase2Mapper extends
			Mapper<NullWritable, BytesWritable, Text, IntWritable> {
		@Override
		public void map(NullWritable key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			// bytesWritable -> ArrayList<String>
			String realstr = new String(value.getBytes());
			String[] rows = realstr.split("\\n");
			ArrayList<String> dataset = new ArrayList<String>();
			String thisrow = null;
			for (int i = 0; i < rows.length; i++) {
				thisrow = rows[i].trim();
				if (thisrow.length() != 0) {
					dataset.add(thisrow);
				}
			}

			// parse this once
			for (String line : dataset) {
				HashSet<String> set = new HashSet<String>();
				StringTokenizer st = new StringTokenizer(line,
						Commons.DATASEPARATOR);
				while (st.hasMoreTokens()) {
					set.add(st.nextToken());
				}
				split.add(set);
			}

			// for each global candidate file, count each line
			if (localFiles != null) {
				for (int i = 0; i < localFiles.length; i++) {
					localCount(localFiles[i], dataset, context);
				}
			}
		}

		private void localCount(File file, ArrayList<String> dataset,
				Context context) throws IOException, InterruptedException {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line;
			HashSet<String> myset = new HashSet<String>();
			int count = 0;
			while ((line = br.readLine()) != null) {
				// \t comes from hadoop framework
				String candidate = line.split("\\t")[0];
				total++;
				if (cache.containsKey(candidate)) {
					hit++;
					context.write(
							new Text(candidate),
							new IntWritable(Integer.parseInt(cache
									.get(candidate))));
					continue;
				}
				StringTokenizer st = new StringTokenizer(candidate,
						Commons.SEPARATOR);
				myset.clear();
				count = 0;
				while (st.hasMoreTokens()) {
					myset.add(st.nextToken());
				}
				// for each candidate, go over the dataset
				for (HashSet<String> set : split) {
					if (set.containsAll(myset)) {
						count++;
					}
				}
				context.write(new Text(candidate), new IntWritable(count));
			}
			br.close();
		}

		// download params
		private String output1stphasedir;
		// solution0
		private double phase1minsup;
		// solution1
		private boolean solution1;
		private double solution1param1;

		// for distributed cache, global candidate
		private File[] localFiles;

		// for cache, only parse value when needed
		private long total, hit;
		private HashMap<String, String> cache = new HashMap<String, String>();

		// for parse once
		private ArrayList<HashSet<String>> split = new ArrayList<HashSet<String>>();

		// bookkeeping
		private long starttime, endtime;
		private int taskid;

		@Override
		public void setup(Context context) throws IOException {
			// download params
			output1stphasedir = context.getConfiguration().get(
					PHASE1OUTDIR_CONFIG);
			// solution0
			phase1minsup = context.getConfiguration().getDouble(
					PHASE1MINSUP_CONFIG, DISABLECACHE);
			// solution1
			solution1 = context.getConfiguration().getBoolean(SOLUTION1_CONFIG,
					false);
			solution1param1 = context.getConfiguration().getDouble(
					SOLUTION1PARAM1_CONFIG, solution1param1default);

			// show params
			System.err.println(Commons.PREFIX + "output1stphasedir: "
					+ output1stphasedir);
			// solution0
			System.err
					.println(Commons.PREFIX + "phase1minsup: " + phase1minsup);
			System.err.println(Commons.PREFIX + "solution0Enabled: "
					+ solution0Enabled(phase1minsup));
			// solution1
			System.err.println(Commons.PREFIX + "solution1: " + solution1);
			System.err.println(Commons.PREFIX + "solution1param1: "
					+ solution1param1);
			System.err.println(Commons.PREFIX + "solution1Enabled: "
					+ solution1Enabled(solution1));

			// my id
			taskid = context.getTaskAttemptID().getTaskID().getId();

			// bookkeeping
			starttime = System.currentTimeMillis();
			System.err.println(Commons.PREFIX + "(2/2) " + taskid
					+ " Map Task start time: " + starttime);

			// for global candidate file
			Path[] cacheFiles = context.getLocalCacheFiles();
			String[] localFileNames;
			if (cacheFiles != null) {
				localFileNames = new String[cacheFiles.length];
				localFiles = new File[cacheFiles.length];
				for (int i = 0; i < cacheFiles.length; i++) {
					localFileNames[i] = cacheFiles[i].toString();
					localFiles[i] = new File(localFileNames[i]);
				}
			}

			// for cache file
			if (solution0Enabled(phase1minsup) || solution1Enabled(solution1)) {
				FileSystem fs = FileSystem.get(context.getConfiguration());
				String splitname = ((FileSplit) context.getInputSplit())
						.getPath().getName();
				FSDataInputStream in = fs.open(new Path(output1stphasedir
						+ "/_" + splitname + cachefilesuffix));
				BufferedReader br = new BufferedReader(
						new InputStreamReader(in));
				String line = null;
				while ((line = br.readLine()) != null) {
					String[] two = line.split(" ");
					cache.put(two[0], two[1]);
				}
				br.close();
			}
		}

		@Override
		public void cleanup(Context context) {
			// bookkeeping
			endtime = System.currentTimeMillis();
			System.err.println(Commons.PREFIX + "(2/2) " + taskid
					+ " Map Task end time: " + endtime);
			System.err.println(Commons.PREFIX + "(2/2) " + taskid
					+ " Map Task execution time: " + (endtime - starttime));
			System.err.println(Commons.PREFIX + "(2/2) " + taskid
					+ " Cache hit: " + hit + " / " + total + " = "
					+ (total == 0 ? "N/A" : (hit / (double) total)));
		}
	}

	public static class Phase2Reducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			if ((sum * 100) >= (minsup * totalrows)) {
				context.write(key, new IntWritable(sum));
			}
		}

		// download params
		private double minsup;
		private long totalrows;

		// bookkeeping
		private long reducestart, reduceend;
		private int taskid;

		@Override
		public void setup(Context context) {
			// download params
			minsup = context.getConfiguration().getDouble(MINSUP_CONFIG, 100);
			totalrows = context.getConfiguration().getLong(TOTALROW_CONFIG, 1);

			// taskid
			taskid = context.getTaskAttemptID().getTaskID().getId();

			// show params
			System.err.println(Commons.PREFIX + "minsup: " + minsup);
			System.err.println(Commons.PREFIX + "totalrows: " + totalrows);

			// bookkeeping
			reducestart = System.currentTimeMillis();
			System.err.println(Commons.PREFIX + "(2/2) " + taskid
					+ " Reduce Task start time: " + reducestart);
		}

		@Override
		public void cleanup(Context context) {
			// bookkeeping
			reduceend = System.currentTimeMillis();
			System.err.println(Commons.PREFIX + "(2/2) " + taskid
					+ " Reduce Task end time: " + reduceend);
			System.err.println(Commons.PREFIX + "(2/2) " + taskid
					+ " Reduce Task execution time: "
					+ (reduceend - reducestart));
		}
	}

	// this function is called in map/reduce task
	// this function has a shadow in AlgoFPGrowth2.java
	public static boolean solution0Enabled(double phase1minsup) {
		return phase1minsup <= 100;
	}

	// this function is called in map/reduce task
	// this function has a shadow in AlgoFPGrowth2.java
	public static boolean solution1Enabled(boolean solution1) {
		return solution1;
	}

	// cmd line params
	private static String inputpath;
	private static String outputpath;
	private static double minsup;
	// param induced by cmd line params
	private static String outputpath1stphase;
	// solution0
	private static double phase1minsup; // for example, 63
	// solution1
	private static boolean solution1;
	private static double solution1param1;
	private static final double solution1param1default = 0.5;
	private static int solution1param2;
	private static final int solution1param2default = 21;
	private static double solution1param3;
	private static final double solution1param3default = 0.8;

	public static void main(String[] args) throws IOException, ParseException,
			ClassNotFoundException, InterruptedException, URISyntaxException {
		// hadoop cmd parse
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		// my cmd parse
		// definition stage
		Options options = buildOptions();

		// parsing stage
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse(options, otherArgs);

		// interrogation stage
		// these params must exist
		inputpath = cmd.getOptionValue("inpath");
		outputpath = cmd.getOptionValue("outpath");
		minsup = Double.parseDouble(cmd.getOptionValue("minsupport"));
		// solution0
		// manually set the phase1minsup
		double beta = 1;
		if (cmd.hasOption("phase1minsup") && cmd.hasOption("phase1minsupbeta")) {
			// skip phase1minsup
			System.err.println(Commons.PREFIX
					+ "phase1minsup cmd param is skipped");
			beta = Double.parseDouble(cmd.getOptionValue("phase1minsupbeta"));
			phase1minsup = beta * minsup;
		} else if (cmd.hasOption("phase1minsup")
				&& !cmd.hasOption("phase1minsupbeta")) {
			phase1minsup = Double.parseDouble(cmd
					.getOptionValue("phase1minsup"));
		} else if (!cmd.hasOption("phase1minsup")
				&& cmd.hasOption("phase1minsupbeta")) {
			beta = Double.parseDouble(cmd.getOptionValue("phase1minsupbeta"));
			phase1minsup = beta * minsup;
		} else {
			phase1minsup = DISABLECACHE;
		}
		// solution1
		// automatically find the threshold for cache
		solution1 = false;
		if (cmd.hasOption("solution1")) {
			solution1 = true;
			solution1param1 = solution1param1default;
			solution1param2 = solution1param2default;
			solution1param3 = solution1param3default;
		}
		if (cmd.hasOption("solution1param1")) {
			solution1param1 = Double.parseDouble(cmd
					.getOptionValue("solution1param1"));
		}
		if (cmd.hasOption("solution1param2")) {
			solution1param2 = Integer.parseInt(cmd
					.getOptionValue("solution1param2"));
		}
		if (cmd.hasOption("solution1param3")) {
			solution1param3 = Double.parseDouble(cmd
					.getOptionValue("solution1param3"));
		}

		// verify stage, semantic check
		// skip basic param's verification
		// solution0
		if (phase1minsup > 100) {
			phase1minsup = DISABLECACHE;
			System.err.println(Commons.PREFIX + "solution0 is disabled");
		} else if (phase1minsup < 0) {
			System.err.println(Commons.PREFIX + "phase1minsup is less than 0");
			System.err.println(Commons.PREFIX + "phase1minsup set to 0");
			phase1minsup = 0;
		}
		if (phase1minsup <= 100 && phase1minsup >= 0) {
			System.err.println(Commons.PREFIX + "solution0 is enabled");
		}
		// solution1
		if (solution1) {
			// solution1 will only starts from less than minsup
			// solution1 assumes the optimum is always in the right side
			if (solution1param1 < 0 || solution1param1 > 1) {
				System.err.println(Commons.PREFIX
						+ "solution1param1 is < 0 or > 1");
				System.err.println(Commons.PREFIX
						+ "solution1param1 set to default");
				solution1param1 = solution1param1default;
			}
			if (solution1param2 < 2) {
				System.err.println(Commons.PREFIX + "solution1param2 is < 2");
				System.err.println(Commons.PREFIX
						+ "solution1param2 set to default");
				solution1param2 = solution1param2default;
			}
			if (solution1param3 < 0 || solution1param3 > 1) {
				System.err.println(Commons.PREFIX
						+ "solution1param3 is < 0 or > 1");
				System.err.println(Commons.PREFIX
						+ "solution1param3 set to default");
				solution1param3 = solution1param3default;
			}

			System.err.println(Commons.PREFIX + "solution1 is enabled");
		} else {
			System.err.println(Commons.PREFIX + "solution1 is disabled");
		}

		// show stage
		System.err.println(Commons.PREFIX + "inpath: " + inputpath);
		System.err.println(Commons.PREFIX + "outpath: " + outputpath);
		System.err.println(Commons.PREFIX + "minsupport: " + minsup);
		// solution0
		System.err.println(Commons.PREFIX + "phase1minsup: " + phase1minsup);
		// solution1
		System.err.println(Commons.PREFIX + "solution1: " + solution1);
		System.err.println(Commons.PREFIX + "solution1param1: "
				+ solution1param1);
		System.err.println(Commons.PREFIX + "solution1param2: "
				+ solution1param2);
		System.err.println(Commons.PREFIX + "solution1param3: "
				+ solution1param3);

		// main logic
		outputpath1stphase = outputpath + "-1stphase";

		long starttime = System.currentTimeMillis();

		long phase1starttime = System.currentTimeMillis();

		run_on_hadoop_phase1();

		long phase1endtime = System.currentTimeMillis();

		long phase2starttime = System.currentTimeMillis();

		run_on_hadoop_phase2();

		long phase2endtime = System.currentTimeMillis();

		long endtime = System.currentTimeMillis();

		System.err.println(Commons.PREFIX + "Total execution time: "
				+ (endtime - starttime));
		System.err.println(Commons.PREFIX + "Phase 1 execution time: "
				+ (phase1endtime - phase1starttime));
		System.err.println(Commons.PREFIX + "Phase 2 execution time: "
				+ (phase2endtime - phase2starttime));

	}

	@SuppressWarnings("static-access")
	private static Options buildOptions() {
		Options options = new Options();

		// this param must exist
		options.addOption(OptionBuilder.withArgName("inpath").hasArg()
				.isRequired().withDescription("hdfs input folder")
				.create("inpath"));

		// this param must exist
		options.addOption(OptionBuilder.withArgName("outpath").hasArg()
				.isRequired().withDescription("hdfs ouput folder")
				.create("outpath"));

		// this param must exist
		options.addOption(OptionBuilder.withArgName("minsupport").hasArg()
				.isRequired().withDescription("minimum support in percentage")
				.create("minsupport"));

		// the following things are different solutions
		// solution that comes afterwards has higher visibility
		// main logic will use if (!solution2) { if (!solution1) { solution0; }
		// else { solution1; }} else { solution2; }

		// this param is optional (solution0)
		// manually set the cache threshold by absolute value
		// phase1minsup is only useful in manual cache mode
		// let's call this as "solution0"
		options.addOption(OptionBuilder.withArgName("phase1minsup").hasArg()
				.withDescription("phase 1 minsupport in percentage")
				.create("phase1minsup"));

		// this param is optional (solution0)
		// manually set the cache threshold by relative ratio
		// override "phase1minsup"
		// phase1minsupbeta is only useful in manual cache mode
		options.addOption(OptionBuilder.withArgName("phase1minsupbeta")
				.hasArg().withDescription("phase 1 minsup ratio")
				.create("phase1minsupbeta"));

		// this param is optional (solution1)
		// automatically find a good cache threshold
		// the switch to open solution1
		options.addOption(OptionBuilder.withArgName("solution1").hasArg(false)
				.withDescription("solution1").create("solution1"));

		// this param is optional (solution1)
		// the lowest limit to consider
		// if set, solution1 will use it as the low limit
		// solution1param1 is only useful in solution1 mode
		options.addOption(OptionBuilder.withArgName("solution1param1").hasArg()
				.withDescription("solution1param1").create("solution1param1"));

		// this param is optional (solution1)
		// number of buckets (remember the 0th bucket is for global candidate)
		// solution1param2
		options.addOption(OptionBuilder.withArgName("solution1param2").hasArg()
				.withDescription("solution1param2").create("solution1param2"));

		// this param is optional (solution1)
		// r-square threshold
		// solution1param3
		options.addOption(OptionBuilder.withArgName("solution1param3").hasArg()
				.withDescription("solution1param3").create("solution1param3"));

		return options;
	}
}
