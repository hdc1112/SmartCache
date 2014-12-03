public class Commons {
	// whenever you change the log code, increment this value
	private static final int log_ver = 11;
	public static final String PROGNAME = "MR2PhaseFPGrowth";
	public static final String PREFIX = "[" + PROGNAME + "][logv " + log_ver
			+ "] ";

	// data input is by default separated by white space
	// for example, http://fimi.ua.ac.be/data/T10I4D100K.dat
	public static final String DATASEPARATOR = " ";
	// this mapreduce implementation's intermediate
	// itemset is represented by comma separated strings
	public static final String SEPARATOR = ",";
}
