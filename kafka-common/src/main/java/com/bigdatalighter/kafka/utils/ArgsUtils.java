package com.bigdatalighter.kafka.utils;

import org.kohsuke.args4j.CmdLineParser;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class ArgsUtils {

    public static <T> T parseArgs(String[] args, Class<T> tClass) throws Exception{
        T t = tClass.newInstance();
        CmdLineParser cmdLineParser = new CmdLineParser(t);
        if (args.length == 0) {
            System.out.print("Usage:");
            cmdLineParser.printSingleLineUsage(System.out);
            System.out.println("\r\nDescription:");
            cmdLineParser.printUsage(System.out);
            System.exit(0);
        }
        cmdLineParser.parseArgument(args);
        return t;
    }

}
