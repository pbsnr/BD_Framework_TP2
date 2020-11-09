package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");

            programDriver.addClass("district_containing_trees", DistrictContainingTrees.class,
                    "A map/reduce program that lists all the roundings having trees in Paris.");

            programDriver.addClass("existing_species", ExistingSpecies.class,
                    "A map/reduce program that lists all the tree species in Paris.");

            programDriver.addClass("nbr_trees_species", NbrTreesSpecies.class,
                    "A map/reduce program that lists the number of trees by species in Paris.");

            programDriver.addClass("sort_by_trees_height", SortByTreesHeight.class,
                    "A map/reduce program that sort the trees by height from smallest to highest");





            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
