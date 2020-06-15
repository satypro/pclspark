package com.pclspark;

import com.pclspark.worker.SpacePartitionFeatureWorker;
import com.pclspark.worker.SpacePartitionWorker;

public class Main
{
    public static  void main(String args[])
    {
        //SparkCassandraObsolete cassandraSpark = new SparkCassandraObsolete();
        //cassandraSpark.BuildDataOutputSpaceSplitNonMortonCassandra();
        //cassandraSpark.BuildDataOutputSpaceSplitCassandra();

        SpacePartitionWorker.BuildDataOutputSpaceSplitCassandra();
        //SpacePartitionFeatureWorker.Process();
    }
}