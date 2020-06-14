package com.pclspark;

import com.pclspark.worker.SpacePartitionFeatureWorker;

public class Main
{
    public static  void main(String args[])
    {
        //SparkCassandraObsolete cassandraSpark = new SparkCassandraObsolete();
        //cassandraSpark.BuildDataOutputSpaceSplitNonMortonCassandra();
        //cassandraSpark.BuildDataOutputSpaceSplitCassandra();

        SpacePartitionFeatureWorker.Process();
    }
}