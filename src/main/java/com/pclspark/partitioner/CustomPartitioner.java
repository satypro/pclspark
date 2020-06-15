package com.pclspark.partitioner;

import org.apache.spark.Partitioner;

public class CustomPartitioner extends Partitioner
{
    private int numParts;

    public CustomPartitioner(int i)
    {
        numParts=i;
    }

    @Override
    public int numPartitions()
    {
        return numParts;
    }

    @Override
    public int getPartition(Object key)
    {
        return ((Long)key).intValue() % numParts;
    }

    /*
    @Override
    public boolean equals(Object obj)
    {
        if(obj instanceof CustomPartitioner)
        {
            CustomPartitioner partitionerObject = (CustomPartitioner)obj;
            if(partitionerObject.numParts == this.numParts)
                return true;
        }

        return false;
    }
     */
}