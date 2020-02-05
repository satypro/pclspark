import org.apache.spark.Partitioner;

class CustomPartitioner extends Partitioner
{

    private int numParts;

    public CustomPartitioner(int i) {
        numParts=i;
    }

    @Override
    public int numPartitions()
    {
        return numParts;
    }

    @Override
    public int getPartition(Object key){

        //partition based on the first character of the key...you can have your logic here !!
        return ((String)key).charAt(0)%numParts;

    }

    @Override
    public boolean equals(Object obj){
        if(obj instanceof CustomPartitioner)
        {
            CustomPartitioner partitionerObject = (CustomPartitioner)obj;
            if(partitionerObject.numParts == this.numParts)
                return true;
        }

        return false;
    }
}