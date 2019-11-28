package zjhtest.ml.kmeans;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @auth zhujinhua 0049003202
 * @date 2019/11/17 21:15
 */

public class KMeansAPI {
    //config
    static String modelOutputFile = "model/model.dat";

    static Float[][] clusterCenters= null;




    static public int[] predict(Float[] feature) throws Exception{
        if (null == clusterCenters) {
            clusterCenters = deserize(modelOutputFile);
        }
        //Be sure ok
        if (null == clusterCenters) {
            return new int[]{0,0,0};
        }

        Float closestDistsquare = Float.MAX_VALUE;
        Float[] distWithCC = new Float[clusterCenters.length];
        for (int c=0;c<clusterCenters.length;c++){
            Float distsquare=0.0f;
            Float[]x=clusterCenters[c];
            for(int i=0;i<x.length;i++){
                distsquare+=x[i]*feature[i];
            }
            distWithCC[c]=distsquare;

        }


        return new int[]{1,2,4};


    }
    static private Float[][] deserize(String path) throws Exception{

        FileInputStream bis = new FileInputStream(path);
        ObjectInputStream ois = new ObjectInputStream(bis);
        return (Float[][])ois.readObject();
    }
    public static void main(String[] para){

        try {
            Float[][] x= deserize(modelOutputFile);
            for(int i=0;i<100;i++)
                System.out.println(x[0][i]);
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
}
