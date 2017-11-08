package com.huawei.zjh.restclient;

/**
 * Created by zhujinhua on 2017/11/8.
 */


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
//import org.json.JSONObject;
import org.apache.wink.client.ClientResponse;
import org.apache.wink.client.Resource;
import org.apache.wink.client.RestClient;


import javax.xml.ws.Response;
import java.util.HashMap;
import java.util.Map;

/**
 * This example demonstrates an alternative way to call a URL
 * using the Apache HttpClient HttpGet and HttpResponse
 * classes.
 *
 * I copied the guts of this example from the Apache HttpClient
 * ClientConnectionRelease class, and decided to leave all the
 * try/catch/finally handling in the class. You don't have to catch
 * all the exceptions individually like this, I just left the code
 * as-is to demonstrate all the possible exceptions.
 *
 * Apache HttpClient: http://hc.apache.org/httpclient-3.x/
 *
 */
public class WinkRestClient {

    RestClient rc = new RestClient();
    ClientResponse clientresponse = null;
    public final static void main(String[] args) {

        String url = "http://10.0.0.121:8081/jobs/f43c29b3da86b53341c50b9a4bb631de";

        WinkRestClient winkCient = new WinkRestClient();

        String ret = winkCient.doget(url);
        System.out.println(ret);

        JSONObject jobj = JSONObject.parseObject(ret);

        System.out.println(jobj.getString("name")+jobj.getString("vertices"));

        winkCient.dopost(url, "Hello, world");
    }


    public void dopost(String url, String data)
    {
        Resource resource = rc
                .resource(url);
        String response = resource.contentType("application/json")
                .accept("application/json").post(String.class, data);
        System.out.println("-----------"+response);
    }
    public String doget(String url)
    {
        Resource resource = rc.resource(url);
        String response = resource.contentType("application/json")
                .accept("application/json").get(String.class);
        System.out.println("-----------"+response);

        return response;
       // clientresponse = resource.get();
        //JSONObject jsonObject = resource.contentType("application/json")
        //       .accept("application/json").get(JSONObject.class);


        //return jsonObject.toString();
    }

}