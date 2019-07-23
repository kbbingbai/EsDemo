package es.demo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * 查询数据
 *
 * @Description:
 * @date 2019年3月22日
 */
public class EsDemoTwo3 {
    private TransportClient client = null;

    @Before
    public void init() throws UnknownHostException {
        String esServerIps = "192.168.44.141,192.168.44.142,192.168.44.143";
        try {
            Settings settings = Settings.builder().put("cluster.name", "escluster").build();
            String esIps[] = esServerIps.split(",");
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.44.141"), 9300));
            System.out.println(client);
        } catch (Exception e) {
            e.printStackTrace();
            if (client != null) {
                client.close();
            }
        }
    }

    // 得到一条数据
    @Test
    public void test1() {
        // 读取
        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader br = null;

        // 写入
        FileWriter fw = null;
        BufferedWriter bw = null;

        try {
            // 读取
            fis = new FileInputStream("D:/my1.csv");
            isr = new InputStreamReader(fis, "GB2312");
            br = new BufferedReader(isr);

            fw = new FileWriter(new File("D:/my2.csv"));
            bw = new BufferedWriter(fw);

            String line = "";
            String[] arrs = null;
            while ((line = br.readLine()) != null) {
                arrs = line.split(",");
                System.out.println(arrs[0] + " : " + arrs[1] + " : " + arrs[2] + " : " + arrs[3]);

                // es操作
                Map<String, Object> map = new HashMap<String, Object>();
                map.put("title", arrs[0]);
                map.put("author", arrs[1]);
                map.put("remarks", arrs[2]);
                map.put("content", arrs[3]);
                Object json = JSONObject.toJSON(map);

                String jsonString = JSON.toJSONString(json);
                String id = client.prepareIndex("dnsanalyse", "intelligence")
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).setSource(jsonString, XContentType.JSON)
                        .get().getId();

                System.out.println(id);

                // 写入
                bw.write(line + "," + id + "\t\n");
            }
        } catch (Exception e) {
            System.out.println("///////");
        } finally {
            try {
                br.close();
                isr.close();
                fis.close();
                bw.close();
                fw.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            client.close();
        }
    }

}
