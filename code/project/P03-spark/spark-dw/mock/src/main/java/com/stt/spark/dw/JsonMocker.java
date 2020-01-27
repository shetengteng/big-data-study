package com.stt.spark.dw;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stt.spark.util.RandomDate;
import com.stt.spark.util.RandomNum;
import com.stt.spark.util.RandomOptionGroup;
import com.stt.spark.util.RandomOptionGroup.RandomOpt;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class JsonMocker {

    int startupNum = 100000;
    int eventNum = 200000;
    Date startTime = null;
    Date endTime = null;
    RandomDate logDateUtil = null;
    String appId = "gmall";

    RandomOptionGroup<String> osOptionGroup =
            new RandomOptionGroup(
            new RandomOpt[]{new RandomOpt("ios", 3), new RandomOpt("andriod", 7)});

    RandomOptionGroup<String> areaOptionGroup =
            new RandomOptionGroup(new RandomOpt[]{new RandomOpt("beijing", 10),
            new RandomOpt("shanghai", 10), new RandomOpt("guangdong", 20), new RandomOpt("hebei", 5),
            new RandomOpt("heilongjiang", 5), new RandomOpt("shandong", 5), new RandomOpt("tianjin", 5),
            new RandomOpt("shan3xi", 5), new RandomOpt("shan1xi", 5), new RandomOpt("sichuan", 5)
    });

    RandomOptionGroup<String> vsOptionGroup =
            new RandomOptionGroup(new RandomOpt[]{new RandomOpt("1.2.0", 50), new RandomOpt("1.1.2", 15),
            new RandomOpt("1.1.3", 30),
            new RandomOpt("1.1.1", 5)
    });

    RandomOptionGroup<String> eventOptionGroup =
            new RandomOptionGroup(new RandomOpt[]{new RandomOpt("addFavor", 10), new RandomOpt("addComment", 30),
            new RandomOpt("addCart", 20), new RandomOpt("clickItem", 40)
    });

    RandomOptionGroup<String> channelOptionGroup =
            new RandomOptionGroup(new RandomOpt[] {new RandomOpt("xiaomi", 10), new RandomOpt("huawei", 20),
            new RandomOpt("wandoujia", 30), new RandomOpt("360", 20), new RandomOpt("tencent", 20)
            , new RandomOpt("baidu", 10), new RandomOpt("website", 10)
    });

    RandomOptionGroup<Boolean> isQuitGroup =
            new RandomOptionGroup(new RandomOpt[] {new RandomOpt(true, 20), new RandomOpt(false, 80)});

    public JsonMocker() {}

    public JsonMocker(String startTimeString, String endTimeString, int startupNum, int eventNum) {
        try {
            startTime = new SimpleDateFormat("yyyy-MM-dd").parse(startTimeString);
            endTime = new SimpleDateFormat("yyyy-MM-dd").parse(endTimeString);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        logDateUtil = new RandomDate(startTime, endTime, startupNum + eventNum);
    }

    String initEventLog(String startLogJson) {
            /*`type` string   COMMENT '日志类型',
             `mid` string COMMENT '设备唯一 表示',
            `uid` string COMMENT '用户标识',
            `os` string COMMENT '操作系统',
            `appid` string COMMENT '应用id',
            `area` string COMMENT '地区' ,
            `evid` string COMMENT '事件id',
            `pgid` string COMMENT '当前页',
            `npgid` string COMMENT '跳转页',
            `itemid` string COMMENT '商品编号',
            `ts` bigint COMMENT '时间',*/

        JSONObject startLog = JSON.parseObject(startLogJson);
        String mid = startLog.getString("mid");
        String uid = startLog.getString("uid");
        String os = startLog.getString("os");
        String appid = this.appId;
        String area = startLog.getString("area");
        String evid = eventOptionGroup.getRandomOpt().getValue();
        int pgid = new Random().nextInt(50) + 1;
        int npgid = new Random().nextInt(50) + 1;
        int itemid = new Random().nextInt(50);
        //  long ts= logDateUtil.getRandomDate().getTime();

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("type", "event");
        jsonObject.put("mid", mid);
        jsonObject.put("uid", uid);
        jsonObject.put("os", os);
        jsonObject.put("appid", appid);
        jsonObject.put("area", area);
        jsonObject.put("evid", evid);
        jsonObject.put("pgid", pgid);
        jsonObject.put("npgid", npgid);
        jsonObject.put("itemid", itemid);
        return jsonObject.toJSONString();
    }


    String initStartupLog() {
            /*`type` string   COMMENT '日志类型',
             `mid` string COMMENT '设备唯一标识',
      `uid` string COMMENT '用户标识',
      `os` string COMMENT '操作系统',
      `appId` string COMMENT '应用id', ,
     `vs` string COMMENT '版本号',
     `ts` bigint COMMENT '启动时间', ,
     `area` string COMMENT '城市' */
        String mid = "mid_" + RandomNum.getRandInt(4500, 5000);
        String uid = "" + RandomNum.getRandInt(1, 500);
        String os = osOptionGroup.getRandomOpt().getValue();
        String appid = this.appId;
        String area = areaOptionGroup.getRandomOpt().getValue();
        String vs = vsOptionGroup.getRandomOpt().getValue();
        //long ts= logDateUtil.getRandomDate().getTime();
        String ch = os.equals("ios") ? "appstore" : channelOptionGroup.getRandomOpt().getValue();


        JSONObject jsonObject = new JSONObject();
        jsonObject.put("type", "startup");
        jsonObject.put("mid", mid);
        jsonObject.put("uid", uid);
        jsonObject.put("os", os);
        jsonObject.put("appid", appid);
        jsonObject.put("area", area);
        jsonObject.put("ch", ch);
        jsonObject.put("vs", vs);
        return jsonObject.toJSONString();
    }

    public void sendLog(String log) {
        sendLogStream(log);
    }

    public static void sendLogStream(String log) {
        try {
            //不同的日志类型对应不同的URL
            URL url = new URL("http://hadoop102/log");

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            //设置请求方式为post
            conn.setRequestMethod("POST");

            //时间头用来供server进行时钟校对的
            conn.setRequestProperty("clientTime", System.currentTimeMillis() + "");
            //允许上传数据
            conn.setDoOutput(true);
            //设置请求的头信息,设置内容类型为JSON
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

            System.out.println("upload" + log);

            //输出流
            OutputStream out = conn.getOutputStream();
            out.write(("log=" + log).getBytes());
            out.flush();
            out.close();
            int code = conn.getResponseCode();
            System.out.println(code);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        JsonMocker jsonMocker = new JsonMocker();
        jsonMocker.startupNum = 1000000;
        for (int i = 0; i < jsonMocker.startupNum; i++) {
            String startupLog = jsonMocker.initStartupLog();
            jsonMocker.sendLog(startupLog);
            while (!jsonMocker.isQuitGroup.getRandomOpt().getValue()) {
                String eventLog = jsonMocker.initEventLog(startupLog);
                jsonMocker.sendLog(eventLog);
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}