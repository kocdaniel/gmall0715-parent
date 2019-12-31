package com.atguigu.gmall0715.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0715.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;


    @GetMapping("realtime-total")
    public String  getRealtimeTotal( @RequestParam("date") String date){
        Long dauCount = publisherService.getDauCount(date);

        List<Map> totalList=new ArrayList<>();

        Map dauMap=new HashMap();

        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauCount);
        totalList.add(dauMap);

        Map newMidMap=new HashMap();

        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",233);
        totalList.add(newMidMap);

        Map orderMap=new HashMap();
        Double orderAmount = publisherService.getOrderAmount(date);
        orderMap.put("id","order_amount");
        orderMap.put("name","新增交易额");
        orderMap.put("value",orderAmount);
        totalList.add(orderMap);

        return JSON.toJSONString(totalList);
    }

    @GetMapping("realtime-hour")
    public String getRealtimeTotalHour(@RequestParam("id") String id, @RequestParam("date") String date){
        if ("dau".equals(id)){
            Map dauCountHourToday = publisherService.getDauCountHour(date);
            String yesterday = getYesterday(date);
            Map dauCountHourYesterday = publisherService.getDauCountHour(date);

            Map<String, Map> resultMap = new HashMap<>();

            resultMap.put("yesterday", dauCountHourYesterday);
            resultMap.put("today", dauCountHourToday);
            return JSON.toJSONString(resultMap);
        } else if("order_amount".equals(id)){
            Map orderAmountHourMapTD = publisherService.getOrderAmountHour(date);
            String yd = getYesterday(date);
            Map orderAmountHourMapYD = publisherService.getOrderAmountHour(yd);

            Map<String,Map> resultMap=new HashMap<>();
            resultMap.put("yesterday",orderAmountHourMapYD);
            resultMap.put("today",orderAmountHourMapTD);
            return   JSON.toJSONString(resultMap) ;

        } else {
            return null;
        }
    }

    private String getYesterday(String today){

        String yesterday=null;
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

        try {
            Date todayDate = formatter.parse(today);
            Date yesterdayDate = DateUtils.addDays(todayDate, -1);

            yesterday = formatter.format(yesterdayDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return yesterday;

    }
}
