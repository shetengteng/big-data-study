package com.stt.demo.bean;

import lombok.Data;

/**
 * 商品点击日志
 */
@Data
public class AppDisplay {

    //动作：曝光商品=1，点击商品=2，
    private String action;
    //商品ID（服务端下发的ID）
    private String goodsid;
    //顺序（第几条商品，第一条为0，第二条为1，如此类推）
    private String place;
    //曝光类型：1 - 首次曝光 2-重复曝光（没有使用）
    private String extend1;
    //分类ID（服务端定义的分类ID）
    private String category;
}