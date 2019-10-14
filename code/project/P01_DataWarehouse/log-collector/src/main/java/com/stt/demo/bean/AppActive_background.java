package com.stt.demo.bean;

import lombok.Data;

/**
 * 用户后台活跃
 */
@Data
public class AppActive_background {
    private String active_source;//1=upgrade,2=download(下载),3=plugin_upgrade

}