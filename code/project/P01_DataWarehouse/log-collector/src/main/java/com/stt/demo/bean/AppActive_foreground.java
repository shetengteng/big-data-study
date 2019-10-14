package com.stt.demo.bean;

import lombok.Data;

/**
 * 用户前台活跃
 */
@Data
public class AppActive_foreground {
    private String push_id;//推送的消息的id，如果不是从推送消息打开，传空
    private String access;//1.push 2.icon 3.其他
}