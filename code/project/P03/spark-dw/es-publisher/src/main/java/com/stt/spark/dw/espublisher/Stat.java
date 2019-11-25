package com.stt.spark.dw.espublisher;

import java.util.List;

public class Stat {

    String title;

    List<Option> options;

    public Stat(String title,List<Option> options){
        this.title=title;
        this.options=options;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<Option> getOptions() {
        return options;
    }

    public void setOptions(List<Option> options) {
        this.options = options;
    }


    public static class Option {
        String name;

        Double value;

        public Option(String name,Double value){
            this.name=name;
            this.value=value;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Double getValue() {
            return value;
        }

        public void setValue(Double value) {
            this.value = value;
        }

    }

}
 
 
