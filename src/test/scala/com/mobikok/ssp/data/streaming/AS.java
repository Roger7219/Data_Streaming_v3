package com.mobikok.ssp.data.streaming;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.mobikok.ssp.data.streaming.util.OM;

import java.io.Serializable;

/**
 * Created by Administrator on 2017/12/11.
 */
public class AS {
    private String s;

    public static void main(String[] args) {

        A a = new A();
        System.out.println(OM.toJOSN(a));
        System.out.println(OM.toJOSN(OM.toBean( OM.toJOSN(a), A.class)) );

    }

    public static class A{

        E e = E.E1;

        public E getE() {
            return e;
        }

        public void setE(E e) {
            this.e = e;
        }
    }
    public static enum E implements IntegerEnum{

        E1(11);

        private int code;

        private E(int code){
            this.code = code;
        }

        public int getCode() {
            return code;
        }
       // @JsonCreator
        public static E valueOf(int code){
            for(E item : values()) {
                if (item.getCode() == code) {
                    return item;
                }
            }
            return null;
        }

        @Override
        public String toString() {
            return ""+code;
        }
    }

    public interface IntegerEnum extends Serializable {
        @JsonValue
        public abstract int getCode();

    }

}
