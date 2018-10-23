package com.mobikok.ssp.data.streaming.entity.feature;

import com.mobikok.ssp.data.streaming.util.OM;

/**
 * Created by Administrator on 2017/8/17.
 */
public abstract class JavaJSONSerializable {

    public String toString() {
        return OM.toJOSN(this);
    }
}
