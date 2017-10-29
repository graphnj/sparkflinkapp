package com.huawei.zjh.kafkaproducertool;

import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;

/**
 * Created by zhujinhua on 2017/10/29.
 */
public class MemoryUsageExtrator {

    private static OperatingSystemMXBean mxBean =
            (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    /**
     * Get current free memory size in bytes
     * @return  free RAM size
     */
    public static long currentFreeMemorySizeInBytes() {
        return mxBean.getFreePhysicalMemorySize();
    }
}
