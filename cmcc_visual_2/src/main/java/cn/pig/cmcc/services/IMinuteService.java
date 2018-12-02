package cn.pig.cmcc.services;

import cn.pig.cmcc.beans.MinutesKpiVo;

public interface IMinuteService {
    /**
     * 根据日期和时间获取数据
     * @param date
     * @param hourMinutes
     * @return
     */
    MinutesKpiVo findBy(String date,String hourMinutes);
}
