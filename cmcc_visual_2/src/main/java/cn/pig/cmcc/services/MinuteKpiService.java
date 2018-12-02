package cn.pig.cmcc.services;

import cn.pig.cmcc.beans.MinutesKpiVo;
import cn.pig.cmcc.utils.Constants;
import cn.pig.cmcc.utils.Jpools;
import redis.clients.jedis.Jedis;

public class MinuteKpiService implements IMinuteService {

    /**
     * 根据日期和时间获取数据
     * @param day
     * @param hourMinutes
     * @return
     */
    @Override
    public MinutesKpiVo findBy(String day, String hourMinutes) {
        MinutesKpiVo vo = new MinutesKpiVo();
        //获取数据
        Jedis jedis = Jpools.getJedis();
        //获取最近一分钟的充值金额，获取最近一分钟的充值笔数
        String money = jedis.hget(Constants.MUNITE_PREFIX+day,Constants.MINUTES_FIELD_M_PREFIX+hourMinutes);
        //获取最近一分钟的充值笔数
        String num = jedis.hget(Constants.MUNITE_PREFIX+day,Constants.MINUTES_FIELD_NUM_PREFIX+hourMinutes);

        jedis.close();
        vo.setCounts(num);
        vo.setMoney(money);
        return vo;
    }
}
