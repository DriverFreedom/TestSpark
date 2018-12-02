package cn.pig.cmcc.services;

import cn.pig.cmcc.beans.MapVo;
import cn.pig.cmcc.utils.Constants;
import cn.pig.cmcc.utils.Jpools;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MapIndexService implements  IMapIndexService{
    @Override
    public List<MapVo> findAllBy(String day) {
        List<MapVo> list = new ArrayList<>();

        //从redis中读取数据
        Jedis jedis = Jpools.getJedis();
        Map<String,String> all = jedis.hgetAll(Constants.MAP_PREFIX+day);
        System.out.println("*****"+all);
        for(Map.Entry<String,String> entry:all.entrySet()){
            MapVo map = new MapVo();

            map.setName(entry.getKey());
            map.setValue(Integer.parseInt(entry.getValue()));
            list.add(map);
        }

        return list;

    }
}
