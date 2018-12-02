package cn.pig.cmcc.controller;

import cn.pig.cmcc.beans.MapVo;
import cn.pig.cmcc.beans.MinutesKpiVo;
import cn.pig.cmcc.services.IMapIndexService;
import cn.pig.cmcc.services.IMinuteService;
import cn.pig.cmcc.services.MapIndexService;
import cn.pig.cmcc.services.MinuteKpiService;
import com.alibaba.fastjson.JSONObject;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * 充值成功业务分布Servlet访问接口
 */
@WebServlet(name = "Servlet_Minute",urlPatterns = "/minutesKpi.cmcc")
public class Servlet_Minute extends HttpServlet {
    //实例化service对象
    IMinuteService service = new MinuteKpiService();
    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        resp.setContentType("application/json;charset=utf-8");
        //接收参数
        String day = req.getParameter("day");
        //获取时间
        Date date = new Date();
        SimpleDateFormat format = new SimpleDateFormat("HHmm");
        String time =  format.format(date);

        MinutesKpiVo vo = service.findBy(day,time);
        //将对象转换成json 并输出
        resp.getWriter().write(JSONObject.toJSONString(vo));

    }
}
