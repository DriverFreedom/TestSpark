package cn.pig.cmcc.controller;

import cn.pig.cmcc.beans.MapVo;
import cn.pig.cmcc.services.IMapIndexService;
import cn.pig.cmcc.services.MapIndexService;
import com.alibaba.fastjson.JSONObject;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

/**
 * 充值成功业务分布Servlet访问接口
 */
@WebServlet(name = "Servlet",urlPatterns = "/mapIndex.cmcc")
public class Servlet extends HttpServlet {
    //实例化service对象
    IMapIndexService service = new MapIndexService();
    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setCharacterEncoding("utf-8");
        resp.setContentType("application/json");

        //接收前端传递的参数
        String day = req.getParameter("day");
        //调用service
        List<MapVo> voList = service.findAllBy(day);

        //将数据返回给前端
        String jsonStr = JSONObject.toJSONString(voList);
        //将jsonzfc写到前端
        resp.getWriter().write(jsonStr);
    }
}
