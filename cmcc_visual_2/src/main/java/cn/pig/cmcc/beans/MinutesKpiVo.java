package cn.pig.cmcc.beans;

public class MinutesKpiVo {
    private String money;
    private String counts;

    public MinutesKpiVo(String money, String counts) {
        this.money = money;
        this.counts = counts;
    }

    public MinutesKpiVo() {
    }

    public String getMoney() {
        return money;
    }

    public void setMoney(String money) {
        this.money = money;
    }

    public String getCounts() {
        return counts;
    }

    public void setCounts(String counts) {
        this.counts = counts;
    }
}
