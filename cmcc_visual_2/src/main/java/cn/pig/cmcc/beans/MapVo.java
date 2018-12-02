package cn.pig.cmcc.beans;

/**
 * 充值量业务实体类
 */
public class MapVo {
    /**
     * 省份和数量
     */
    private String name;
    private int value;

    public MapVo(String name, int value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public MapVo() {
    }
}
