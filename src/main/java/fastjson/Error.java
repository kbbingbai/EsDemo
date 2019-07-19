package fastjson;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * date  2019/7/18-14:45
 * Description:
 * 运行的结果：
 */
public class Error implements Serializable {

    private static final long serialVersionUID = -432908543160176349L;

    private String code;
    private String message;
    private String success;
    private List<Data> data = new ArrayList<>();

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getSuccess() {
        return success;
    }

    public void setSuccess(String success) {
        this.success = success;
    }

    public List<Data> getData() {
        return data;
    }

    public void setData(List<Data> data) {
        this.data = data;
    }
}
