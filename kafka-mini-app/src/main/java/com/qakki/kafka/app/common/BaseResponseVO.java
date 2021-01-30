package com.qakki.kafka.app.common;

import lombok.Data;

import java.util.UUID;

@Data
public class BaseResponseVO<M> {

  private String requestId;
  private M result;
  private Integer code;
  private String msg;

  public static <M> BaseResponseVO<M> success() {
    BaseResponseVO<M> baseResponseVO = new BaseResponseVO<>();
    baseResponseVO.setRequestId(genRequestId());
    baseResponseVO.setCode(ResultTypeEnum.SUCCESS.code);
    baseResponseVO.setMsg(ResultTypeEnum.SUCCESS.msg);
    return baseResponseVO;
  }

  public static <M> BaseResponseVO<M> success(M result) {
    BaseResponseVO<M> baseResponseVO = new BaseResponseVO<>();
    baseResponseVO.setRequestId(genRequestId());
    baseResponseVO.setResult(result);
    baseResponseVO.setCode(ResultTypeEnum.SUCCESS.code);
    baseResponseVO.setMsg(ResultTypeEnum.SUCCESS.msg);
    return baseResponseVO;
  }

  private static String genRequestId() {
    return UUID.randomUUID().toString();
  }

  /**
   * 状态类
   */
  enum ResultTypeEnum {
    /**
     * 成功
     */
    SUCCESS(1, "成功");
    private int code;
    private String msg;

    ResultTypeEnum(int code, String msg) {
      this.code = code;
      this.msg = msg;
    }

    public int getCode() {
      return code;
    }

    public void setCode(int code) {
      this.code = code;
    }

    public String getMsg() {
      return msg;
    }

    public void setMsg(String msg) {
      this.msg = msg;
    }
  }

}
