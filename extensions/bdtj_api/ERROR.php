<?php

class ERROR {
    const DEF_MSG = '系统异常';
    
    //自定义异常
    const PARAM_ERROR = '输入参数异常，需要提供正确的username/password/token';
    const LOGIN_ERROR = '百度统计登录失败';
    const LOGIN_RET_ERROR = '百度统计登录鉴权信息获取失败';
}
