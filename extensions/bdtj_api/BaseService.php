<?php
/**
*   class LoginService, provide PreLogin, DoLogin, DoLogout methods
*   @author Baidu Holmes
*/

class BaseService
{
    /**
     * @var string
     **/
    protected $username;

    /**
     * @var string
     **/
    protected $password;


    /**
     * @var string
     **/
    protected $token;

    /**
     * UUID, used to identify your device, for instance: MAC address
     * @var string
     **/
    protected $uuid = "6C:AE:8B:52:AD:EA";

    /**
     * ACCOUNT_TYPE
     * @var number
     **/
    protected $account_type = 1;

    /**
     * LOGIN_URL, preLogin,doLogin URL
     * @var string
     **/
    static $LOGIN_URL = "https://api.baidu.com/sem/common/HolmesLoginService";

    /**
     * DataApi URL
     * @var string
     **/
    static $API_URL = "https://api.baidu.com/json/tongji/v1/ProductService/api";

    public function __construct($username, $password, $token, $uuid = null, $account_type = 1) {
        $this->username = $username;
        $this->token = $token;
        $this->password = $password;
        if (!(null === $uuid)) {
            $this->uuid = $uuid;
        }
        $this->account_type = $account_type;
    }
}
