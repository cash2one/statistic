<?php
/**
*   class LoginService, provide PreLogin, DoLogin, DoLogout methods
*   @author Baidu Holmes
*/
require_once('LoginConnection.class.php');
require_once('BaseService.php');

class LoginService extends BaseService
{
    public function PreLogin()
    {
        print("----------------------preLogin----------------------\r\n");
        print("[notice] start preLogin!\r\n");

        $preLogin = new LoginConnection();
        $preLogin->init(self::$LOGIN_URL, $this->uuid);
        $preLoginData = array(
            'username' => $this->username,
            'token' => $this->token,
            'functionName' => 'preLogin',
            'uuid' => $this->uuid,
            'request' => array(
                'osVersion' => 'linux',
                'deviceType' => 'pc',
                'clientVersion' => '1.0',
                ),
            );
        $preLogin->POST($preLoginData);

        if ($preLogin->returnCode === 0)
        {
            $retData = gzdecode($preLogin->retData, strlen($preLogin->retData));
            $retArray = json_decode($retData, TRUE);
            if (!isset($retArray['needAuthCode']) || $retArray['needAuthCode'] === TRUE)
            {   
                print('[error] preLogin return data format error: '.$retData."\r\n");
                print("--------------------preLogin End--------------------\r\n");
                return FALSE;
            }
            else if ($retArray['needAuthCode'] === FALSE)
            {
                print("[notice] preLogin successfully!\r\n");
                print("--------------------preLogin End--------------------\r\n");
                return TRUE;
            }
            else
            {
                print("[error] unexpected preLogin return data: ".$retData."\r\n");
                print("--------------------preLogin End--------------------\r\n");
                return FALSE;
            }
        }
        else
        {
            print("[error] preLogin unsuccessfully with return code: ".$preLogin->returnCode."\r\n");
            print("--------------------preLogin End--------------------\r\n");
            return FALSE;
        }
    }

    public function DoLogin()
    {
        print("----------------------doLogin----------------------\r\n");
        print("[notice] start doLogin!\r\n");

        $doLogin = new LoginConnection();
        $doLogin->init(self::$LOGIN_URL, $this->uuid);
        $doLoginData = array(
            'username' => $this->username,
            'token' => $this->token,
            'functionName' => 'doLogin',
            'uuid' => $this->uuid,
            'request' => array(
                'password' => $this->password,
                ),
            );
        $doLogin->POST($doLoginData);

        if ($doLogin->returnCode === 0)
        {
            //$retData = gzdecode($doLogin->retData, strlen($doLogin->retData));
            $retData = gzinflate(substr($doLogin->retData,10,-8));
            $retArray = json_decode($retData, TRUE);
            if (!isset($retArray['retcode']) || !isset($retArray['ucid']) || !isset($retArray['st']))
            {
                print("[error] doLogin return data format error: ".$retData."\r\n");
                print("--------------------doLogin End--------------------\r\n");
                return null;
            }
            else if ($retArray['retcode'] === 0)
            {
                print("[notice] doLogin successfully!\r\n");
                print("--------------------doLogin End--------------------\r\n");
                return array('ucid' => $retArray['ucid'], 'st' => $retArray['st']);
            }
            else
            {
                print("[error] doLogin unsuccessfully with retcode: ".$retArray['retcode']."\r\n");
                print("--------------------doLogin End--------------------\r\n");
                return null;
            }
        }
        else
        {
            print("[error] doLogin unsuccessfully with return code: ".$doLogin->returnCode."\r\n");
            print("--------------------doLogin End--------------------\r\n");
            return null;
        }
    }

    public function DoLogout($ucid, $st)
    {
        print("----------------------doLogout----------------------\r\n");
        print("[notice] start doLogout!\r\n");

        $doLogout = new LoginConnection();
        $doLogout->init(self::$LOGIN_URL);
        $doLogoutData = array(
            'username' => $this->username,
            'token' => $this->token,
            'functionName' => 'doLogout',
            'uuid' => $this->uuid,
            'request' => array(
                'ucid' => $ucid,
                'st' => $st,
                ),
            );
        $doLogout->POST($doLogoutData);

        if ($doLogout->returnCode === 0)
        {
            $retData = gzdecode($doLogout->retData, strlen($doLogout->retData));
            $retArray = json_decode($retData, TRUE);
            if (!isset($retArray['retcode']))
            {
                print("[error] doLogout return data format error: ".$retData."\r\n");
                print("--------------------doLogout End--------------------\r\n");
                return FALSE;
            }
            else if ($retArray['retcode'] === 0 )
            {
                print("[notice] doLogout successfully!\r\n");
                print("--------------------doLogout End--------------------\r\n");
                return TRUE;
            }
            else
            {
                print("[error] doLogout unsuccessfully with retcode: ".$retArray['retcode']."\r\n");
                print("--------------------doLogout End--------------------\r\n");
                return FALSE;
            }
        }
        else
        {
            print("[error] doLogout unsuccessfully with return code: ".$doLogout->returnCode."\r\n");
            print("--------------------doLogout End--------------------\r\n");
            return FALSE;
        }
    }
}
