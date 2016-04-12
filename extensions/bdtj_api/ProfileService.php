<?php
/**
*   class ProfileService, provide getsites method of profile service
*   @author Baidu Holmes
*/
require_once('DataApiConnection.class.php');
require_once('BaseService.php');
class ProfileService extends BaseService
{
    public function getsites($ucid, $st)
    {
        print("----------------------getsites----------------------\r\n");
        $apiConnection = new DataApiConnection();
        $apiConnection->init(self::$API_URL, $ucid);
        
        $apiConnectionData = array(
            'header' => array(
                'username' => $this->username,
                'password' => $st,
                'token' => $this->token,
                'account_type' => $this->account_type,
                ),
            'body' => array(
                'serviceName' => 'profile',
                'methodName' => 'getsites',
                ),
            );
        $apiConnection->POST($apiConnectionData);
        return array('retHead' => $apiConnection->retHead, 'retBody' => $apiConnection->retBody);
    }

    public function get_trans_info($ucid, $st, $parameterJSON)
    {
        print("----------------------get_trans_info----------------------\r\n");
        $apiConnection = new DataApiConnection();
        $apiConnection->init(self::$API_URL, $ucid);

        $apiConnectionData = array(
            'header' => array(
                'username' => $this->username,
                'password' => $st,
                'token' => $this->token,
                'account_type' => $this->account_type,
                ),
            'body' => array(
                'serviceName' => 'profile',
                'methodName' => 'get_trans_info',
                'parameterJSON' => $parameterJSON,
                ),
            );
        $apiConnection->POST($apiConnectionData);
        return array('retHead' => $apiConnection->retHead, 'retBody' => $apiConnection->retBody);
    }
}


