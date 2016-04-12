<?php
/**
*   class ReportService, provide query and getstatus methods of report service
*   @author Baidu Holmes
*/
require_once('DataApiConnection.class.php');
require_once('BaseService.php');

class ReportService extends BaseService
{
    public function query($ucid, $st, $parameterJSON)
    {
        print("----------------------query----------------------\r\n");
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
                'serviceName' => 'report',
                'methodName' => 'query',
                'parameterJSON' => $parameterJSON,
                ),
            );
        $apiConnection->POST($apiConnectionData);
        return array('retHead' => $apiConnection->retHead, 'retBody' => $apiConnection->retBody);
        
    }

    public function getstatus($ucid, $st, $parameterJSON)
    {
        print("----------------------getstatus----------------------\r\n");
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
                'serviceName' => 'report',
                'methodName' => 'getstatus',
                'parameterJSON' => $parameterJSON,
                ),      
            );      
        $apiConnection->POST($apiConnectionData);
        return array('retHead' => $apiConnection->retHead, 'retBody' => $apiConnection->retBody);
    }
    
    public function query_trans($ucid, $st, $parameterJSON)
    {
        print("----------------------query_trans----------------------\r\n");
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
                'serviceName' => 'report',
                'methodName' => 'query_trans',
                'parameterJSON' => $parameterJSON,
                ),
            );
        $apiConnection->POST($apiConnectionData);
        return array('retHead' => $apiConnection->retHead, 'retBody' => $apiConnection->retBody);
        
    }
}
