<?php
/**
*   Demo of DataApi
*   
*       set your information such as USERNAME, PASSWORD ... before use
*
*   @author Baidu Holmes
*/
require_once('ProfileService.php');
require_once('LoginService.php');
require_once('ReportService.php');

//preLogin,doLogin URL
define('LOGIN_URL', 'https://api.baidu.com/sem/common/HolmesLoginService');

//DataApi URL
define('API_URL', 'https://api.baidu.com/json/tongji/v1/ProductService/api');

//USERNAME
define('USERNAME', 'kgb-mobile');

//PASSWORD
define('PASSWORD', 'kgb@mobile');

//TOKEN
define('TOKEN', '908ed91bfdfae86c7e42ab50b5953a01');

//UUID, used to identify your device, for instance: MAC address 
define('UUID', '6C:AE:8B:52:AD:EA');

//ACCOUNT_TYPE
define('ACCOUNT_TYPE', 1);

//Parameter for query API
$Query_Parameter = array(
    'reportid' => 1,
    'metrics' => array('pageviews', 'visitors'),
    'dimensions' => array('pageid'),
    'start_time' => '20130801000000',
    'end_time' => '20130830235959',
    'filters' => array(),
    'start_index' => 0,
    'max_results' => 10,
    'sort' => array('pageviews desc'),
    );
//Set siteid by yourself
//$Query_Parameter['siteid']='******';

//Parameter for query_trans API
$Query_Trans_Parameter = array(
    'metrics' => array('transformNum'),
    'dimensions' => array('targetid'),
    'start_time' => '20130801000000',
    'end_time' => '20130830235959',
    'filters' => array(),
    'start_index' => 0,
    'max_results' => 10,
    'sort' => array('transformNum desc'),
    );
//Set siteid by yourself
//$Query_Trans_Parameter['siteid']='******';

//TRANS_NAME
define('TRANS_NAME', '******');

//TRANS_URL
define('TRANS_URL', '******');

//QUERY_TRANS_TYPE, 'name' or 'url'
define('QUERY_TRANS_TYPE', 'name');
