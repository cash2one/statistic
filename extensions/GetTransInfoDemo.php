<?php
/**
*   Demo of DataApi
*   
*       set your information in Config.php such as USERNAME, PASSWORD ... before use
*
*       especially, you can modify this Demo on your need!
*
*   @author Baidu Holmes
*/
require_once('ProfileService.php');
require_once('LoginService.php');
require_once('ReportService.php');
require_once('Config.php');

$loginService = new LoginService();

//preLogin
if (!$loginService->PreLogin())
{
    exit();
}


//doLogin
$ret = $loginService->DoLogin();
if ($ret)
{
    $ucid = $ret['ucid'];
    $st = $ret['st'];
}
else
{
    exit();
}


//call get_trans_info method of profile service
$parameter = array(
);
if(QUERY_TRANS_TYPE == 'url'){
    $parameter['url'] = TRANS_URL;
}
else if(QUERY_TRANS_TYPE =='name'){
    $parameter['name'] = TRANS_NAME;
}

$parameterJSON = json_encode($parameter);
$profile = new ProfileService();
$ret = $profile->get_trans_info($ucid, $st, $parameterJSON);

$retHead = $ret['retHead'];
$retBody = $ret['retBody'];

if(!$retHead || !$retBody)
{
    exit();
}

$retBodyArray = json_decode($retBody, TRUE);
$transInfo = json_decode($retBodyArray['responseData'],TRUE);
var_dump($transInfo);

//doLogout, please doLogout after call DataApi services

if (isset($ret['ucid']) && isset($ret['st']))
{
    $loginService->DoLogout($ucid, $st);
}
