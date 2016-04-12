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
define(BDTJ, dirname(__FILE__) . "/bdtj_api");
require_once(BDTJ . '/ProfileService.php');
require_once(BDTJ . '/LoginService.php');
require_once(BDTJ . '/ReportService.php');
require_once(BDTJ . '/Config.php');

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


//call getsites method of profile service
$profile = new ProfileService();
$ret = $profile->getsites($ucid, $st);

$retHead = $ret['retHead'];
$retBody = $ret['retBody'];

if(!$retHead || !$retBody)
{
    exit();
}

/*
*   Now, you have successfully call the getsites method of
*   profile service. You can deal with retHead and retBody
*   an you need.
*   For instance:Get quota cost in this call and quota remain
*           $retHeadArray = json_decode($retHead, TRUE);
*           print("This call cost quota: ".$retHeadArray['quota']."\r\n");
*           print("My account has remain quota: ".$retHeadArray['rquota']."\r\n");
*
*   In the next, we will use the first siteid of retuned site Information
*   to show: how to call query, getstatus method of report service.
*
*/

$retBodyArray = json_decode($retBody, TRUE);
$siteInfo = json_decode($retBodyArray['responseData'],TRUE);
if (count($siteInfo['sites']) > 0 )
{
    $siteid = $siteInfo['sites'][0]['siteid'];
}
else
{
    exit();
}


$report = new ReportService();

//call query method of report service

$parameter = $Query_Trans_Parameter;
if(!isset($parameter['siteid'])){
    $parameter['siteid'] = $siteid;
}
if(QUERY_TRANS_TYPE == 'url'){
    $parameter['url'] = TRANS_URL;
}
else if(QUERY_TRANS_TYPE =='name'){
    $parameter['name'] = TRANS_NAME;
}
$parameterJSON = json_encode($parameter);

$ret = $report->query_trans($ucid, $st, $parameterJSON);

$retHead = $ret['retHead'];
$retBody = $ret['retBody'];

if(!$retHead || !$retBody)
{
    exit();
}

/*
*   Now, you have successfully call query method of 
*   report service. Similarly, you can deal with retHead
*   or retBody on you need.
*   In the next, we will show how to call getstauts method
*   using result_id that we have got by calling query method.
*/
$retHeadArray = json_decode($retHead, TRUE);
$retBodyArray = json_decode($retBody, TRUE);

$trans_count = json_decode($retBodyArray['responseData'], TRUE);
var_dump($trans_count);

//doLogout, please doLogout after call DataApi services

if (isset($ret['ucid']) && isset($ret['st']))
{
    $loginService->DoLogout($ucid, $st);
}
