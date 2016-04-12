<?php
/**
*   Environment check for DataApi
*   
*       Please check your php environment by env_check() method before use DataApi Demo!
*
*   @author Baidu Holmes
*/
require_once('Utility.php');

function env_check()
{
    print("------------------environment checking------------------\r\n");
//step1, shallow check : check  openssl and curl extensions

    print("[notice] start shallow check !\r\n");
    
    $extensions = get_loaded_extensions();
    if(!in_array('curl', $extensions))
    {
        print("[error] shallow Check failed: please enable curl extension for php !\r\n");
        return FALSE;
    }
    if(!in_array('openssl', $extensions))
    {
        print("[error] shallow Check failed: please enable openssl extension for php !\r\n");
        return FALSE;
    }
    print("[notice] shallow check passed !\r\n");

//step2, function check : check used functions of openssl and curl

    print("[notice] start function check !\r\n");
    
    $func_openssl = get_extension_funcs("openssl");
    if(!in_array('openssl_pkey_get_public', $func_openssl))
    {
        print("[error] function check failed: unknow function openssl_pkey_get_public !\r\n");
        return FALSE;
    }

    if(!in_array('openssl_public_encrypt', $func_openssl))
    {
        print("[error] function check failed: unknow function openssl_public_encrypt !\r\n");
        return FALSE;
    }
    
    $func_curl = get_extension_funcs("curl");
    if(!in_array('curl_init', $func_curl))
    {
        print("[error] function check failed: unknow function curl_init !\r\n");
        return FALSE;
    }

    if(!in_array('curl_setopt', $func_curl))
    {
        print("[error] function check failed: unknow function curl_setopt !\r\n");
        return FALSE;
    }

    if(!in_array('curl_exec', $func_curl))
    {
        print("[error] function check failed: unknow function curl_exec !\r\n");
        return FALSE;
    }

    if(!in_array('curl_error', $func_curl))
    {
        print("[error] function check failed: unknow function curl_error !\r\n");
        return FALSE;
    }

    if(!in_array('curl_close', $func_curl))
    {
        print("[error] function check failed: unknow function curl_close !\r\n");
        return FALSE;
    }

    if(!in_array('curl_errno', $func_curl))
    {
        print("[error] function check failed: unknow function curl_errno !\r\n");
        return FALSE;
    }
    
    print("[notice] function check passed !\r\n");

//step3, deep check: test pub encrypt and curl post indeed

    print("[notice] start deep check !\r\n");

    $rsa = new RsaPublicEncrypt('./');
    if(!$rsa->pubEncrypt("test pub encrypt"))
    {
        print("[error] deep check failed: pub encrypt failed !\r\n");
	return false;
    }

    $url = "www.baidu.com";
    $heads = array('Content-Type:  text/html;charset=UTF-8');
    $curl = curl_init();
    curl_setopt($curl, CURLOPT_URL, $url);
    curl_setopt($curl, CURLOPT_SSL_VERIFYPEER, 0);
    curl_setopt($curl, CURLOPT_SSL_VERIFYHOST, 1);
    curl_setopt($curl, CURLOPT_USERAGENT, $_SERVER['HTTP_USER_AGENT']);
    curl_setopt($curl, CURLOPT_FOLLOWLOCATION, 1);
    curl_setopt($curl, CURLOPT_AUTOREFERER, 1);
    curl_setopt($curl, CURLOPT_HTTPHEADER, $heads);
    curl_setopt($curl, CURLOPT_POST, 1);
    curl_setopt($curl, CURLOPT_POSTFIELDS, "test curl post");
    curl_setopt($curl, CURLOPT_TIMEOUT, 30);
    curl_setopt($curl, CURLOPT_HEADER, 0);
    curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);

    $tmpInfo = curl_exec($curl);
    if (curl_errno($curl))
    {
        print("[error] deep check failed: curl post failed !\r\n");
        return FALSE;
    }
    curl_close($curl);
    
    print("[notice] deep check passed !\r\n");

    print("----------------environment checking End----------------\r\n");
}

env_check();
