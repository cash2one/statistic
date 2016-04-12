<?php

/**
 *  class LoginConnection, provide POST method: send POST request to Login URL
 * 
 *  @author Baidu Holmes
 *
 */

require_once('Utility.php');
 
class LoginConnection
{
    var $url;
    var $headers;
    var $postData;

    var $returnCode;
    var $retData;

    public function init($url, $uuid, $account_type = 1)
    {
        $this->url = $url;
        $this->headers = array('UUID: '.$uuid, 'account_type: '.$account_type, 'Content-Type:  data/gzencode and rsa public encrypt;charset=UTF-8');
    }

    public function genPostData($data)
    {
        $index = 0;
        $json_data = json_encode($data);
        $gz_data = gzencode($json_data,9);
        $rsa = new RsaPublicEncrypt(dirname(__FILE__));
        $en_data = "";
        while ($index < strlen($gz_data))
        {
            $gz_pack_data = substr($gz_data, $index, 117);
            $index += 117;
            $en_data .= $rsa->pubEncrypt($gz_pack_data);
        }
        $this->postData = $en_data;
        return TRUE;
    }
    
    public function POST($data)
    { 
        $this->genPostData($data);
        
        $curl = curl_init();
        curl_setopt($curl, CURLOPT_URL, $this->url);
        curl_setopt($curl, CURLOPT_SSL_VERIFYPEER, 0);
        curl_setopt($curl, CURLOPT_SSL_VERIFYHOST, 1);
        curl_setopt($curl, CURLOPT_USERAGENT, $_SERVER['HTTP_USER_AGENT']);
        curl_setopt($curl, CURLOPT_FOLLOWLOCATION, 1);
        curl_setopt($curl, CURLOPT_AUTOREFERER, 1);
        curl_setopt($curl, CURLOPT_HTTPHEADER, $this->headers);
        curl_setopt($curl, CURLOPT_POST, 1);
        curl_setopt($curl, CURLOPT_POSTFIELDS, $this->postData); 
        curl_setopt($curl, CURLOPT_TIMEOUT, 30);
        curl_setopt($curl, CURLOPT_HEADER, 0);
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);

        $tmpInfo = curl_exec($curl);
        if (curl_errno($curl))
        {
            print("[error] CURL ERROR: ".curl_error($curl)."\r\n");
        }
        curl_close($curl);
        
        $this->returnCode = ord($tmpInfo[0])*64 + ord($tmpInfo[1]);

        if ($this->returnCode === 0)
        {
            $this->retData = substr($tmpInfo, 8);
        }
    }
}
