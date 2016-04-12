<?php
/**
*   class DataApiConnection, provide POST method: send POST request to DataApi URL
*
*    @author Baidu Holmes
*/
class DataApiConnection
{
    var $url;
    var $headers;
    var $postData;

    var $retHead;
    var $retBody;

    public function init($url, $ucid)
    {
        $this->url = $url;
        $this->headers = array('UUID: '.UUID, 'USERID: '.$ucid, 'Content-Type:  data/json;charset=UTF-8');
    }

    public function genPostData($data)
    {
        $this->postData = json_encode($data);
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

        $tmpRet = curl_exec($curl);
        if (curl_errno($curl))
        {
            print("[error] CURL ERROR: ".curl_error($curl)."\r\n");
        }
        curl_close($curl);
        $tmpArray = json_decode($tmpRet,TRUE);
        if  (isset($tmpArray['header']) && isset($tmpArray['body']))
        {
            $this->retHead = json_encode($tmpArray['header']);
            $this->retBody = json_encode($tmpArray['body']);
        }
        else
        {
            print("[error] SERVICE ERROR: ".$tmpRet."\r\n");
        }
    }
}
