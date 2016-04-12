<?php

/**
*   Utility, provide RSA public encrypt method and gzdecode function
*   
*/

class RsaPublicEncrypt
{
        private $_publicKey;
        private $path;

        public function __construct($path)
        {
                if(empty($path) || !is_dir($path))
                {
                    print("[error] error public key path: ".$path."\r\n");
                }
                $this->path = $path;
        }

        public function setupPublicKey()
        {
                if(is_resource($this->_publicKey))
                {
                        return true;
                }
                $file = $this->path . DIRECTORY_SEPARATOR .  'apiPub.key';
                $puk = file_get_contents($file);
                $this->_publicKey = openssl_pkey_get_public($puk);
                return true;
        }

        public function pubEncrypt($data)
        {
                if(!is_string($data))
                {
                        return null;
                }
                $this->setupPublicKey();
                $ret = openssl_public_encrypt($data, $encrypted, $this->_publicKey);
                if($ret)
                {
                        return $encrypted;
                }
                else
                {
                    return null;
                }
        }
        public function __destruct()
        {
                @ fclose($this->_publicKey);
        }
}

if (!function_exists('gzdecode')) 
{ 
    function gzdecode ($data) 
    { 
        $flags = ord(substr($data, 3, 1)); 
        $headerlen = 10; 
        $extralen = 0; 
        $filenamelen = 0; 
        if ($flags & 4) 
        {       
            $extralen = unpack('v' ,substr($data, 10, 2)); 
            $extralen = $extralen[1]; 
            $headerlen += 2 + $extralen; 
        }       
        if ($flags & 8)
        {
            $headerlen = strpos($data, chr(0), $headerlen) + 1; 
        }
        if ($flags & 16) 
        {
            $headerlen = strpos($data, chr(0), $headerlen) + 1; 
        }
        if ($flags & 2) 
        {
            $headerlen += 2; 
        }
        $unpacked = @gzinflate(substr($data, $headerlen)); 
        if ($unpacked === FALSE)  
        {
            $unpacked = $data;
        }
        return $unpacked; 
    } 
}
