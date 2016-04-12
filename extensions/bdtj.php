<?php
/**
 * 
 **/
define(BDTJ, dirname(__FILE__) . '/bdtj_api');
require_once(BDTJ . '/ProfileService.php');
require_once(BDTJ . '/LoginService.php');
require_once(BDTJ . '/ReportService.php');
require_once(BDTJ . '/ERROR.php');

class Bdtj {
    /**
     * @var object
     **/
    private $data = null;

    /**
     * @var string
     **/
    private $ucid = null;

    /**
     * @var string
     **/
    private $st = null;

    public function __construct($json) {
        if (is_string($json) && $json) {
            $this->data = json_decode($json, true);
        }
    }
    
    public function init() {
        if (isset($this->data['username']) && isset($this->data['token']) && isset($this->data['password'])) {
            $login = new LoginService($this->data['username'], $this->data['password'], $this->data['token']);
        } else {
            $this->log(ERROR::PARAM_ERROR);
        }
        if ($login) {
            if (!$login->PreLogin()) {
                $this->log(ERROR::LOGIN_ERROR);
            } else {
                $ret = $login->DoLogin();
                if ($ret) {
                    $this->ucid = $ret['ucid'];
                    $this->st = $ret['st'];
                } else {
                    $this->log(ERROR::LOGIN_RET_ERROR);
                }
            }
        }
    }   

    public function execute() {
        $this->init();


        /*$parameter['url'] = '%baidu.com%';
        $parameterJSON = json_encode($parameter);
        $profile = new ProfileService($this->data['username'], $this->data['password'], $this->data['token']);
        $ret = $profile->get_trans_info($this->ucid, $this->st, $parameterJSON);

        $retHead = $ret['retHead'];
        $retBody = $ret['retBody'];
        var_dump($ret);
        exit(); */

        $parameter['reportid'] = 1; 
        $parameter['siteid'] = 7942097;
        $parameter['start_time'] = '20160401000000';
        $parameter['end_time'] = '20160401235959';
        $parameter['metrics'] = array('pageviews', 'visitors', 'stayTime', 'exitRate', 'outwards', 'entrances');
        $parameter['max_results'] = 10000;
        $parameter['dimensions'] = array('pageid');
        $parameter['sort']  = array('pageviews desc');

        $url = null;
        $pv = 0;
        if (isset($this->data['username']) && isset($this->data['token']) && isset($this->data['password'])) {
            $offset = 0;
            while (($url == null || $url != '')) {
                $url = '';
                $result = "[]";
                $parameter['start_index'] = 10000*$offset;
                $offset ++;
                $parameterJSON = json_encode($parameter);

                $report = new ReportService($this->data['username'], $this->data['password'], $this->data['token']); 
                $ret = $report->query($this->ucid, $this->st, $parameterJSON); 

                $retBody = json_decode($ret['retBody'], true);
                var_dump($ret);
                if ($retBody) {
                    $query = json_decode($retBody['responseData'], true);
                    $report_id = $query['query']['result_id'];
                    $paramJSON = json_encode(array('result_id' => $report_id));

                    $status = null;
                    $counter = 0;
                    while (($status == null || !in_array($status, array(0,2,3))) && $counter < 20) {
                        $retStats = $report->getstatus($this->ucid, $this->st, $paramJSON);
                        $response = json_decode($retStats['retBody'], true);
                        $responseData = json_decode($response['responseData'], true);
                        $status = $responseData['result']['status'];
                        $url = $responseData['result']['result_url'];
                        sleep(5);
                        $counter ++;
                        if ($status == 3) {
                            $result = @file_get_contents($url);
                        } 
                    }
                    $this->log($url, 'BDTJ_NOTICE');
                    //$this->log($result, 'BDTJ_RESULT');
                } else {
                    $retHead = json_decode($ret['retHead'], true);
                    $this->log($retHead['failures'][0]['message']);
                }
                $dataTemp = json_decode($result, true);
                if ($dataTemp) {
                    foreach ($dataTemp as $value) {
                        $pv += $value['pageviews'];
                    }
                } else {
                    break;
                }
                var_dump($pv);
            }
        }
    }

    public function log($msg, $type = "ERROR") {
        echo("[" . date("Y-m-d H:i:s", time()) . "] [{$type}] " . $msg . "\n");
    }

}

if ($argc < 4) {
    exit("Usage: php bdtj.php '{\"username\":\"kgb-mobile\",\"token\":\"908ed91bfdfae86c7e42ab50b5953a01\"}' 20160401 20160402 7942097\n");
} else {
    $obj = new Bdtj($argv[1], $argv[2], $argv[3], $argv[4]);
    $obj->execute();
}
