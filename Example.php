<?php


require_once __DIR__ . './mns-autoloader.php';
require_once __DIR__ . './autoload.php';
$_SERVER['environment'] = 'test';
$consumer = new AliMns();
$consumer->sendMns('test message.', 'test-queue-01');

$worker = new CashOrderWorker();
$consumer->handle($worker, 'test-obj.01');

class TestObjWorker
{
    public function consume($msg, $consumer)
    {
        try {
            $params = $msg['MessageBody'];
            $msg_id = $msg['MessageId'];
            echo $params,$msg_id."\n";
        } catch (Exception $e) {
            echo 'Exception:',$e->getMessage()."\n";
        }
    }

}
