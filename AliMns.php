<?php
/**
 * Created by PhpStorm.
 * User: tommy.jin
 * Date: 17/7/20
 * Time: 上午10:52
 * mns封装调用
 */


use AliyunMNS\Client;
use AliyunMNS\Requests\SendMessageRequest;
use AliyunMNS\Requests\CreateQueueRequest;
use AliyunMNS\Exception\MnsException;

class AliMns
{
    //连接端
    private $client = null;
    //新建Queue实例
    private $newqueue = null;
    //Queue实例
    private $queue = null;
    //消息反馈实例
    private $receiptHandle = null;
    //配置信息
    private $configs = null;
    //当前环境
    private $env = null;

    //构造函数
    function __construct($q = "")
    {
        //读取配置信息
        $this->env = Utils::getEnvironment();
        $this->configs = yaml_parse_file('./mns.yml');
        if (empty($this->configs)) throw new Exception("file error!");
        // 1. 首先初始化一个client
        $this->client = new Client($this->configs[$this->env]['endPoint'], $this->configs[$this->env]['accessId'],
            $this->configs[$this->env]['accessKey']);
        if ($q) {
            // 2. 生成一个CreateQueueRequest对象。CreateQueueRequest还可以接受一个QueueAttributes参数，用来初始化生成的queue的属性。
            // 2.1 对于queue的属性，请参考help.aliyun.com/document_detail/27476.html
            $request = new CreateQueueRequest($q);
            try {
                $this->client->createQueue($request);
            } catch (MnsException $e) {
                // 2.3 可能因为网络错误，或者Queue已经存在等原因导致CreateQueue失败，这里CatchException并做对应的处理
                echo "CreateQueueFailed: " . $e . "\n";
                echo "MNSErrorCode: " . $e->getMnsErrorCode() . "\n";
                return false;
            }
        }

    }

    /**
     * 发送消息
     * @param string $messageBody
     * @param string $queueName
     * @return Exception&object
     */
    public function push($messageBody = "", $queueName = "", $delaySeconds = null)
    {
        $queueName = empty($queueName) ? $queueName = $this->configs[$this->env]['queue']['default'] : $queueName =
            $this->configs[$this->env]['queue'][$queueName];
        // 1. 首先获取Queue的实例
        // 1.1 PHP SDK默认会对发送的消息做Base64 Encode，对接收到的消息做Base64 Decode。
        // 1.2 如果不希望SDK做这样的Base64操作，可以在getQueueRef的时候，传入参数$base64=FALSE。即$queue = $this->client->getQueueRef($queueName, FALSE);
        // 1.2 如果不希望SDK做这样的Base64操作，可以在getQueueRef的时候，传入参数$base64=FALSE。即$queue = $this->client->getQueueRef($queueName, FALSE);
        $q = $this->client->getQueueRef($queueName);
        // 2. 生成一个SendMessageRequest对象
        // 2.1 SendMessageRequest对象本身也包含了DelaySeconds和Priority属性可以设置。
        // 2.2 对于Message的属性，请参考help.aliyun.com/document_detail/27477.htmlWarning: XMLReader::read():
        $request = new SendMessageRequest($messageBody, $delaySeconds);
        try {
            $res = $q->sendMessage($request);
        } catch (MnsException $e) {
            // 4. 可能因为网络错误，或MessageBody过大等原因造成发送消息失败，这里CatchException并做对应的处理。
            return ['succeed' => false, 'data' => "SendMessage Failed: " . $e];
        }
        return ['succeed' => true, 'message_id' => $res->getMessageId(), 'data' => 'MessageSent!'];
    }

    /**
     * 发送消息
     * @param string $messageBody
     * @param string $queueName
     * @return Exception&object
     */
    public function sendMns($messageBody = "", $queueName = "", $delaySeconds = null)
    {
        if (empty($queueName)) {
            return false;
        }
        // 1. 首先获取Queue的实例
        // 1.1 PHP SDK默认会对发送的消息做Base64 Encode，对接收到的消息做Base64 Decode。
        // 1.2 如果不希望SDK做这样的Base64操作，可以在getQueueRef的时候，传入参数$base64=FALSE。即$queue = $this->client->getQueueRef($queueName, FALSE);
        // 1.2 如果不希望SDK做这样的Base64操作，可以在getQueueRef的时候，传入参数$base64=FALSE。即$queue = $this->client->getQueueRef($queueName, FALSE);
        $q = $this->client->getQueueRef($queueName);
        // 2. 生成一个SendMessageRequest对象
        // 2.1 SendMessageRequest对象本身也包含了DelaySeconds和Priority属性可以设置。
        // 2.2 对于Message的属性，请参考help.aliyun.com/document_detail/27477.htmlWarning: XMLReader::read():
        $request = new SendMessageRequest($messageBody, $delaySeconds);
        try {
            $res = $q->sendMessage($request);
        } catch (MnsException $e) {
            // 4. 可能因为网络错误，或MessageBody过大等原因造成发送消息失败，这里CatchException并做对应的处理。
            return ['succeed' => false, 'data' => "SendMessage Failed: " . $e];
        }
        return ['succeed' => true, 'message_id' => $res->getMessageId(), 'data' => 'MessageSent!'];
    }

    /**
     * 拉取消息
     * @param object $work
     * @param string $queueName
     * @return Exception&bool
     */
    public function handle($work = null, $queueName = "")
    {
        $queueName = empty($queueName) ? $queueName = $this->configs[$this->env]['queue']['default'] : $queueName =
            $this->configs[$this->env]['queue'][$queueName];

        $receiptHandle = NULL;
        // 获取Queue的实例
        $this->queue = $this->client->getQueueRef($queueName);
        while (true) {
            try {
                // 1. 直接调用receiveMessage函数
                // 1.1 receiveMessage函数接受waitSeconds参数，无特殊情况这里都是建议设置为30
                // 1.2 waitSeconds非0表示这次receiveMessage是一次http long polling，如果queue内刚好没有message，那么这次request会在server端等到queue内有消息才返回。最长等待时间为waitSeconds的值，最大为30。
                $res = $this->queue->receiveMessage(30);
                //echo "ReceiveMessage Succeed! \n";
                // 2. 获取ReceiptHandle，这是一个有时效性的Handle，可以用来设置Message的各种属性和删除Message。具体的解释请参考：help.aliyun.com/document_detail/27477.html 页面里的ReceiptHandle
                $this->receiptHandle = $res->getReceiptHandle();
            } catch (MnsException $e) {
                // 3. 像前面的CreateQueue和SendMessage一样，我们认为ReceiveMessage也是有可能出错的，所以这里加上CatchException并做对应的处理。
                //echo "ReceiveMessage Failed: " . $e . "\n";
                //echo "MNSErrorCode: " . $e->getMnsErrorCode() . "\n";
                continue;
            }
            $msg = array();
            $msg['MessageId'] = $res->getMessageId();
            $msg['DequeueCount'] = $res->getDequeueCount();
            $msg['EnqueueTime'] = $res->getEnqueueTime();
            $msg['FirstDequeueTime'] = $res->getFirstDequeueTime();
            $msg['MessageBody'] = $res->getMessageBody();
            $msg['MessageBodyMD5'] = $res->getMessageBodyMD5();
            $msg['NextVisibleTime'] = $res->getNextVisibleTime();
            $msg['Priority'] = $res->getPriority();
            $msg['ReceiptHandle'] = $res->getReceiptHandle();
            $msg['StatusCode'] = $res->getStatusCode();
            $work->consume($msg, $this);
        }
        return false;
    }

    /**
     * 新拉取消息
     * @param null $work
     * @param string $queueName
     * @return bool
     * @throws AtsException
     */
    public function newHandle($work = null, $queueName = "")
    {
        $conf = $this->configs[$this->env]['queue'];
        if (is_string($queueName) && strpos($queueName, '.') !== false) {

            foreach (explode('.', $queueName) as $segment) {
                if (!array_key_exists($segment, $conf)) {
                    throw new AtsException(AtsMessages::FAILURE, array('info' => array('请求的配置文件key不存在', $segment)));
                }

                $conf = $conf[$segment];
            }
        }

        $receiptHandle = NULL;
        // 获取Queue的实例
        $this->queue = $this->client->getQueueRef($conf);
        while (true) {
            try {
                // 1. 直接调用receiveMessage函数
                // 1.1 receiveMessage函数接受waitSeconds参数，无特殊情况这里都是建议设置为30
                // 1.2 waitSeconds非0表示这次receiveMessage是一次http long polling，如果queue内刚好没有message，那么这次request会在server端等到queue内有消息才返回。最长等待时间为waitSeconds的值，最大为30。
                $res = $this->queue->receiveMessage(30);
                //echo "ReceiveMessage Succeed! \n";
                // 2. 获取ReceiptHandle，这是一个有时效性的Handle，可以用来设置Message的各种属性和删除Message。具体的解释请参考：help.aliyun.com/document_detail/27477.html 页面里的ReceiptHandle
                $this->receiptHandle = $res->getReceiptHandle();
            } catch (MnsException $e) {
                // 3. 像前面的CreateQueue和SendMessage一样，我们认为ReceiveMessage也是有可能出错的，所以这里加上CatchException并做对应的处理。
                //echo "ReceiveMessage Failed: " . $e . "\n";
                //echo "MNSErrorCode: " . $e->getMnsErrorCode() . "\n";
                continue;
            }
            $msg = array();
            $msg['MessageId'] = $res->getMessageId();
            $msg['DequeueCount'] = $res->getDequeueCount();
            $msg['EnqueueTime'] = $res->getEnqueueTime();
            $msg['FirstDequeueTime'] = $res->getFirstDequeueTime();
            $msg['MessageBody'] = $res->getMessageBody();
            $msg['MessageBodyMD5'] = $res->getMessageBodyMD5();
            $msg['NextVisibleTime'] = $res->getNextVisibleTime();
            $msg['Priority'] = $res->getPriority();
            $msg['ReceiptHandle'] = $res->getReceiptHandle();
            $msg['StatusCode'] = $res->getStatusCode();
            $work->consume($msg, $this);
        }
        return false;
    }
    /**
     * 删除消息
     * @return Exception&bool
     */
    public function delete()
    {
        try {
            // 5. 直接调用deleteMessage即可。
            $res = $this->queue->deleteMessage($this->receiptHandle);
        } catch (MnsException $e) {
            // 6. 这里CatchException并做异常处理
            // 6.1 如果是receiptHandle已经过期，那么ErrorCode是MessageNotExist，表示通过这个receiptHandle已经找不到对应的消息。
            // 6.2 为了保证receiptHandle不过期，VisibilityTimeout的设置需要保证足够消息处理完成。并且在消息处理过程中，也可以调用changeMessageVisibility这个函数来延长消息的VisibilityTimeout时间。
            return ['succeed' => false, 'data' => "DeleteMessage Failed: " . $e];
        }
        return ['succeed' => true, 'data' => 'DeleteMessage Succeed!'];

    }
}

