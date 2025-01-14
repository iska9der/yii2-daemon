<?php

namespace vyants\daemon;

use yii\base\Action;
use yii\base\ExitException;
use yii\base\NotSupportedException;
use yii\console\Controller;
use yii\console\ExitCode;
use yii\db\Exception;
use yii\helpers\Console;

/**
 * Class DaemonController
 *
 * @author Vladimir Yants <vladimir.yants@gmail.com>
 */
abstract class DaemonController extends Controller
{

    const EVENT_BEFORE_JOB = "EVENT_BEFORE_JOB";
    const EVENT_AFTER_JOB = "EVENT_AFTER_JOB";

    const EVENT_BEFORE_ITERATION = "event_before_iteration";
    const EVENT_AFTER_ITERATION = "event_after_iteration";

    /**
     * @var $demonize boolean Run controller as Daemon
     * @default false
     */
    public $demonize = false;

    /**
     * @var $isMultiInstance boolean allow daemon create a few instances
     * @see $maxChildProcesses
     * @default false
     */
    public $isMultiInstance = false;

    /**
     * @var $parentPID int main procces pid
     */
    protected $parentPID;

    /**
     * @var $maxChildProcesses int max daemon instances
     * @default 10
     */
    public $maxChildProcesses = 10;

    /**
     * @var $currentJobs [] array of running instances
     */
    protected static $currentJobs = [];

    /**
     * @var int Memory limit for daemon, must bee less than php memory_limit
     * @default 32M
     */
    protected $memoryLimit = 268435456;

    /**
     * @var boolean used for soft daemon stop, set 1 to stop
     */
    private static $stopFlag = false;

    /**
     * @var int Delay between task list checking
     * @default 5sec
     */
    protected $sleep = 5;

    protected $pidDir = "@runtime/daemons/pids";

    protected $logClass = \yii\log\FileTarget::class;
    protected $logDir = "@runtime/daemons/logs";
    protected $logInterval = 1000;

    private $stdIn;
    private $stdOut;
    private $stdErr;

    /**
     * Init function
     */
    public function init()
    {
        parent::init();

        //set PCNTL signal handlers
        pcntl_signal(SIGTERM, ['vyants\daemon\DaemonController', 'signalHandler']);
        pcntl_signal(SIGINT, ['vyants\daemon\DaemonController', 'signalHandler']);
        pcntl_signal(SIGHUP, ['vyants\daemon\DaemonController', 'signalHandler']);
        pcntl_signal(SIGUSR1, ['vyants\daemon\DaemonController', 'signalHandler']);
        pcntl_signal(SIGCHLD, ['vyants\daemon\DaemonController', 'signalHandler']);
    }

    function __destruct()
    {
        $this->deletePid();
    }

    /**
     * Adjusting logger. You can override it.
     */
    protected function initLogger()
    {
        $targets = \Yii::$app->getLog()->targets;
        foreach ($targets as $name => $target) {
            $target->enabled = false;
        }
        $config = [
            'levels' => ['error', 'warning', 'trace', 'info'],
            'logFile' => \Yii::getAlias($this->logDir) . DIRECTORY_SEPARATOR . $this->getProcessName() . '.log',
            'logVars' => [],
            'exportInterval' => $this->logInterval,
            'except' => [
                'yii\db\*', // Don't include messages from db
            ],
        ];
        $targets['daemon'] = new $this->logClass($config);
        \Yii::$app->getLog()->targets = $targets;
        \Yii::$app->getLog()->flushInterval = $this->logInterval;
        \Yii::$app->getLog()->init();
    }

    /**
     * Daemon worker body
     *
     * @param $job
     *
     * @return boolean
     */
    abstract protected function doJob($job);

    /**
     * Base action
     * @return int|null
     * @throws Exception
     * @throws ExitException
     */
    final public function actionIndex()
    {
        if ($this->demonize) {
            $pid = pcntl_fork();
            if ($pid == -1) {
                $this->halt(ExitCode::UNSPECIFIED_ERROR, 'pcntl_fork() rise error');
            } elseif ($pid) {
                $this->cleanLog();
                $this->halt(ExitCode::OK);
            } else {
                posix_setsid();
                $this->closeStdStreams();
            }
        }
        $this->changeProcessName();

        //run loop
        return $this->loop();
    }

    /**
     * Set new process name
     */
    protected function changeProcessName()
    {
        //rename process
        if (version_compare(PHP_VERSION, '5.5.0') >= 0) {
            cli_set_process_title($this->getProcessName());
        } else {
            if (function_exists('setproctitle')) {
                setproctitle($this->getProcessName());
            } else {
                \Yii::error('Can\'t find cli_set_process_title or setproctitle function');
            }
        }
    }

    /**
     * Close std streams and open to /dev/null
     * need some class properties
     */
    protected function closeStdStreams()
    {
        if (is_resource(STDIN)) {
            fclose(STDIN);
            $this->stdIn = fopen('/dev/null', 'r');
        }
        if (is_resource(STDOUT)) {
            fclose(STDOUT);
            $this->stdOut = fopen('/dev/null', 'ab');
        }
        if (is_resource(STDERR)) {
            fclose(STDERR);
            $this->stdErr = fopen('/dev/null', 'ab');
        }
    }

    /**
     * Init logger
     *
     * @param Action $action
     *
     * @return bool
     * @throws NotSupportedException
     */
    public function beforeAction($action)
    {
        if (!parent::beforeAction($action)) {
            return false;
        }

        $this->initLogger();
        if ($action->id != "index") {
            throw new NotSupportedException(
                "Only index action allowed in daemons. So, don't create and call another"
            );
        }

        return true;
    }

    /**
     * Возвращает доступные опции
     *
     * @param string $actionID
     *
     * @return array
     */
    public function options($actionID)
    {
        return [
            'demonize',
            'taskLimit',
            'isMultiInstance',
            'maxChildProcesses',
        ];
    }

    /**
     * Extract current unprocessed jobs
     * You can extract jobs from DB (DataProvider will be great), queue managers (ZMQ, RabbiMQ etc), redis and so on
     *
     * @return array with jobs
     */
    abstract protected function defineJobs();

    /**
     * Fetch one task from array of tasks
     *
     * @param array $jobs
     *
     * @return mixed one task
     */
    protected function defineJobExtractor(&$jobs)
    {
        return array_shift($jobs);
    }

    /**
     * Main Loop
     *
     * *
     * @return int 0|1
     * @throws Exception
     * @throws ExitException
     */
    private function loop()
    {
        if (!file_put_contents($this->getPidPath(), getmypid())) {
            $this->halt(ExitCode::UNSPECIFIED_ERROR, 'Can\'t create pid file ' . $this->getPidPath());
        }

        $this->parentPID = getmypid();
        \Yii::info('Daemon ' . $this->getProcessName() . ' pid ' . getmypid() . ' started.');
        while (!self::$stopFlag) {
            if (memory_get_usage() > $this->memoryLimit) {
                \Yii::info('Daemon ' . $this->getProcessName() . ' pid ' .
                    getmypid() . ' used ' . memory_get_usage() . ' bytes on ' . $this->memoryLimit .
                    ' bytes allowed by memory limit');
                break;
            }
            $this->trigger(self::EVENT_BEFORE_ITERATION);
            $this->renewConnections();
            $jobs = $this->defineJobs();
            if ($jobs && !empty($jobs)) {
                while (($job = $this->defineJobExtractor($jobs)) !== null) {
                    //if no free workers, wait
                    if ($this->isMultiInstance && (count(static::$currentJobs) >= $this->maxChildProcesses)) {
                        \Yii::info('Reached maximum number of child processes. Waiting...');
                        while (count(static::$currentJobs) >= $this->maxChildProcesses) {
                            sleep(1);
                            pcntl_signal_dispatch();
                        }
                        \Yii::info(
                            'Free workers found: ' .
                            ($this->maxChildProcesses - count(static::$currentJobs)) .
                            ' worker(s). Delegate tasks.'
                        );
                    }
                    pcntl_signal_dispatch();
                    $this->runDaemon($job);
                }
            } else {
                //\Yii::info('Job list is empty. Daemon sleeps for ' . $this->sleep . ' seconds');
                sleep($this->sleep);
            }
            pcntl_signal_dispatch();
            $this->trigger(self::EVENT_AFTER_ITERATION);
        }

        \Yii::info('Daemon ' . $this->getProcessName() . ' pid ' . getmypid() . ' is stopped.');

        return ExitCode::OK;
    }

    /**
     * Delete pid file
     * @return bool
     * @throws ExitException
     */
    protected function deletePid()
    {
        $pid = $this->getPidPath();
        if (is_file($pid) && file_exists($pid)) {
            if (file_get_contents($pid) == getmypid()) {
                unlink($this->getPidPath());
            }
        } else {
            \Yii::error('Can\'t unlink pid file ' . $this->getPidPath());
        }
    }

    /**
     * PCNTL signals handler
     *
     * @param int $signo
     * @param array $siginfo
     * @param null $status
     */
    final static function signalHandler($signo, $siginfo = [], $status = null)
    {
        switch ($signo) {
            case SIGINT:
            case SIGTERM:
                //shutdown
                self::$stopFlag = true;
                break;
            case SIGHUP:
                //restart, not implemented
                break;
            case SIGUSR1:
                //user signal, not implemented
                break;
            case SIGCHLD:
                $pid = $siginfo['pid'] ?? null;
                if (!$pid) {
                    $pid = pcntl_waitpid(-1, $status, WNOHANG);
                }
                while ($pid > 0) {
                    if ($pid && isset(static::$currentJobs[$pid])) {
                        unset(static::$currentJobs[$pid]);
                    }
                    $pid = pcntl_waitpid(-1, $status, WNOHANG);
                }
                break;
        }
    }

    /**
     * Tasks runner
     *
     * @param string $job
     *
     * @return boolean
     * @throws Exception
     * @throws ExitException
     */
    final public function runDaemon($job)
    {
        if ($this->isMultiInstance) {
            $this->flushLog();
            $pid = pcntl_fork();

            if ($pid == -1) {
                return false;
            }
            if ($pid !== 0) {
                static::$currentJobs[$pid] = true;
                return true;
            }

            $this->cleanLog();
            $this->renewConnections();
            //child process must die
            $this->trigger(self::EVENT_BEFORE_JOB);
            $status = $this->doJob($job);
            $this->trigger(self::EVENT_AFTER_JOB);
            if ($status) {
                $this->halt(ExitCode::OK);
            } else {
                $this->halt(ExitCode::UNSPECIFIED_ERROR, 'Child process #' . $pid . ' return error.');
            }
        } else {
            $this->trigger(self::EVENT_BEFORE_JOB);
            $status = $this->doJob($job);
            $this->trigger(self::EVENT_AFTER_JOB);

            return $status;
        }
    }

    /**
     * Stop process and show or write message
     *
     * @param $code int -1|0|1
     * @param null $message string
     * @throws ExitException
     */
    protected function halt($code, $message = null)
    {
        if ($message !== null) {
            if ($code == ExitCode::UNSPECIFIED_ERROR) {
                \Yii::error($message);
                if (!$this->demonize) {
                    $message = Console::ansiFormat($message, [Console::FG_RED]);
                }
            } else {
                \Yii::info($message);
            }
            if (!$this->demonize) {
                $this->writeConsole($message);
            }
        }
        if ($code !== -1) {
            \Yii::$app->end($code);
        }
    }

    /**
     * Renew connections
     * @throws Exception
     */
    protected function renewConnections()
    {
        if (isset(\Yii::$app->db)) {
            \Yii::$app->db->close();
            \Yii::$app->db->open();
        }
    }

    /**
     * Show message in console
     *
     * @param $message
     */
    private function writeConsole($message)
    {
        $out = Console::ansiFormat('[' . date('d.m.Y H:i:s') . '] ', [Console::BOLD]);
        $this->stdout($out . $message . "\n");
    }

    /**
     * @param null $daemon
     *
     * @return string
     * @throws ExitException
     */
    public function getPidPath($daemon = null)
    {
        $dir = \Yii::getAlias($this->pidDir);
        if (!file_exists($dir)) {
            if (!mkdir($dir, 0744, true) && !is_dir($dir)) {
                $this->halt(ExitCode::UNSPECIFIED_ERROR, 'Can\'t create directory for pidDir');
            }
        }
        $daemon = $this->getProcessName($daemon);

        return $dir . DIRECTORY_SEPARATOR . $daemon;
    }

    /**
     * @return string
     */
    public function getProcessName($route = null)
    {
        if (is_null($route)) {
            $route = \Yii::$app->requestedRoute;
        }

        return str_replace(['/index', '/'], ['', '.'], $route);
    }

    /**
     *  If in daemon mode - no write to console
     *
     * @param string $string
     *
     * @return bool|int
     */
    public function stdout($string)
    {
        if (!$this->demonize && is_resource(STDOUT)) {
            return parent::stdout($string);
        }

        return false;
    }

    /**
     * If in daemon mode - no write to console
     *
     * @param string $string
     *
     * @return int
     */
    public function stderr($string)
    {
        if (!$this->demonize && is_resource(\STDERR)) {
            return parent::stderr($string);
        }

        return false;
    }

    /**
     * Empty log queue
     */
    protected function cleanLog()
    {
        \Yii::$app->log->logger->messages = [];
    }

    /**
     * Empty log queue
     */
    protected function flushLog($final = false)
    {
        \Yii::$app->log->logger->flush($final);
    }
}
