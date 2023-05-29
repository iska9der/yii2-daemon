<?php

namespace vyants\daemon\controllers;

use vyants\daemon\DaemonController;
use yii\base\ExitException;
use yii\base\InvalidRouteException;
use yii\console\Exception;
use yii\console\ExitCode;

/**
 * watcher-daemon - check another daemons and run it if need
 *
 * @author Vladimir Yants <vladimir.yants@gmail.com>
 */
abstract class WatcherDaemonController extends DaemonController
{
    /**
     * @var string subfolder in console/controllers
     */
    public $daemonFolder = 'daemons';

    /**
     * @var boolean flag for first iteration
     */
    protected $firstIteration = true;

    /**
     * Prevent double start
     */
    public function init()
    {
        $pid_file = $this->getPidPath();
        if (is_file($pid_file) && file_exists($pid_file) && ($pid = file_get_contents($pid_file)) && file_exists("/proc/$pid")) {
            $this->halt(ExitCode::UNSPECIFIED_ERROR, 'Another Watcher is already running.');
        }
        parent::init();
    }

    /**
     * Job processing body
     *
     * @param $job array
     *
     * @return boolean
     * @throws ExitException
     * @throws InvalidRouteException
     * @throws Exception
     */
    protected function doJob($job)
    {
        $pid_file = $this->getPidPath($job['daemon']);

        \Yii::info('Check daemon ' . $job['daemon']);
        if (is_file($pid_file) && file_exists($pid_file)) {
            $pid = file_get_contents($pid_file);
            if ($this->isProcessRunning($pid)) {
                if ($job['enabled']) {
                    \Yii::info('Daemon ' . $job['daemon'] . ' running and working fine');
                } else {
                    \Yii::warning('Daemon ' . $job['daemon'] . ' running, but disabled in config. Send SIGTERM signal.');
                    if (isset($job['hardKill']) && $job['hardKill']) {
                        posix_kill($pid, SIGKILL);
                    } else {
                        posix_kill($pid, SIGTERM);
                    }
                }

                return true;
            }
        }
        \Yii::error('Daemon pid not found.');
        if ($job['enabled']) {
            \Yii::info('Try to run daemon ' . $job['daemon'] . '.');
            $command_name = $job['daemon'] . DIRECTORY_SEPARATOR . 'index';
            //flush log before fork
            $this->flushLog(true);
            //run daemon
            $pid = pcntl_fork();
            if ($pid === -1) {
                $this->halt(ExitCode::UNSPECIFIED_ERROR, 'pcntl_fork() returned error');
            } elseif ($pid === 0) {
                $this->cleanLog();
                \Yii::$app->requestedRoute = $command_name;
                \Yii::$app->runAction("$command_name", ['demonize' => 1]);
                $this->halt(0);
            } else {
                $this->initLogger();
                \Yii::info('Daemon ' . $job['daemon'] . ' is running with pid ' . $pid);
            }
        }
        \Yii::info('Daemon ' . $job['daemon'] . ' is checked.');

        return true;
    }

    /**
     * @return array
     */
    protected function defineJobs()
    {
        if ($this->firstIteration) {
            $this->firstIteration = false;
        } else {
            \Yii::info('Watcher daemon sleeps for ' . $this->sleep . ' seconds');
            sleep($this->sleep);
        }

        return $this->getDaemonsList();
    }

    /**
     * Daemons for check. Better way - get it from database
     * [
     *  ['daemon' => 'one-daemon', 'enabled' => true]
     *  ...
     *  ['daemon' => 'another-daemon', 'enabled' => false]
     * ]
     * @return array
     */
    abstract protected function getDaemonsList();

    /**
     * @param $pid
     *
     * @return bool
     */
    public function isProcessRunning($pid)
    {
        return file_exists("/proc/$pid");
    }
}
